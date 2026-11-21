package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Principal struct {
	Org    string   `json:"org"`
	Scopes []string `json:"scopes"`
}
type Document struct {
	SourceID      string `json:"source_id"`
	SourceVersion *int   `json:"source_version,omitempty"`
	Filename      string `json:"filename"`
	Content       string `json:"content_base64"`
	Category      string `json:"category"`
	Approved      bool   `json:"approved"`
}
type Batch struct {
	Documents []Document `json:"documents"`
}
type prepared struct {
	Document
	ID, Hash, Key, Type string
	Data                []byte
}
type Server struct {
	db         *pgxpool.Pool
	s3         *minio.Client
	bucket     string
	keys       map[string]Principal
	slots      chan struct{}
	mu         sync.Mutex
	rates      map[string]*rate
	accepted   prometheus.Counter
	duplicates prometheus.Counter
}
type rate struct {
	start time.Time
	count int
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func checksum(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func safeFilename(s string) (string, error) {
	s = strings.ReplaceAll(s, "\\", "/")
	name := filepath.Base(s)
	if name != s || name == "." || len(name) > 180 || strings.ContainsAny(name, "\x00\r\n") {
		return "", errors.New("invalid filename")
	}
	return name, nil
}
func prepare(d Document, org string) (prepared, error) {
	name, err := safeFilename(d.Filename)
	if err != nil {
		return prepared{}, err
	}
	ext := strings.ToLower(filepath.Ext(name))
	allowed := map[string]bool{".pdf": true, ".txt": true, ".docx": true, ".csv": true, ".json": true, ".png": true, ".jpg": true, ".jpeg": true}
	if !allowed[ext] || d.SourceID == "" || len(d.SourceID) > 200 || d.Category == "" {
		return prepared{}, errors.New("invalid source, category or extension")
	}
	if d.SourceVersion != nil && *d.SourceVersion < 1 {
		return prepared{}, errors.New("invalid source version")
	}
	data, err := base64.StdEncoding.DecodeString(d.Content)
	if err != nil || len(data) == 0 || len(data) > 8*1024*1024 {
		return prepared{}, errors.New("invalid content or file exceeds 8 MiB")
	}
	if ext == ".pdf" && !bytes.HasPrefix(data, []byte("%PDF-")) {
		return prepared{}, errors.New("invalid PDF signature")
	}
	if ext == ".docx" && !bytes.HasPrefix(data, []byte("PK")) {
		return prepared{}, errors.New("invalid DOCX signature")
	}
	if ext == ".png" && !bytes.HasPrefix(data, []byte("\x89PNG\r\n\x1a\n")) {
		return prepared{}, errors.New("invalid PNG signature")
	}
	if (ext == ".jpg" || ext == ".jpeg") && !bytes.HasPrefix(data, []byte{255, 216, 255}) {
		return prepared{}, errors.New("invalid JPEG signature")
	}
	hash := checksum(data)
	id := uuid.NewSHA1(uuid.NameSpaceOID, []byte(org+"\x00"+d.SourceID+"\x00"+hash)).String()
	return prepared{Document: d, ID: id, Hash: hash, Key: "raw/source=reference/org=" + org + "/" + id + "/source", Type: strings.TrimPrefix(ext, "."), Data: data}, nil
}
func respond(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, code int, msg string) {
	respond(w, code, map[string]string{"error": msg})
}
func (s *Server) auth(next func(http.ResponseWriter, *http.Request, Principal)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			fail(w, 401, "bearer API key required")
			return
		}
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		var p Principal
		ok := false
		for key, value := range s.keys {
			if subtle.ConstantTimeCompare([]byte(key), []byte(token)) == 1 {
				p = value
				ok = true
			}
		}
		if !ok {
			fail(w, 401, "invalid API key")
			return
		}
		scope := "read"
		if r.Method == "POST" {
			scope = "write"
		}
		permitted := false
		for _, v := range p.Scopes {
			if v == scope {
				permitted = true
			}
		}
		if !permitted {
			fail(w, 403, "insufficient scope")
			return
		}
		s.mu.Lock()
		entry := s.rates[token]
		if entry == nil || time.Since(entry.start) > time.Minute {
			entry = &rate{start: time.Now()}
			s.rates[token] = entry
		}
		entry.count++
		limited := entry.count > 600
		s.mu.Unlock()
		if limited {
			fail(w, 429, "rate limit exceeded")
			return
		}
		select {
		case s.slots <- struct{}{}:
			defer func() { <-s.slots }()
		default:
			fail(w, 503, "admission capacity exceeded")
			return
		}
		trace := uuid.NewString()
		w.Header().Set("X-Trace-ID", trace)
		ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
		defer cancel()
		slog.Info("request", "trace_id", trace, "org_id", p.Org, "method", r.Method, "route", r.Pattern)
		next(w, r.WithContext(ctx), p)
	}
}
func (s *Server) create(w http.ResponseWriter, r *http.Request, p Principal) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 128 {
		fail(w, 400, "Idempotency-Key required (max 128)")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 32*1024*1024)
	var batch Batch
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if r.URL.Path == "/api/v1/documents" {
		var d Document
		if err := dec.Decode(&d); err != nil {
			fail(w, 400, "invalid JSON document")
			return
		}
		batch.Documents = []Document{d}
	} else if err := dec.Decode(&batch); err != nil {
		fail(w, 400, "invalid JSON batch")
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		fail(w, 400, "multiple JSON values")
		return
	}
	if len(batch.Documents) == 0 || len(batch.Documents) > 1000 {
		fail(w, 400, "batch requires 1..1000 documents")
		return
	}
	canonical, _ := json.Marshal(batch)
	requestHash := checksum(canonical)
	ctx := r.Context()
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		fail(w, 503, "registry unavailable")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Serialize equal keys across concurrent requests, including object publication.
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", p.Org+":"+key); err != nil {
		fail(w, 503, "registry lock failed")
		return
	}
	var oldID, oldHash string
	err = tx.QueryRow(ctx, "SELECT id::text,request_hash FROM batches WHERE org_id=$1 AND idempotency_key=$2", p.Org, key).Scan(&oldID, &oldHash)
	if err == nil {
		if oldHash != requestHash {
			fail(w, 409, "idempotency key payload conflict")
			return
		}
		respond(w, 200, map[string]string{"batch_id": oldID})
		return
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		fail(w, 503, "registry unavailable")
		return
	}
	items := make([]prepared, len(batch.Documents))
	for i, d := range batch.Documents {
		v, e := prepare(d, p.Org)
		if e != nil {
			fail(w, 400, e.Error())
			return
		}
		items[i] = v
	}
	// At most eight uploads per request, and four admitted requests globally.
	uploadCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan prepared)
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobs {
				_, e := s.s3.PutObject(uploadCtx, s.bucket, item.Key, bytes.NewReader(item.Data), int64(len(item.Data)), minio.PutObjectOptions{UserMetadata: map[string]string{"sha256": item.Hash}, ContentType: "application/octet-stream"})
				if e != nil {
					select {
					case errs <- e:
					default:
					}
					cancel()
				}
			}
		}()
	}
	for _, item := range items {
		select {
		case jobs <- item:
		case <-uploadCtx.Done():
		}
	}
	close(jobs)
	wg.Wait()
	close(errs)
	if len(errs) > 0 {
		fail(w, 503, "object storage unavailable")
		return
	}
	id := uuid.NewString()
	manifestKey := "manifests/batches/" + id + ".json"
	if _, err = tx.Exec(ctx, "INSERT INTO batches(id,org_id,idempotency_key,request_hash,manifest_key) VALUES($1,$2,$3,$4,$5)", id, p.Org, key, requestHash, manifestKey); err != nil {
		fail(w, 503, "batch registration failed")
		return
	}
	manifest := []map[string]string{}
	for _, item := range items {
		tag, e := tx.Exec(ctx, "INSERT INTO documents(id,org_id,source_id,source_type,original_filename,file_checksum,raw_key,category,approved,source_version) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT DO NOTHING", item.ID, p.Org, item.SourceID, item.Type, item.Filename, item.Hash, item.Key, item.Category, item.Approved, item.SourceVersion)
		if e != nil {
			fail(w, 503, "document registration failed")
			return
		}
		if tag.RowsAffected() == 0 {
			s.duplicates.Inc()
		}
		if _, e = tx.Exec(ctx, "INSERT INTO batch_documents VALUES($1,$2) ON CONFLICT DO NOTHING", id, item.ID); e != nil {
			fail(w, 503, "membership failed")
			return
		}
		if _, e = tx.Exec(ctx, "INSERT INTO processing_results(document_id,pipeline_version,status) VALUES($1,'v1','pending') ON CONFLICT DO NOTHING", item.ID); e != nil {
			fail(w, 503, "job registration failed")
			return
		}
		manifest = append(manifest, map[string]string{"document_id": item.ID, "raw_key": item.Key, "sha256": item.Hash})
	}
	body, _ := json.Marshal(map[string]any{"batch_id": id, "org_id": p.Org, "documents": manifest})
	if _, err = s.s3.PutObject(ctx, s.bucket, manifestKey, bytes.NewReader(body), int64(len(body)), minio.PutObjectOptions{ContentType: "application/json"}); err != nil {
		fail(w, 503, "manifest upload failed")
		return
	}
	if _, err = tx.Exec(ctx, "INSERT INTO audit_events(org_id,batch_id,event) VALUES($1,$2,'batch.accepted')", p.Org, id); err != nil {
		fail(w, 503, "audit failed")
		return
	}
	if err = tx.Commit(ctx); err != nil {
		fail(w, 503, "batch commit failed")
		return
	}
	s.accepted.Add(float64(len(items)))
	respond(w, 202, map[string]string{"batch_id": id, "status": "pending"})
}
func (s *Server) list(w http.ResponseWriter, r *http.Request, p Principal) {
	rows, err := s.db.Query(r.Context(), "SELECT id::text,status,created_at FROM batches WHERE org_id=$1 ORDER BY created_at DESC LIMIT 100", p.Org)
	if err != nil {
		fail(w, 503, "registry unavailable")
		return
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id, status string
		var created time.Time
		if err := rows.Scan(&id, &status, &created); err != nil {
			fail(w, 503, "registry read failed")
			return
		}
		out = append(out, map[string]any{"batch_id": id, "status": status, "created_at": created})
	}
	if rows.Err() != nil {
		fail(w, 503, "registry read failed")
		return
	}
	respond(w, 200, out)
}
func (s *Server) get(w http.ResponseWriter, r *http.Request, p Principal) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		fail(w, 400, "invalid identifier")
		return
	}
	var payload []byte
	query := `SELECT jsonb_build_object('batch_id',b.id,'status',b.status,'kind',b.kind,'metadata_records',(SELECT count(*) FROM metadata_records mr WHERE mr.batch_id=b.id),'manifest_key',b.manifest_key,'version_counts',
 (SELECT coalesce(jsonb_object_agg(pipeline_version,status_counts),'{}') FROM
  (SELECT pipeline_version,jsonb_object_agg(status,n) status_counts FROM
   (SELECT pr.pipeline_version,pr.status,count(*) n FROM batch_documents bd
    JOIN processing_results pr ON pr.document_id=bd.document_id WHERE bd.batch_id=b.id
    GROUP BY pr.pipeline_version,pr.status) counters GROUP BY pipeline_version) versions),
 'counts',(SELECT coalesce(jsonb_object_agg(status,n),'{}') FROM (SELECT pr.status,count(*) n FROM batch_documents bd JOIN processing_results pr ON pr.document_id=bd.document_id WHERE bd.batch_id=b.id AND pr.pipeline_version='v1' GROUP BY pr.status) c)) FROM batches b WHERE b.id=$1 AND b.org_id=$2`
	if strings.HasPrefix(r.URL.Path, "/api/v1/documents/") {
		query = `SELECT jsonb_build_object('document_id',d.id,'source_id',d.source_id,'file_checksum',d.file_checksum,'results',(SELECT jsonb_agg(jsonb_build_object('pipeline_version',pr.pipeline_version,'status',pr.status,'error_code',pr.error_code,'attempts',pr.attempts)) FROM processing_results pr WHERE pr.document_id=d.id)) FROM documents d WHERE d.id=$1 AND d.org_id=$2`
	}
	err := s.db.QueryRow(r.Context(), query, id, p.Org).Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		fail(w, 404, "not found")
		return
	}
	if err != nil {
		fail(w, 503, "registry unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(payload)
}
func (s *Server) action(w http.ResponseWriter, r *http.Request, p Principal) {
	id := r.PathValue("id")
	if _, err := uuid.Parse(id); err != nil {
		fail(w, 400, "invalid identifier")
		return
	}
	ctx := r.Context()
	tx, err := s.db.Begin(ctx)
	if err != nil {
		fail(w, 503, "registry unavailable")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status, kind string
	err = tx.QueryRow(ctx, "SELECT status,kind FROM batches WHERE id=$1 AND org_id=$2 FOR UPDATE", id, p.Org).Scan(&status, &kind)
	if errors.Is(err, pgx.ErrNoRows) {
		fail(w, 404, "not found")
		return
	}
	if err != nil {
		fail(w, 503, "registry unavailable")
		return
	}
	event := "batch.cancelled"
	next := "cancelled"
	if strings.HasSuffix(r.URL.Path, "/retry") {
		if kind == "metadata" {
			fail(w, 409, "metadata admission batches have no file processing jobs")
			return
		}
		if status == "processing" {
			fail(w, 409, "batch is processing")
			return
		}
		next = "pending"
		event = "batch.retried"
		_, err = tx.Exec(ctx, "UPDATE processing_results SET status='pending',attempt_limit=attempts+5,error_code=NULL,lease_until=NULL,claim_token=NULL WHERE document_id IN (SELECT document_id FROM batch_documents WHERE batch_id=$1) AND status IN ('failed','quarantined')", id)
	}
	if err == nil {
		_, err = tx.Exec(ctx, "UPDATE batches SET status=$1,updated_at=now() WHERE id=$2", next, id)
	}
	if err == nil {
		_, err = tx.Exec(ctx, "INSERT INTO audit_events(org_id,batch_id,event) VALUES($1,$2,$3)", p.Org, id, event)
	}
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err != nil {
		fail(w, 503, "state update failed")
		return
	}
	respond(w, 200, map[string]string{"batch_id": id, "status": next})
}
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx := context.Background()
	db, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		panic(err)
	}
	defer db.Close()
	s3, err := minio.New(env("S3_ENDPOINT", "minio:9000"), &minio.Options{Creds: credentials.NewStaticV4(os.Getenv("S3_ACCESS_KEY"), os.Getenv("S3_SECRET_KEY"), ""), Secure: env("S3_TLS", "false") == "true"})
	if err != nil {
		panic(err)
	}
	keys := map[string]Principal{}
	if err = json.Unmarshal([]byte(os.Getenv("API_KEYS")), &keys); err != nil || len(keys) == 0 {
		panic("API_KEYS must contain scoped organization keys")
	}
	for _, p := range keys {
		if !regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`).MatchString(p.Org) {
			panic("invalid organization identifier")
		}
	}
	s := &Server{db: db, s3: s3, bucket: env("S3_BUCKET", "herbintel-data"), keys: keys, slots: make(chan struct{}, 4), rates: map[string]*rate{}, accepted: prometheus.NewCounter(prometheus.CounterOpts{Name: "herbintel_documents_accepted_total", Help: "Accepted document submissions"}), duplicates: prometheus.NewCounter(prometheus.CounterOpts{Name: "herbintel_duplicates_total", Help: "Duplicate logical document versions"})}
	prometheus.MustRegister(s.accepted, s.duplicates)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { respond(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		c, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if db.Ping(c) != nil {
			fail(w, 503, "database unavailable")
			return
		}
		if _, e := s3.StatObject(c, s.bucket, ".ready", minio.StatObjectOptions{}); e != nil {
			fail(w, 503, "storage unavailable")
			return
		}
		respond(w, 200, map[string]string{"status": "ready"})
	})
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("POST /api/v1/records", s.auth(s.metadata))
	mux.HandleFunc("POST /api/v1/documents", s.auth(s.create))
	mux.HandleFunc("POST /api/v1/batches", s.auth(s.create))
	mux.HandleFunc("GET /api/v1/batches", s.auth(s.list))
	mux.HandleFunc("GET /api/v1/documents/{id}", s.auth(s.get))
	mux.HandleFunc("GET /api/v1/batches/{id}", s.auth(s.get))
	mux.HandleFunc("GET /api/v1/batches/{id}/status", s.auth(s.get))
	mux.HandleFunc("POST /api/v1/batches/{id}/retry", s.auth(s.action))
	mux.HandleFunc("POST /api/v1/batches/{id}/cancel", s.auth(s.action))
	srv := &http.Server{Addr: env("HTTP_ADDR", ":8080"), Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 100 * time.Second, WriteTimeout: 100 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16384}
	go func() {
		slog.Info("listening", "port", 8080)
		if e := srv.ListenAndServe(); e != nil && !errors.Is(e, http.ErrServerClosed) {
			slog.Error("server failed", "error", fmt.Sprint(e))
			os.Exit(1)
		}
	}()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	shutdown, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
}
