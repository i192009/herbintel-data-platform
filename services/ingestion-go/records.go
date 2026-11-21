package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/minio/minio-go/v7"
)

// Metadata admission is intentionally separate from file/document processing.
func (s *Server) metadata(w http.ResponseWriter, r *http.Request, p Principal) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 128 {
		fail(w, 400, "Idempotency-Key required")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 32*1024*1024)
	var body struct {
		Records []map[string]any `json:"records"`
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil || len(body.Records) == 0 || len(body.Records) > 90000 {
		fail(w, 400, "requires 1..90000 metadata records")
		return
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		fail(w, 400, "multiple JSON values")
		return
	}
	ids := make([]string, len(body.Records))
	payloads := make([]string, len(body.Records))
	for i, record := range body.Records {
		source, ok := record["source_id"].(string)
		if !ok || source == "" || len(source) > 200 {
			fail(w, 400, "metadata source_id required")
			return
		}
		revision, ok := record["source_version"].(string)
		if !ok || revision == "" {
			fail(w, 400, "source_version required")
			return
		}
		encoded, _ := json.Marshal(record)
		ids[i] = checksum([]byte(source + "\x00" + revision))
		payloads[i] = string(encoded)
	}
	encoded, _ := json.Marshal(body)
	hash := checksum(encoded)
	ctx := r.Context()
	tx, err := s.db.Begin(ctx)
	if err != nil {
		fail(w, 503, "registry unavailable")
		return
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", p.Org+":"+key); err != nil {
		fail(w, 503, "registry unavailable")
		return
	}
	var oldID, oldHash string
	err = tx.QueryRow(ctx, "SELECT id::text,request_hash FROM batches WHERE org_id=$1 AND idempotency_key=$2", p.Org, key).Scan(&oldID, &oldHash)
	if err == nil {
		if oldHash != hash {
			fail(w, 409, "idempotency payload conflict")
			return
		}
		respond(w, 200, map[string]any{"batch_id": oldID, "record_count": len(ids), "kind": "metadata_only"})
		return
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		fail(w, 503, "registry unavailable")
		return
	}
	id := uuid.NewString()
	objectKey := "raw/source=reference/org=" + p.Org + "/metadata/" + id + ".json"
	if _, err = s.s3.PutObject(ctx, s.bucket, objectKey, bytes.NewReader(encoded), int64(len(encoded)), minio.PutObjectOptions{ContentType: "application/json", UserMetadata: map[string]string{"sha256": hash}}); err != nil {
		fail(w, 503, "object storage unavailable")
		return
	}
	if _, err = tx.Exec(ctx, "INSERT INTO batches(id,org_id,idempotency_key,request_hash,status,manifest_key,kind) VALUES($1,$2,$3,$4,'completed',$5,'metadata')", id, p.Org, key, hash, objectKey); err != nil {
		fail(w, 503, "batch registration failed")
		return
	}
	tag, err := tx.Exec(ctx, "INSERT INTO metadata_records(org_id,record_id,batch_id,record) SELECT $1,x.id,$2,x.payload::jsonb FROM unnest($3::text[],$4::text[]) AS x(id,payload) ON CONFLICT DO NOTHING", p.Org, id, ids, payloads)
	if err != nil {
		fail(w, 503, "metadata registration failed")
		return
	}
	var conflicts int
	err = tx.QueryRow(ctx, "SELECT count(*) FROM metadata_records mr JOIN unnest($2::text[],$3::text[]) x(id,payload) ON mr.record_id=x.id WHERE mr.org_id=$1 AND mr.record!=x.payload::jsonb", p.Org, ids, payloads).Scan(&conflicts)
	if err != nil {
		fail(w, 503, "metadata reconciliation failed")
		return
	}
	if conflicts > 0 {
		fail(w, 409, "source version already has different metadata")
		return
	}
	if _, err = tx.Exec(ctx, "INSERT INTO audit_events(org_id,batch_id,event) VALUES($1,$2,'metadata.accepted')", p.Org, id); err != nil {
		fail(w, 503, "audit failed")
		return
	}
	if err = tx.Commit(ctx); err != nil {
		fail(w, 503, "metadata commit failed")
		return
	}
	respond(w, 202, map[string]any{"batch_id": id, "record_count": len(ids), "inserted_records": tag.RowsAffected(), "kind": "metadata_only", "fully_processed_pdfs": 0})
}
