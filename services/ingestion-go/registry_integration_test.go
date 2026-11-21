package main

import (
	"context"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresRegistryAuthorizationAndRetry(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL required for actual PostgreSQL test")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	batch, doc := uuid.NewString(), uuid.NewString()
	org := "test" + strings.ReplaceAll(uuid.NewString(), "-", "")
	_, err = db.Exec(ctx, "INSERT INTO batches(id,org_id,idempotency_key,request_hash,status) VALUES($1,$2,'registry-test','test','failed')", batch, org)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(ctx, "INSERT INTO documents(id,org_id,source_id,source_type,original_filename,file_checksum,raw_key,category) VALUES($1,$2,'test-source','txt','test.txt',$3,'raw/test','synthetic')", doc, org, strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(ctx, "INSERT INTO batch_documents VALUES($1,$2)", batch, doc)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(ctx, "INSERT INTO processing_results(document_id,pipeline_version,status,attempts,error_code) VALUES($1,'v1','failed',5,'Outage')", doc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, q := range []string{"DELETE FROM audit_events WHERE batch_id=$1", "DELETE FROM batch_documents WHERE batch_id=$1", "DELETE FROM batches WHERE id=$1"} {
			_, _ = db.Exec(ctx, q, batch)
		}
		_, _ = db.Exec(ctx, "DELETE FROM processing_results WHERE document_id=$1", doc)
		_, _ = db.Exec(ctx, "DELETE FROM documents WHERE id=$1", doc)
	})
	s := &Server{db: db, keys: map[string]Principal{"writer": {Org: org, Scopes: []string{"read", "write"}}, "reader": {Org: org, Scopes: []string{"read"}}, "other": {Org: "other", Scopes: []string{"read", "write"}}}, slots: make(chan struct{}, 4), rates: map[string]*rate{}}
	for _, tc := range []struct {
		method, token, action string
		code                  int
	}{{"GET", "other", "", 404}, {"GET", "invalid", "", 401}, {"GET", "writer", "", 200}, {"POST", "reader", "retry", 403}, {"POST", "writer", "retry", 200}} {
		req := httptest.NewRequest(tc.method, "/api/v1/batches/"+batch+"/"+tc.action, nil)
		req.SetPathValue("id", batch)
		req.Header.Set("Authorization", "Bearer "+tc.token)
		recorder := httptest.NewRecorder()
		handler := s.get
		if tc.method == "POST" {
			handler = s.action
		}
		s.auth(handler)(recorder, req)
		if recorder.Code != tc.code {
			t.Fatalf("%s %s: got %d %s", tc.method, tc.token, recorder.Code, recorder.Body.String())
		}
	}
	var status string
	var attempts int
	err = db.QueryRow(ctx, "SELECT status,attempts FROM processing_results WHERE document_id=$1", doc).Scan(&status, &attempts)
	if err != nil || status != "pending" || attempts != 5 {
		t.Fatalf("retry did not reset failed job: %s %d %v", status, attempts, err)
	}
}
