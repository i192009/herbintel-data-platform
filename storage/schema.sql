CREATE TABLE batches (
 id uuid PRIMARY KEY, org_id text NOT NULL, idempotency_key text NOT NULL,
 request_hash text NOT NULL, status text NOT NULL DEFAULT 'pending'
 CHECK(status IN ('pending','processing','completed','partial','failed','cancelled')),
 manifest_key text NOT NULL DEFAULT '', kind text NOT NULL DEFAULT 'files' CHECK(kind IN ('files','metadata')),
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now(), UNIQUE(org_id,idempotency_key)
);
CREATE TABLE documents (
 id uuid PRIMARY KEY, org_id text NOT NULL, source_id text NOT NULL,
 source_type text NOT NULL, source_version integer CHECK(source_version>0),
 original_filename text NOT NULL, file_checksum text NOT NULL,
 raw_key text NOT NULL, category text NOT NULL, approved boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT now(), UNIQUE(org_id,source_id,file_checksum)
);
CREATE TABLE batch_documents (
 batch_id uuid REFERENCES batches(id), document_id uuid REFERENCES documents(id),
 PRIMARY KEY(batch_id,document_id)
);
CREATE TABLE metadata_records (
 org_id text NOT NULL, record_id text NOT NULL, batch_id uuid REFERENCES batches(id),
 record jsonb NOT NULL, PRIMARY KEY(org_id,record_id)
);
CREATE TABLE processing_results (
 document_id uuid REFERENCES documents(id), pipeline_version text NOT NULL,
 status text NOT NULL CHECK(status IN ('pending','processing','completed','quarantined','failed')),
 attempts integer NOT NULL DEFAULT 0, attempt_limit integer NOT NULL DEFAULT 5,
 lease_until timestamptz, claim_token uuid,
 record jsonb, error_code text, updated_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(document_id,pipeline_version)
);
CREATE TABLE manifests (
 id text PRIMARY KEY, org_id text NOT NULL, batch_id uuid REFERENCES batches(id),
 kind text NOT NULL, object_key text NOT NULL, checksum text NOT NULL,
 body jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE quality_results (
 id bigserial PRIMARY KEY, batch_id uuid REFERENCES batches(id), run_id uuid NOT NULL,
 outcome text NOT NULL CHECK(outcome IN ('PASS','FAIL','WARNING','INCONCLUSIVE')),
 checks jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE audit_events (
 id bigserial PRIMARY KEY, org_id text NOT NULL, batch_id uuid,
 event text NOT NULL, details jsonb NOT NULL DEFAULT '{}', created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX pending_results ON processing_results(status,lease_until);
CREATE INDEX org_batches ON batches(org_id,created_at DESC);
CREATE INDEX document_org ON documents(org_id);
\getenv ingest_password INGEST_DB_PASSWORD
\getenv worker_password WORKER_DB_PASSWORD
SELECT format('CREATE ROLE herb_ingest LOGIN PASSWORD %L', :'ingest_password')
 WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname='herb_ingest') \gexec
SELECT format('CREATE ROLE herb_worker LOGIN PASSWORD %L', :'worker_password')
 WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname='herb_worker') \gexec
SELECT format('GRANT CONNECT ON DATABASE %I TO herb_ingest,herb_worker', current_database()) \gexec
GRANT USAGE ON SCHEMA public TO herb_ingest,herb_worker;
GRANT SELECT,INSERT,UPDATE ON batches,documents,batch_documents,processing_results,audit_events,metadata_records TO herb_ingest;
GRANT SELECT ON manifests,quality_results TO herb_ingest;
GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA public TO herb_worker;
GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO herb_ingest,herb_worker;
SELECT 'CREATE DATABASE airflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname='airflow') \gexec
