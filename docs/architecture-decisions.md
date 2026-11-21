# Architectural decisions

- **PostgreSQL is the job authority.** State transitions, batch membership, cumulative attempts and publication manifests are durable. Prometheus counters are operational observations rather than a billing ledger.
- **Upload before registry commit.** This avoids accepted rows referring to objects never written. Failed transactions can leave harmless unreferenced objects; retention and reconciliation remain operator responsibilities.
- **Content-addressed outputs plus a commit manifest.** Text paths include their content checksum. Dataset consumers resolve registered, verified manifests; object-store listing consistency is not a commit protocol.
- **Immutable versions.** File content identifies a source version, and pipeline revisions identify reprocessing. Backfills create new result rows and objects. An explicit retry addresses failed work without replacing prior completed output.
- **Parser isolation.** Untrusted files run outside the API process with bounded resources. Local synthetic data is still parsed defensively. Linux limits and non-root containers provide additional controls, not a guarantee against all parser vulnerabilities.
- **No warehouse emulator.** The local system stops at MinIO/Parquet when cloud credentials are absent. Real Snowflake connector methods, S3 IAM templates, external-stage DDL, pipes and dbt execution are kept on an explicit cloud path.
- **One configured organization per worker identity.** Ingestion supports multiple organizations, while the internal worker rejects other organization headers. Scale by deploying reviewed worker identities/queues rather than accepting arbitrary organization headers with a shared key.
- **LocalExecutor for local reproducibility.** The project needs no message broker. PostgreSQL leases handle durable processing claims independently of Airflow retries.
- **Separate metadata benchmark.** Metadata records have a dedicated admission path and persistence table. They never increment the count of fully processed files or PDFs.
- **Explicit verification boundaries.** Native-service testing, Docker startup, actual Airflow execution, dbt parsing, live SQL execution and cloud auto-ingestion are separate acceptance claims.
