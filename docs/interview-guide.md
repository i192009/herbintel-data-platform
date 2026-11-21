# Interview guide

1. **Why Go for admission?** Its HTTP server, contexts and bounded goroutines support concurrent uploads while keeping admission and cancellation explicit. CPU-heavy extraction remains a separate service.

2. **How is concurrency bounded?** Four ingestion requests are admitted, each with eight upload workers. Python admits two processing calls with six parser subprocesses each. Airflow also caps active tasks and runs.

3. **How does idempotency work?** A tenant-scoped key and canonical request hash are stored in PostgreSQL under an advisory transaction lock. Equal replays return the original batch; conflicting replays return 409.

4. **How do you distinguish duplicates from updated sources?** Organization/source/file-checksum identifies an immutable document version. Content checksums detect equivalent normalized text, while optional source revisions identify updated content for exports.

5. **What happens if storage succeeds but the database commit fails?** Unreferenced objects may remain, but no batch is reported accepted. A retry reuses deterministic object paths and registers one logical document version.

6. **How are workers recovered after a crash?** Durable result rows contain a lease and claim UUID. Expired work can be reclaimed; a stale claimant cannot overwrite the current result. Completed versions are reused.

7. **Why not pass extracted text through XCom?** It increases metadata-database pressure and exposes document bodies to orchestration logs. Tasks exchange batch IDs and object references instead.

8. **How are native PDFs and scans handled?** Native text is extracted first. Sparse pages render through Poppler and run through Tesseract. Page, resource and time bounds constrain parser cost.

9. **Why Pydantic and Arrow?** Pydantic validates the processing contract; Arrow supplies an explicit, typed Parquet schema, including empty splits. A schema version makes downstream expectations visible.

10. **What blocks publication?** Unaccounted records, unfinished work, invalid schemas and critical consistency failures. Known quarantines remain visible while valid records may form a partial dataset.

11. **How is object publication atomic?** Objects upload and are read back before the registry commits a checksum manifest. Readers use registered manifests, so a partially uploaded prefix is not treated as a dataset.

12. **What makes an ML export reproducible?** Selected document/pipeline versions, hashes, filters, seed and schema version determine the dataset version. The manifest records splits, object checksums and lineage.

13. **How do you prevent leakage?** Union-find joins shared source IDs and identical content before deduplication. Connected documents receive one deterministic split, including transitive duplicate relationships.

14. **What is the difference between S3 and Snowpipe?** S3 stores the files; Snowpipe loads them into Snowflake after cloud notifications. A functioning local MinIO bucket does not validate the AWS notification chain.

15. **How do storage integrations improve security?** Snowflake assumes a restricted AWS IAM role using an external ID. Application code avoids embedding permanent AWS keys in stage SQL.

16. **Why use arrival time for incremental dbt models?** Source events may arrive late. Filtering on warehouse arrival time includes those events even if their business timestamp is old. A lookback also covers recent corrections.

17. **How do you handle nondeterministic MERGE input?** Staging selects one row per tenant/document/pipeline key before incremental MERGE. Composite uniqueness tests catch violations. Raw load history remains available for diagnosis.

18. **How do you control Snowflake cost?** Use XSMALL with auto-suspend, bounded verification batches and explicit provisioning. Monitor serverless Snowpipe cost separately from warehouse resource monitors.

19. **What does the 90,000-record benchmark prove?** Metadata HTTP admission, raw-object persistence and database registration throughput for the measured environment. It does not establish 90,000-PDF OCR throughput or clinical data quality.

20. **What is needed before shared production use?** Live cloud and orchestration acceptance, managed secrets/TLS, restricted Airflow database ownership, reviewed tenant worker isolation, load testing, image scanning and operational retention/recovery procedures.
