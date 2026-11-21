# Validation evidence

These results were measured locally on Windows ARM64 with Docker Desktop's Linux ARM64 engine. Native Python runs used the x64 interpreter. Results are environment-specific; remote GitHub workflows and live Snowflake execution are separate checks.

## Verified results

| Check | Actual result |
|---|---|
| Go formatting, vet and race tests | Passed in the Go container, including the PostgreSQL registry authorization/retry test |
| Python unit suite | 17 passed, five integration tests deselected |
| Python integration suite | Five passed against actual PostgreSQL, MinIO and OCR binaries |
| Ruff / mypy | Passed; mypy checked all 12 processing modules |
| Airflow DAG validation | All six DAGs imported and passed cycle validation |
| Other local DAG execution | Quality, training export, incremental ETL and historical backfill all succeeded; 40 `v2` results were created while preserving `v1` records |
| dbt parse | Passed with dbt Core 1.12.7 and dbt-snowflake 1.12.5 |
| Python dependency audit | No known vulnerabilities reported for the resolved lock at verification time |
| PostgreSQL schema | Initialization, transactions, foreign keys, status constraints and duplicate protections passed |
| Full Airflow acceptance | Passed: 1,000 submitted file records, 999 unique versions, 998 completed files, one quarantined malformed source, 994 curated records |
| Worker restart | Completed results and verified Parquet survived restart without reprocessing completed versions |
| PostgreSQL and MinIO outages | API readiness returned 503 during each outage and recovered after restart |
| Actual in-flight worker SIGKILL | Recovered 40 documents without duplicate versions; six active claims were observed before killing the worker |

The crash test explicitly advances the processing lease clock in the test database rather than waiting five real minutes. Already completed attempts remain unchanged. This is an actual process kill followed by expired-claim recovery, not an assertion that an arbitrary machine crash was reproduced.

Full orchestration evidence: [Airflow acceptance](evidence/acceptance-airflow.json) and [other DAG executions](evidence/dag-verification.json). Earlier independent evidence: [native acceptance](evidence/acceptance-native.json), [PostgreSQL checks](evidence/postgres-validation.json), and [outage/crash checks](evidence/failure-tests.json). The 40-document backfill completed in 8.821 seconds of actual DAG execution, measured from Airflow's recorded start and end timestamps.

## Measured benchmark

The instrumented Docker run admitted **90,000 metadata records in 6.3283 seconds**, or **14,221.89 records/second**. Batch p50 was 0.244 seconds and p95 was 0.354 seconds. The Go process consumed 2.29 CPU seconds, and its highest RSS observed across 27 samples was 40,787,968 bytes (38.9 MiB). These server measurements come from real Prometheus process metrics. Client CPU was 3.220 seconds and peak traced allocation was 1,477,429 bytes. See [the raw Compose benchmark](evidence/benchmark-metadata-compose.json).

The independent native metadata benchmark admitted **90,000 metadata records in 19.595 seconds**, or **4,593.02 records/second**, in 90 batches using four HTTP clients. Median batch latency was 0.328 seconds and p95 was 4.243 seconds. Client CPU consumption was 2.984 seconds; peak traced Python allocation during admission was 1,465,183 bytes. Traced allocations are not process RSS or server memory.

This benchmark performed durable Go admission, MinIO raw-object writes and PostgreSQL registration. It processed **zero PDFs**. See the [raw native benchmark report](evidence/benchmark-metadata-native.json). The native 1,000-file processing acceptance took 136.403 seconds; the recorded Airflow acceptance took 126.627 seconds including orchestration and verification. These wall-clock measurements are different workloads and are not interchangeable throughput claims.

## Verification boundaries

- AWS/Snowflake credentials were unavailable. Provisioning, S3 trust/notifications, live Snowpipe ingestion, actual warehouse row reconciliation and dbt build/test/docs execution remain unverified. No cloud resource was provisioned and no warehouse load is simulated.
- Parsing dbt with the installed Snowflake adapter validates the project graph/configuration; it does not prove the SQL executed on Snowflake.
- CI/CD workflow files are included in the private GitHub repository. This report records local verification; consult GitHub Actions for remote run status.
- Python dependency audit results do not constitute an audit of every operating-system package, container image or archived MinIO source dependency.
- The 15,001-document generator and chunked admission design support larger source corpora; a full 15,001-file OCR/extraction performance run has not been claimed.

Reproduce local checks with `python scripts/manage.py test`, `test-integration`, `failure-tests`, `verify-dags`, and `benchmark`. Reports under `artifacts/` are generated by real assertions. Reviewed evidence snapshots are retained under `docs/evidence/`. Actual screenshots are in [Grafana operations](screenshots/grafana-operations.png) and [Airflow acceptance](screenshots/airflow-acceptance.png).
