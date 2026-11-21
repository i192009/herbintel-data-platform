# Implementation status

This is a new HerbIntel portfolio implementation, not a claim about capabilities deployed by the real HerbIntel organization.

| Area | Implementation | Verification |
|---|---|---|
| Go admission, tenant/scoped keys, idempotency, retry/cancel | Complete | Formatting, vet and race tests passed, including actual PostgreSQL registry tests |
| PostgreSQL registry and constraints | Complete | Actual PostgreSQL schema/constraint checks passed |
| Python extraction and OCR | Complete | 17 unit tests, five service integration tests, Ruff and mypy passed |
| Private storage, Parquet, quality and versioned exports | Complete | Native and Airflow 1,000-file acceptance passed: 998 completed, one quarantined, 994 curated rows; two actual PDFs processed |
| Metadata-only admission benchmark | Complete | Instrumented Compose run: 90,000 records in 6.3283 seconds, 14,221.89 records/second; zero processed PDFs |
| Airflow, Docker, dashboards and local operations | Complete | All six DAGs parsed; all five local DAGs executed successfully; actual Grafana and Airflow screenshots captured |
| Retries, backfill and failure recovery | Complete | PostgreSQL/MinIO outage recovery, actual in-flight worker SIGKILL recovery, and a 40-document `v2` backfill preserving `v1` all passed |
| Snowflake, AWS stage, five Snowpipes and dbt | Complete integration source | dbt parsing passed; live cloud verification requires credentials |
| CI/CD | Workflows implemented | Remote run status is tracked in GitHub Actions; recorded validation evidence covers local execution |
| Dependency scanning | Locked Python dependency set | pip-audit reported no known vulnerabilities at verification time |

The local platform is delivered and running. Detailed evidence is maintained in `docs/validation.md` and `docs/evidence/`. Live AWS/Snowflake provisioning, notification ingestion and dbt execution remain unverified because credentials were unavailable. No paid cloud resources were created. The repository is uploaded privately to [i192009/herbintel-data-platform](https://github.com/i192009/herbintel-data-platform) with the project owner's authorization. Local environment files, generated datasets, caches and runtime artifacts are excluded.
