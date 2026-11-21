PYTHON ?= python3

verify-dags:
	$(PYTHON) scripts/manage.py verify-dags
.PHONY: build test test-integration lint up down seed generate-documents generate-records ingest run-airflow validate-data export-dataset benchmark dbt-parse dbt-build dbt-test dbt-debug dbt-deps dbt-docs snowflake-verify acceptance failure-tests verify-dags
build test test-integration lint up down seed generate-documents generate-records ingest run-airflow validate-data export-dataset benchmark dbt-parse dbt-build dbt-test dbt-debug dbt-deps dbt-docs snowflake-verify acceptance failure-tests:
	$(PYTHON) scripts/manage.py $@ $(ARGS)
