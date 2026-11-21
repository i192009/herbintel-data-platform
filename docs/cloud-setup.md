# AWS and Snowflake runbook

These commands can create billable resources. They are explicit operator steps and were not automatically executed during implementation. Use only a non-sensitive synthetic test bucket and a dedicated Snowflake environment.

## Prerequisites

Provide an AWS account with permission to create a private S3 bucket, an IAM policy/role and S3 event notifications, plus a Snowflake provisioning identity authorized to create roles, database, warehouse and storage integration. Use an S3 bucket in a compatible region. Keep Block Public Access enabled, use encryption and consider bucket policies enforcing TLS. Raw local reference files are not automatically uploaded to a public bucket.

Set `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, authentication settings, `AWS_BUCKET`, `AWS_REGION` and `AWS_ROLE_ARN` in your secret environment. Password authentication is supported; key-pair authentication uses `SNOWFLAKE_PRIVATE_KEY_FILE` and optional passphrase. For containers, mount the key read-only at the specified **container** path through a local Compose override. Never copy the private key into an image or commit it. External-browser authentication is available to an interactive connector session, not unattended CI.

## Provision in this order

1. Create the private AWS bucket. The warehouse loader writes only under `warehouse/`; give its AWS identity PutObject/GetObject on that prefix. Snowflake's IAM role receives the narrower read policy in `snowflake/security/aws-policy.json`.
2. Execute `snowflake/setup/001_bootstrap.sql` as the provisioning administrator. Grant the created roles to the designated service users using your identity-management process.
3. Create the AWS read role with an initial trust policy under operator control. Render only the non-secret `${AWS_BUCKET}` and `${AWS_ROLE_ARN}` placeholders in `snowflake/stages/001_s3.sql`, then execute it.
4. Read `DESC INTEGRATION HERBINTEL_S3_ACCESS`. Use the returned `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` in the trust policy template. Apply it to the AWS role. The external-ID condition is required.
5. Execute the five files in `snowflake/schemas/` and the five files in `snowflake/pipes/`.
6. For every pipe, inspect `SHOW PIPES ...` and take its `notification_channel` SQS ARN. Configure AWS S3 `ObjectCreated:*` notifications for `warehouse/<table>/` and suffix `.parquet` to that Snowflake queue. Preserve existing unrelated bucket notification rules. Do not overwrite another application's configuration.
7. Verify the stage with `LIST @HERBINTEL.RAW.HERBINTEL_S3`. Inspect `SYSTEM$PIPE_STATUS` and loading history using `snowflake/setup/verify.sql`.
8. If using the dedicated dbt test target, explicitly provision `HERBINTEL_TEST` with `snowflake/setup/002_test_database.sql`. Clone after the required RAW data is present, or load synthetic fixtures into that isolated database.

Snowflake documents this trust and event wiring in [Configuring Snowpipe for S3](https://docs.snowflake.com/en/user-guide/data-load-snowpipe-auto-s3). The integration's external stage is genuine AWS S3, rather than MinIO emulation.

## Verify real ingestion

After a local acceptance batch is available, configure credentials and run:

```bash
make snowflake-verify
```

The command validates the actual Snowflake session, uploads Parquet envelopes to AWS, reads back their bytes, confirms visibility through the external stage, loads or waits for the configured strategy, inspects history, compares all five source/target row counts and executes dbt. A source manifest identifies the exact uploaded files and expected counts. Results are written only after successful execution.

Use `SNOWFLAKE_LOAD_STRATEGY=snowpipe` to test the actual notification path. Use `copy` for explicit COPY testing. `snowpipe_refresh` is an operator recovery mode; it does not establish that AWS notifications work. Snowflake explains the relative prefix and limited recovery purpose in [ALTER PIPE REFRESH](https://docs.snowflake.com/en/sql-reference/sql/alter-pipe).

The loader connector uses `SNOWFLAKE_ROLE=HERBINTEL_LOADER`; dbt uses `SNOWFLAKE_DBT_ROLE=HERBINTEL_TRANSFORMER`. Grant both roles to a shared service user only if appropriate, or configure separate credentials in your deployment. `SNOWFLAKE_DATABASE` can select a different database at runtime, but SQL provisioning must be consistently rendered for the same database. The default scripts intentionally create only the named HerbIntel objects.

dbt commands:

```bash
make dbt-debug
make dbt-deps
make dbt-parse
make dbt-build
make dbt-test
make dbt-docs
# Explicit cloud target after operator provisioning:
make dbt-build ARGS="--target cloud"
```

`dbt parse` creates graph artifacts without a live connection. `debug`, `build`, `test`, source freshness queries and documentation catalog generation require real warehouse access. Successful parsing alone does not validate compiled Snowflake SQL against a live warehouse.

## Cost and teardown

The warehouse starts suspended, is XSMALL, and auto-suspends after 60 seconds. An optional resource-monitor statement is included, but a warehouse resource monitor does not cap serverless Snowpipe charges. Limit test object volume and inspect account usage separately.

After export/retention review, an authorized operator can execute `snowflake/setup/teardown.sql`. It drops the named database, warehouse, integration and roles. The test database, AWS notifications, IAM policies/roles and S3 objects require separate explicit cleanup. Removing the application containers does not remove AWS or Snowflake resources.

Live validation must remain marked pending when credentials, trust configuration or event notifications are unavailable. No local fixture is a replacement for that verification.
