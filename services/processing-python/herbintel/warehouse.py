"""Real Snowflake connector and AWS S3 integration; no local Snowflake emulation."""

import io
import json
import os
import re
import subprocess
import time

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import snowflake.connector
from prometheus_client import Counter, Histogram

from .infra import canonical, database, digest, register_manifest
from .pipeline import records, results, verify

LOAD_TIME = Histogram("herbintel_warehouse_load_seconds", "Actual Snowflake loading duration")
DBT_FAILURES = Counter("herbintel_dbt_failures_total", "Failed dbt subprocesses")
TABLES = ("document_metadata", "extraction_results", "document_events", "processing_runs", "dataset_manifests")


def configured() -> bool:
    return bool(os.getenv("SNOWFLAKE_ACCOUNT") and os.getenv("SNOWFLAKE_USER"))


def identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("invalid SQL identifier")
    return value


def connect():
    if not configured():
        raise RuntimeError("live Snowflake credentials are not configured")
    args = {key: os.environ["SNOWFLAKE_" + key.upper()] for key in ("account", "user")}
    for key in ("role", "warehouse", "database", "schema", "authenticator"):
        if os.getenv("SNOWFLAKE_" + key.upper()):
            args[key] = os.environ["SNOWFLAKE_" + key.upper()]
    if os.getenv("SNOWFLAKE_PRIVATE_KEY_FILE"):
        args["private_key_file"] = os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"]
        if os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"):
            args["private_key_file_pwd"] = os.environ["SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"]
    elif os.getenv("SNOWFLAKE_PASSWORD"):
        args["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    return snowflake.connector.connect(
        **args,
        login_timeout=20,
        network_timeout=60,
        session_parameters={"QUERY_TAG": "herbintel", "STATEMENT_TIMEOUT_IN_SECONDS": 120},
    )


def staged_manifest(batch_id: str, org: str, version: str) -> dict:
    with database() as conn:
        row = conn.execute(
            """SELECT body FROM manifests WHERE batch_id=%s AND org_id=%s AND kind='warehouse'
            AND body->>'pipeline_version'=%s ORDER BY created_at DESC LIMIT 1""",
            (batch_id, org, version),
        ).fetchone()
    if not row:
        raise ValueError("warehouse staging manifest missing")
    return row["body"]


def stage(batch_id: str, org: str, version: str) -> dict:
    verify(batch_id, org, version)
    if not os.getenv("AWS_BUCKET"):
        raise ValueError("AWS_BUCKET required; MinIO is not a Snowpipe deployment")
    docs = records(batch_id, org, version)
    # Override processing-batch context for versions reused across multiple batches.
    docs = [{**row, "batch_id": batch_id} for row in docs]
    with database() as conn:
        manifests = [
            r["body"]
            for r in conn.execute(
                "SELECT body FROM manifests WHERE batch_id=%s AND org_id=%s AND kind IN ('processed','ml')",
                (batch_id, org),
            )
        ]
    payloads = {
        "document_metadata": docs,
        "extraction_results": docs,
        "document_events": [
            {
                **r,
                "event_id": digest(f"{batch_id}:{r['document_id']}:{version}".encode()),
                "event": "extraction.completed",
            }
            for r in docs
        ],
        "processing_runs": [
            {"batch_id": batch_id, "org_id": org, "pipeline_version": version, **results(batch_id, org, version)}
        ],
        "dataset_manifests": [
            {**m, "batch_id": batch_id, "org_id": org, "pipeline_version": version} for m in manifests
        ],
    }
    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
    objects = []
    for table, rows in payloads.items():
        sink = io.BytesIO()
        pq.write_table(
            pa.table({"payload": [canonical(r).decode() for r in rows]}, schema=pa.schema([("payload", pa.string())])),
            sink,
            compression="snappy",
        )
        data = sink.getvalue()
        key = f"warehouse/{table}/org={org}/batch={batch_id}/{version}/{digest(data)}.parquet"
        s3.put_object(
            Bucket=os.environ["AWS_BUCKET"],
            Key=key,
            Body=data,
            Metadata={"sha256": digest(data)},
            ServerSideEncryption="AES256",
        )
        if digest(s3.get_object(Bucket=os.environ["AWS_BUCKET"], Key=key)["Body"].read()) != digest(data):
            raise ValueError("AWS S3 upload verification failed")
        objects.append({"table": table, "key": key, "rows": len(rows), "sha256": digest(data)})
    return register_manifest(
        {"batch_id": batch_id, "org_id": org, "pipeline_version": version, "objects": objects},
        org,
        batch_id,
        "warehouse",
    )


def warehouse_step(step: str, batch_id: str, org: str, version: str) -> dict:
    if step == "configured":
        return {"configured": configured()}
    if step == "stage":
        result = stage(batch_id, org, version)
        db = identifier(os.getenv("SNOWFLAKE_DATABASE", "HERBINTEL"))
        with connect() as conn:
            cursor = conn.cursor()
            for obj in result["objects"]:
                path = obj["key"].removeprefix("warehouse/")
                # Keys contain only service-generated tenant IDs, UUIDs and hashes.
                cursor.execute(f"LIST @{db}.RAW.HERBINTEL_S3/{path}")
                if not cursor.fetchall():
                    raise ValueError("uploaded object not visible through Snowflake storage integration")
        return {"batch_id": batch_id, "manifest_key": result["manifest_key"]}
    if step == "dbt":
        dbt_dir = "/app/dbt/herbintel"
        for command in ("deps", "build"):
            dbt_process = subprocess.run(
                ["dbt", command, "--project-dir", dbt_dir, "--profiles-dir", dbt_dir, "--target", "cloud"],
                capture_output=True,
                timeout=600,
            )
            if dbt_process.returncode:
                DBT_FAILURES.inc()
                raise RuntimeError("dbt execution failed; inspect dbt logs in container")
        return {"dbt": "passed"}
    with connect() as conn:
        cursor = conn.cursor()
        if step == "connect":
            cursor.execute("SELECT CURRENT_ACCOUNT(),CURRENT_ROLE(),CURRENT_WAREHOUSE()")
            return {"connection": "verified", "context": list(cursor.fetchone())}
        manifest = staged_manifest(batch_id, org, version)
        db = identifier(os.getenv("SNOWFLAKE_DATABASE", "HERBINTEL"))
        stage_name = f"{db}.RAW.HERBINTEL_S3"
        if step == "load":
            strategy = os.getenv("SNOWFLAKE_LOAD_STRATEGY", "copy")
            with LOAD_TIME.time():
                for obj in manifest["objects"]:
                    table = identifier(obj["table"].upper())
                    path = obj["key"].removeprefix("warehouse/")
                    if strategy == "snowpipe":
                        cursor.execute("SELECT SYSTEM$PIPE_STATUS(%s)", (f"{db}.RAW.PIPE_{table}",))
                        pipe_status = json.loads(cursor.fetchone()[0])
                        if pipe_status.get("executionState") != "RUNNING":
                            raise ValueError("Snowpipe is not running")
                        # Rely on actual AWS ObjectCreated -> SQS notifications for auto-ingestion.
                        # REFRESH is reserved for the explicitly selected recovery strategy below.
                    elif strategy == "snowpipe_refresh":
                        cursor.execute(
                            f"ALTER PIPE {db}.RAW.PIPE_{table} REFRESH PREFIX = %s",
                            (path.removeprefix(obj["table"] + "/"),),
                        )
                    elif strategy == "copy":
                        cursor.execute(
                            f"COPY INTO {db}.RAW.{table}(payload,filename,loaded_at) FROM (SELECT PARSE_JSON($1:payload::string),METADATA$FILENAME,CURRENT_TIMESTAMP() FROM @{stage_name}) FILES=(%s) FILE_FORMAT=(FORMAT_NAME={db}.RAW.HERBINTEL_PARQUET) ON_ERROR=ABORT_STATEMENT",
                            (path,),
                        )
                    else:
                        raise ValueError("load strategy must be copy, snowpipe or snowpipe_refresh")
            return {"strategy": strategy, "files_submitted": len(manifest["objects"])}
        if step == "history":
            cursor.execute(
                f"SELECT FILE_NAME,STATUS,ROW_COUNT FROM TABLE({db}.INFORMATION_SCHEMA.COPY_HISTORY(TABLE_NAME=>'{db}.RAW.DOCUMENT_METADATA',START_TIME=>DATEADD('hour',-24,CURRENT_TIMESTAMP()))) WHERE FILE_NAME LIKE %s",
                (f"%batch={batch_id}/%",),
            )
            rows = cursor.fetchall()
            return {"history": [list(r) for r in rows]}
        if step == "verify":
            deadline = time.monotonic() + 120
            while True:
                counts = {}
                for obj in manifest["objects"]:
                    table = identifier(obj["table"].upper())
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {db}.RAW.{table} WHERE payload:batch_id::string=%s AND payload:org_id::string=%s AND payload:pipeline_version::string=%s AND filename LIKE %s",
                        (batch_id, org, version, "%" + obj["key"].removeprefix("warehouse/")),
                    )
                    counts[table] = cursor.fetchone()[0]
                if all(counts[o["table"].upper()] == o["rows"] for o in manifest["objects"]):
                    return {"verified": True, "counts": counts}
                if time.monotonic() >= deadline:
                    raise ValueError(f"Snowflake source/target row count mismatch: {counts}")
                time.sleep(3)
    raise ValueError("unknown warehouse step")
