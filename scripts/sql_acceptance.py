"""Validate the real PostgreSQL schema without substituting another database engine."""

import argparse
import json
import os
import subprocess
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--psql", default="psql")
parser.add_argument("--url", required=True)
args = parser.parse_args()


def sql(text, expect_success=True):
    result = subprocess.run(
        [args.psql, "-d", args.url, "-X", "-v", "ON_ERROR_STOP=1", "-tAc", text], capture_output=True, text=True
    )
    assert (result.returncode == 0) == expect_success, result.stderr
    return result.stdout.strip()


result = subprocess.run(
    [args.psql, "-d", args.url, "-X", "-v", "ON_ERROR_STOP=1", "-f", "storage/schema.sql"],
    capture_output=True,
    text=True,
    env=os.environ,
)
assert result.returncode == 0, result.stderr
batch = "00000000-0000-4000-8000-000000000001"
doc = "00000000-0000-4000-8000-000000000002"
insert_batch = f"INSERT INTO batches(id,org_id,idempotency_key,request_hash) VALUES('{batch}','sql-test','one','hash');"
insert_doc = f"INSERT INTO documents(id,org_id,source_id,source_type,original_filename,file_checksum,raw_key,category,source_version) VALUES('{doc}','sql-test','source','txt','test.txt','sha256','raw/test','synthetic',1);"
sql(
    "BEGIN;"
    + insert_batch
    + insert_doc
    + f"INSERT INTO batch_documents VALUES('{batch}','{doc}'); INSERT INTO processing_results(document_id,pipeline_version,status) VALUES('{doc}','v1','pending'); ROLLBACK;"
)
sql("BEGIN;" + insert_batch + insert_batch + "ROLLBACK;", expect_success=False)
sql(
    f"INSERT INTO batches(id,org_id,idempotency_key,request_hash,status) VALUES('{batch}','sql-test','bad','hash','imaginary');",
    expect_success=False,
)
sql(
    f"INSERT INTO processing_results(document_id,pipeline_version,status) VALUES('{doc}','v1','pending');",
    expect_success=False,
)
sql(
    "BEGIN;" + insert_doc + insert_doc.replace(doc, "00000000-0000-4000-8000-000000000003") + "ROLLBACK;",
    expect_success=False,
)
assert sql("SELECT count(*) FROM information_schema.tables WHERE table_schema='public'") == "8"
report = {
    "status": "PASS",
    "engine": "PostgreSQL",
    "checks": [
        "final schema initialization",
        "valid batch/document/result transaction",
        "idempotency uniqueness",
        "status constraint",
        "result foreign key",
        "logical source/checksum uniqueness",
        "eight operational tables",
    ],
    "acceptance_scope": "database only; Go, MinIO, Airflow and warehouse are not exercised",
}
Path("artifacts").mkdir(exist_ok=True)
Path("artifacts/postgres-validation.json").write_text(json.dumps(report, indent=2))
print(json.dumps(report, indent=2))
