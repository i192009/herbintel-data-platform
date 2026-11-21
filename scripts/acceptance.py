"""Actual Compose acceptance, executed in the worker container. No cloud emulation."""

import io
import json
import os
import time
import uuid
from pathlib import Path

import pyarrow.parquet as pq
import requests
from herbintel.infra import database, digest, get
from herbintel.synthetic import documents

INGEST = os.getenv("INGEST_URL", "http://localhost:8080")
WORKER = os.getenv("WORKER_URL", "http://localhost:8000")
HEADERS = {"Authorization": "Bearer " + os.getenv("API_KEY", "local-demo-write-key")}
INTERNAL = {"Authorization": "Bearer " + os.environ["WORKER_API_KEY"], "X-Org-ID": "demo"}


def post(path, body):
    response = requests.post(WORKER + path, json=body, headers=INTERNAL, timeout=900)
    assert response.ok, (path, response.status_code, response.text[:300])
    return response.json()


def run(count=1000, via_airflow=True):
    start = time.perf_counter()
    key = "acceptance-" + uuid.uuid4().hex
    # Isolate acceptance runs while preserving deterministic corpus contents and duplicate relationships.
    items = [{**row, "source_id": key + ":" + row["source_id"]} for row in documents(count)]
    # A duplicate accepted submission is represented once in batch membership.
    items.append(items[0])
    # Maximum file batch admission is 1000, so keep exactly requested count.
    items.pop(2)
    response = requests.post(
        INGEST + "/api/v1/batches", headers={**HEADERS, "Idempotency-Key": key}, json={"documents": items}, timeout=120
    )
    assert response.status_code == 202, response.text
    batch = response.json()["batch_id"]
    request = {"batch_id": batch, "pipeline_version": "v1"}
    replay = requests.post(
        INGEST + "/api/v1/batches", headers={**HEADERS, "Idempotency-Key": key}, json={"documents": items}, timeout=120
    )
    assert replay.status_code == 200 and replay.json()["batch_id"] == batch
    conflict = requests.post(
        INGEST + "/api/v1/batches",
        headers={**HEADERS, "Idempotency-Key": key},
        json={"documents": items[:1]},
        timeout=30,
    )
    assert conflict.status_code == 409
    invalid = {k: v for k, v in items[0].items() if k != "source_id"}
    rejected = requests.post(
        INGEST + "/api/v1/documents", headers={**HEADERS, "Idempotency-Key": key + "-missing"}, json=invalid, timeout=30
    )
    assert rejected.status_code == 400
    unsafe = {**items[0], "filename": "../unsafe.txt"}
    rejected = requests.post(
        INGEST + "/api/v1/documents", headers={**HEADERS, "Idempotency-Key": key + "-unsafe"}, json=unsafe, timeout=30
    )
    assert rejected.status_code == 400
    assert (
        requests.get(
            INGEST + f"/api/v1/batches/{batch}", headers={"Authorization": "Bearer local-other-key"}, timeout=10
        ).status_code
        == 404
    )
    assert (
        requests.post(
            INGEST + "/api/v1/batches",
            headers={"Authorization": "Bearer local-read-key", "Idempotency-Key": key},
            json={"documents": items[:1]},
            timeout=10,
        ).status_code
        == 403
    )
    with database() as conn:
        assert (
            conn.execute("SELECT count(*) n FROM batch_documents WHERE batch_id=%s", (batch,)).fetchone()["n"]
            == count - 1
        )
    dag_run = None
    if via_airflow:
        airflow = os.getenv("AIRFLOW_URL", "http://airflow-web:8080")
        auth = (os.getenv("AIRFLOW_USERNAME", "admin"), os.getenv("AIRFLOW_PASSWORD", "local-airflow-change-me"))
        requests.patch(
            airflow + "/api/v1/dags/herbintel_document_ingestion", auth=auth, json={"is_paused": False}, timeout=30
        ).raise_for_status()
        dag_run = "acceptance__" + uuid.uuid4().hex
        trigger = requests.post(
            airflow + "/api/v1/dags/herbintel_document_ingestion/dagRuns",
            auth=auth,
            json={"dag_run_id": dag_run, "conf": request},
            timeout=30,
        )
        assert trigger.ok, trigger.text
        deadline = time.monotonic() + 1200
        while time.monotonic() < deadline:
            status = requests.get(
                airflow + f"/api/v1/dags/herbintel_document_ingestion/dagRuns/{dag_run}", auth=auth, timeout=30
            )
            status.raise_for_status()
            state = status.json()["state"]
            print(json.dumps({"batch_id": batch, "airflow_state": state}), flush=True)
            if state == "success":
                break
            assert state != "failed", status.text
            time.sleep(5)
        else:
            raise AssertionError("Airflow acceptance timeout")
    else:
        for operation in ("validate", "process", "quality", "publish", "verify", "finalize"):
            post("/internal/" + operation, request)
    result = post("/internal/results", request)
    assert result["counts"].get("quarantined") == 1, result
    assert result["counts"].get("completed") == count - 2, result
    verified = post("/internal/verify", request)
    dataset = post("/datasets/export", {**request, "seed": 42, "dataset_id": "acceptance"})
    assert dataset["rows"] < verified["rows"]
    duplicate_export = post("/datasets/export", {**request, "seed": 42, "dataset_id": "acceptance"})
    assert duplicate_export["dataset_version"] == dataset["dataset_version"]
    content_splits, source_splits = {}, {}
    for obj in dataset["objects"]:
        data = get(obj["key"])
        assert digest(data) == obj["sha256"]
        table = pq.read_table(io.BytesIO(data))
        assert table.num_rows == obj["rows"]
        for row in table.to_pylist():
            assert row["approved"]
            assert content_splits.setdefault(row["content_checksum"], obj["split"]) == obj["split"]
            assert source_splits.setdefault(row["source_id"], obj["split"]) == obj["split"]
    # Repeated work must preserve completed logical versions and attempts.
    post("/internal/process", request)
    with database() as conn:
        rows = conn.execute(
            """SELECT pr.status,pr.attempts,pr.record FROM processing_results pr
                             JOIN batch_documents bd ON bd.document_id=pr.document_id WHERE bd.batch_id=%s AND pipeline_version='v1'""",
            (batch,),
        ).fetchall()
        assert all(r["attempts"] == 1 for r in rows)
        assert any(r["record"] and "ocr" in r["record"]["extraction_method"] for r in rows)
    metrics = requests.get(WORKER + "/metrics", timeout=30)
    assert metrics.ok and "herbintel_documents_processed_total" in metrics.text
    cloud = post("/warehouse/configured", request)
    if cloud["configured"]:
        for step in ("connect", "stage", "load", "history", "verify", "dbt"):
            post("/warehouse/" + step, request)
    report = {
        "status": "PASS",
        "batch_id": batch,
        "submitted_file_records": count,
        "unique_file_versions": count - 1,
        "completed_files": count - 2,
        "fully_processed_pdfs": sum(bool(r["record"] and r["record"]["source_type"] == "pdf") for r in rows),
        "quarantined_files": 1,
        "dataset_rows": dataset["rows"],
        "dataset_version": dataset["dataset_version"],
        "airflow_run_id": dag_run,
        "snowflake": "verified" if cloud["configured"] else "NOT_RUN_NO_CREDENTIALS",
        "elapsed_seconds": round(time.perf_counter() - start, 3),
        "pipeline_version": "v1",
    }
    Path("artifacts").mkdir(exist_ok=True)
    Path("artifacts/acceptance.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2), flush=True)
    return report


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument(
        "--direct", action="store_true", help="Process without Airflow; not the full orchestration acceptance"
    )
    args = parser.parse_args()
    run(args.count, not args.direct)
