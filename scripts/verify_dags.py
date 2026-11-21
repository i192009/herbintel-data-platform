"""Exercise the remaining local DAGs through the actual Airflow REST API."""

import base64
import json
import subprocess
import time
import urllib.request
import uuid
from pathlib import Path


def settings():
    result = {}
    for line in Path(".env").read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            key, value = line.split("=", 1)
            result[key] = value
    return result


config = settings()
root = "http://localhost:8081/api/v1"
credential = base64.b64encode(
    (config.get("AIRFLOW_USERNAME", "admin") + ":" + config.get("AIRFLOW_PASSWORD", "local-airflow-change-me")).encode()
).decode()


def api(path, method="GET", body=None):
    request = urllib.request.Request(
        root + path,
        method=method,
        headers={"Authorization": "Basic " + credential, "Content-Type": "application/json"},
        data=json.dumps(body).encode() if body is not None else None,
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def sql(query):
    return subprocess.run(
        ["docker", "compose", "exec", "-T", "postgres", "psql", "-U", "postgres", "-d", "herbintel", "-tAc", query],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def trigger(dag, conf):
    api("/dags/" + dag, "PATCH", {"is_paused": False})
    run_id = "verification__" + uuid.uuid4().hex
    api("/dags/" + dag + "/dagRuns", "POST", {"dag_run_id": run_id, "conf": conf})
    deadline = time.monotonic() + 600
    previous = None
    while time.monotonic() < deadline:
        state = api(f"/dags/{dag}/dagRuns/{run_id}")["state"]
        if state != previous:
            print(json.dumps({"dag_id": dag, "run_id": run_id, "state": state}), flush=True)
            previous = state
        if state == "success":
            return {"state": state, "run_id": run_id}
        assert state != "failed", (dag, run_id)
        time.sleep(2)
    raise AssertionError("DAG execution deadline exceeded: " + dag)


batch = json.loads(Path("artifacts/failure-tests.json").read_text())["worker_sigkill_recovery"]["batch_id"]
uuid.UUID(batch)
membership = f"SELECT document_id FROM batch_documents WHERE batch_id='{batch}'"
baseline = sql(
    f"SELECT md5(string_agg(record::text,'' ORDER BY document_id)) FROM processing_results WHERE document_id IN ({membership}) AND pipeline_version='v1'"
)
report = {}
for dag in ("herbintel_data_quality", "herbintel_training_dataset_export", "herbintel_incremental_etl"):
    report[dag] = trigger(dag, {"batch_id": batch, "dataset_id": "dag-verification", "pipeline_version": "v1"})
report["herbintel_historical_backfill"] = trigger(
    "herbintel_historical_backfill", {"batch_ids": [batch], "pipeline_version": "v2"}
)
assert baseline == sql(
    f"SELECT md5(string_agg(record::text,'' ORDER BY document_id)) FROM processing_results WHERE document_id IN ({membership}) AND pipeline_version='v1'"
)
assert (
    sql(
        f"SELECT count(*) FROM processing_results WHERE document_id IN ({membership}) AND pipeline_version='v2' AND status='completed'"
    )
    == "40"
)
report["prior_versions_preserved"] = True
report["warehouse_loading"] = "NOT_RUN_NO_CREDENTIALS"
Path("artifacts/dag-verification.json").write_text(json.dumps(report, indent=2))
print(json.dumps(report, indent=2))
