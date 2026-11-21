"""Exercise actual service outages; always restore the affected Compose service."""

import base64
import json
import os
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

settings = {}
if Path(".env").exists():
    for line in Path(".env").read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            key, value = line.split("=", 1)
            settings[key] = value


def setting(key, fallback):
    return os.getenv(key) or settings.get(key) or fallback


def compose(*args, capture=False):
    return subprocess.run(["docker", "compose", *args], check=True, capture_output=capture, text=True)


def json_request(url, body=None, worker=False):
    key = (
        setting("WORKER_API_KEY", "local-internal-service-key")
        if worker
        else setting("API_KEY", "local-demo-write-key")
    )
    headers = {"Authorization": "Bearer " + key, "Content-Type": "application/json"}
    if worker:
        headers["X-Org-ID"] = "demo"
    if body is not None and not worker:
        headers["Idempotency-Key"] = "failure-" + uuid.uuid4().hex
    request = urllib.request.Request(url, headers=headers, data=json.dumps(body).encode() if body is not None else None)
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.load(response)


def sql(query):
    return compose(
        "exec", "-T", "postgres", "psql", "-U", "postgres", "-d", "herbintel", "-tAc", query, capture=True
    ).stdout.strip()


def request(path):
    try:
        with urllib.request.urlopen(
            "http://localhost:" + setting("INGEST_PORT", "18080") + path, timeout=10
        ) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


results = {}
for service in ("postgres", "minio"):
    subprocess.run(["docker", "compose", "stop", service], check=True)
    try:
        code = request("/readyz")
        assert code == 503, (service, code)
        results[service + "_outage"] = "PASS"
    finally:
        subprocess.run(["docker", "compose", "start", service], check=True)
    for attempt in range(60):
        try:
            if request("/readyz") == 200:
                break
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(1)
    else:
        raise AssertionError(service + " failed to recover")
Path("artifacts/failure-tests.json").write_text(json.dumps(results, indent=2))

# Kill an actual worker with committed processing claims, then recover those same logical versions.
ingest = "http://localhost:" + setting("INGEST_PORT", "18080")
worker_url = "http://localhost:8000"
run_id = uuid.uuid4().hex
documents = [
    {
        "source_id": f"crash-{run_id}-{i}",
        "filename": f"reference-{i}.txt",
        "category": "synthetic_educational",
        "approved": True,
        "content_base64": base64.b64encode(
            f"Synthetic reference document number {i}. Worker crash recovery research. No patient data.".encode()
        ).decode(),
    }
    for i in range(40)
]
batch = json_request(ingest + "/api/v1/batches", {"documents": documents})["batch_id"]
uuid.UUID(batch)
membership = f"SELECT document_id FROM batch_documents WHERE batch_id='{batch}'"
payload = {"batch_id": batch, "pipeline_version": "v1"}
with ThreadPoolExecutor(max_workers=1) as pool:
    attempt = pool.submit(json_request, worker_url + "/internal/process", payload, True)
    try:
        deadline = time.monotonic() + 30
        claimed = 0
        while time.monotonic() < deadline:
            claimed = int(
                sql(
                    f"SELECT count(*) FROM processing_results WHERE document_id IN ({membership}) AND status='processing'"
                )
            )
            if claimed:
                break
            time.sleep(0.1)
        assert claimed, "No active processing claim was observed before worker kill"
        compose("kill", "-s", "SIGKILL", "worker")
    finally:
        compose("start", "worker")
    try:
        attempt.result(timeout=30)
    except (OSError, urllib.error.URLError, TimeoutError):
        pass

completed_before = sql(
    f"SELECT document_id::text||'|'||attempts FROM processing_results WHERE document_id IN ({membership}) AND status='completed' ORDER BY document_id"
)
# Advance the lease clock in this test only rather than waiting five real minutes.
sql(
    f"UPDATE processing_results SET lease_until=now()-interval '1 second' WHERE document_id IN ({membership}) AND status='processing'"
)
for attempt in range(60):
    try:
        with urllib.request.urlopen(worker_url + "/healthz", timeout=3) as response:
            if response.status == 200:
                break
    except (OSError, urllib.error.URLError):
        pass
    time.sleep(1)
else:
    raise AssertionError("worker failed to restart")

assert json_request(worker_url + "/internal/process", payload, True)["counts"] == {"completed": 40}
for operation in ("quality", "publish", "verify", "finalize"):
    value = json_request(worker_url + "/internal/" + operation, payload, True)
    assert value.get("outcome") != "FAIL"
assert (
    int(sql(f"SELECT count(*) FROM processing_results WHERE document_id IN ({membership}) AND pipeline_version='v1'"))
    == 40
)
completed_after = set(
    sql(
        f"SELECT document_id::text||'|'||attempts FROM processing_results WHERE document_id IN ({membership}) ORDER BY document_id"
    ).splitlines()
)
assert set(completed_before.splitlines()) <= completed_after
results["worker_sigkill_recovery"] = {
    "status": "PASS",
    "batch_id": batch,
    "documents": 40,
    "active_claims_before_kill": claimed,
    "lease_clock_advanced_in_test": True,
    "preserved_completed_attempts": True,
}
Path("artifacts/failure-tests.json").write_text(json.dumps(results, indent=2))
print(json.dumps(results))
