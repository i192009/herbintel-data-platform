"""Run after an actual Compose worker restart to verify durable outputs and idempotency."""

import json
import time
from pathlib import Path

import requests
from acceptance import WORKER, post

report = json.loads(Path("artifacts/acceptance.json").read_text())
for attempt in range(30):
    try:
        if requests.get(WORKER + "/healthz", timeout=3).ok:
            break
    except requests.RequestException:
        pass
    time.sleep(1)
request = {"batch_id": report["batch_id"], "pipeline_version": "v1"}
result = post("/internal/process", request)
assert result["counts"]["completed"] == report["completed_files"]
assert post("/internal/verify", request)["verified"]
print(json.dumps({"worker_restart": "PASS", "batch_id": report["batch_id"], "counts": result["counts"]}))
