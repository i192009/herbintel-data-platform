import argparse
import json
from pathlib import Path

from herbintel.warehouse import configured, warehouse_step

parser = argparse.ArgumentParser()
parser.add_argument("--batch-id")
args = parser.parse_args()
if not configured():
    raise SystemExit("NOT RUN: live Snowflake credentials unavailable; this is not a successful cloud validation")
batch = args.batch_id or json.loads(Path("artifacts/acceptance.json").read_text())["batch_id"]
results = {}
for step in ("connect", "stage", "load", "history", "verify", "dbt"):
    results[step] = warehouse_step(step, batch, "demo", "v1")
Path("artifacts/cloud-verification.json").write_text(json.dumps(results, indent=2))
print(json.dumps(results, indent=2))
