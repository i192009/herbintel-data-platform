import argparse
import json
import os
from pathlib import Path

import requests

from .datasets import compare_versions
from .synthetic import documents, metadata_records


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["generate-documents", "generate-records", "ingest", "compare"])
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--left")
    parser.add_argument("--right")
    args = parser.parse_args()
    output = Path("datasets/generated")
    output.mkdir(parents=True, exist_ok=True)
    if args.command == "generate-records":
        rows = metadata_records(args.count, args.seed)
        (output / "records.json").write_text(json.dumps({"records": rows}), encoding="utf-8")
        print(json.dumps({"metadata_records": len(rows), "fully_processed_pdfs": 0}))
    elif args.command == "generate-documents":
        rows = documents(args.count, args.seed)
        (output / "documents.json").write_text(json.dumps({"documents": rows}), encoding="utf-8")
        # Also publish representative actual files, not just encoded payloads.
        import base64

        for row in rows[-12:]:
            (output / row["filename"]).write_bytes(base64.b64decode(row["content_base64"]))
        print(json.dumps({"file_records": len(rows), "seed": args.seed}))
    elif args.command == "ingest":
        import uuid

        rows = json.loads((output / "documents.json").read_text())["documents"]
        batches = []
        for offset in range(0, len(rows), 1000):
            response = requests.post(
                os.getenv("INGEST_URL", "http://ingestion:8080") + "/api/v1/batches",
                headers={
                    "Authorization": "Bearer " + os.getenv("API_KEY", "local-demo-write-key"),
                    "Idempotency-Key": "cli-" + uuid.uuid4().hex,
                },
                json={"documents": rows[offset : offset + 1000]},
                timeout=120,
            )
            response.raise_for_status()
            batches.append(response.json()["batch_id"])
        (output / "batches.json").write_text(json.dumps(batches))
        print(json.dumps({"batch_ids": batches}))
    else:
        print(
            json.dumps(
                compare_versions(json.loads(Path(args.left).read_text()), json.loads(Path(args.right).read_text()))
            )
        )


if __name__ == "__main__":
    main()
