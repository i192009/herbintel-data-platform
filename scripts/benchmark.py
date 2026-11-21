"""90,000 metadata records admitted through Go; zero PDFs processed in this benchmark."""

import json
import os
import threading
import time
import tracemalloc
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests
from herbintel.infra import database
from herbintel.synthetic import metadata_records


def run(count=90000, chunk=1000, concurrency=4):
    rows = metadata_records(count)
    run_id = uuid.uuid4().hex
    # Each run has an independent synthetic source namespace, so inserted counts are reproducible.
    for row in rows:
        row["source_id"] = run_id + ":" + row["source_id"]
    chunks = [rows[i : i + chunk] for i in range(0, count, chunk)]
    latencies = []
    server_samples = []
    stop_sampling = threading.Event()

    def sample_server():
        response = requests.get(os.getenv("INGEST_URL", "http://ingestion:8080") + "/metrics", timeout=5)
        response.raise_for_status()
        values = {}
        for line in response.text.splitlines():
            for metric in ("process_cpu_seconds_total", "process_resident_memory_bytes"):
                if line.startswith(metric + " "):
                    values[metric] = float(line.split()[1])
        server_samples.append(values)

    def monitor():
        while not stop_sampling.wait(0.2):
            sample_server()

    def admit(item):
        index, records = item
        started = time.perf_counter()
        response = requests.post(
            os.getenv("INGEST_URL", "http://ingestion:8080") + "/api/v1/records",
            headers={
                "Authorization": "Bearer " + os.getenv("API_KEY", "local-demo-write-key"),
                "Idempotency-Key": f"benchmark-{run_id}-{index}",
            },
            json={"records": records},
            timeout=120,
        )
        assert response.status_code == 202, response.text
        body = response.json()
        assert body["record_count"] == len(records) and body["inserted_records"] == len(records)
        latencies.append(time.perf_counter() - started)
        return body["batch_id"]

    cpu_start = time.process_time()
    tracemalloc.start()
    sample_server()
    sampler = threading.Thread(target=monitor, daemon=True)
    sampler.start()
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        batches = list(pool.map(admit, enumerate(chunks)))
    elapsed = time.perf_counter() - started
    stop_sampling.set()
    sampler.join(timeout=10)
    sample_server()
    cpu_seconds = time.process_time() - cpu_start
    _, peak_allocated = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    with database() as conn:
        actual = conn.execute(
            "SELECT count(*) n FROM metadata_records WHERE batch_id=ANY(%s::uuid[])", (batches,)
        ).fetchone()["n"]
    assert actual == count
    report = {
        "benchmark_type": "metadata_only_admission",
        "records": actual,
        "batches": len(batches),
        "fully_processed_pdfs": 0,
        "concurrency": concurrency,
        "elapsed_seconds": round(elapsed, 4),
        "records_per_second": round(count / elapsed, 2),
        "batch_latency_p50_seconds": sorted(latencies)[len(latencies) // 2],
        "batch_latency_p95_seconds": sorted(latencies)[int((len(latencies) - 1) * 0.95)],
        "client_peak_traced_allocation_bytes": peak_allocated,
        "client_cpu_seconds": cpu_seconds,
        "ingestion_server_cpu_seconds": round(
            server_samples[-1]["process_cpu_seconds_total"] - server_samples[0]["process_cpu_seconds_total"], 4
        )
        if "process_cpu_seconds_total" in server_samples[0] and "process_cpu_seconds_total" in server_samples[-1]
        else None,
        "ingestion_server_peak_sampled_rss_bytes": max(
            (s.get("process_resident_memory_bytes", 0) for s in server_samples), default=0
        )
        or None,
        "server_resource_samples": len(server_samples),
        "measurement_scope": "HTTP admission, MinIO raw write and PostgreSQL commit; client CPU/traced allocations; Go process CPU delta and RSS sampled through actual Prometheus process metrics",
        "seed": 42,
        "run_id": run_id,
    }
    Path("artifacts/benchmark-metadata.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=90000)
    parser.add_argument("--concurrency", type=int, default=4)
    args = parser.parse_args()
    run(args.count, concurrency=args.concurrency)
