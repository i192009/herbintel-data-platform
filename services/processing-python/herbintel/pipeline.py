import io
import json
import logging
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from prometheus_client import Counter, Gauge, Histogram

from .extraction import language
from .infra import canonical, database, digest, get, jsonb, put, register_manifest
from .models import DocumentRecord

PROCESSED = Counter("herbintel_documents_processed_total", "Completed document versions")
FAILURES = Counter("herbintel_processing_failures_total", "Document processing errors", ["kind"])
EXTRACTION = Histogram("herbintel_extraction_seconds", "Document extraction latency")
OCR_TIME = Histogram("herbintel_ocr_seconds", "Extraction time for documents requiring OCR")
QUALITY = Counter("herbintel_quality_violations_total", "Critical publication checks failed")
BATCH_TIME = Histogram("herbintel_batch_seconds", "Batch processing latency")
log = logging.getLogger("herbintel")


class ParseRejected(ValueError):
    def __init__(self, code):
        self.error_code = code
        super().__init__(code)


class ParserUnavailable(RuntimeError):
    def __init__(self, code):
        self.error_code = code
        super().__init__(code)


def batch_info(batch_id: str, org: str) -> dict:
    with database() as conn:
        row = conn.execute("SELECT * FROM batches WHERE id=%s AND org_id=%s", (batch_id, org)).fetchone()
    if row is None:
        raise LookupError("batch not found")
    return row


def discover(org: str) -> list[str]:
    with database() as conn:
        return [
            str(r["id"])
            for r in conn.execute(
                "SELECT id FROM batches WHERE org_id=%s AND status IN ('pending','processing') ORDER BY created_at LIMIT 20",
                (org,),
            )
        ]


def validate_manifest(batch_id: str, org: str) -> dict:
    batch = batch_info(batch_id, org)
    if batch["kind"] != "files":
        raise ValueError("metadata-only admission batches have no file processing pipeline")
    body = json.loads(get(batch["manifest_key"]))
    with database() as conn:
        rows = conn.execute(
            """SELECT d.id,d.raw_key,d.file_checksum FROM documents d
                            JOIN batch_documents bd ON bd.document_id=d.id WHERE bd.batch_id=%s""",
            (batch_id,),
        ).fetchall()
    expected = {(str(r["id"]), r["raw_key"], r["file_checksum"]) for r in rows}
    actual = {(r["document_id"], r["raw_key"], r["sha256"]) for r in body["documents"]}
    if expected != actual or body["org_id"] != org or body["batch_id"] != batch_id:
        with database() as conn:
            conn.execute(
                "UPDATE batches SET status='failed',updated_at=now() WHERE id=%s AND org_id=%s AND status!='cancelled'",
                (batch_id, org),
            )
        raise ValueError("batch manifest does not match registry")
    return {"batch_id": batch_id, "documents": len(expected)}


def _process_one(document_id: str, batch_id: str, org: str, version: str) -> str:
    claim = str(uuid.uuid4())
    with database() as conn:
        # Lock only one result. SKIP LOCKED allows independent batches to share versions safely.
        row = conn.execute(
            """SELECT d.*,pr.attempts FROM documents d JOIN processing_results pr ON pr.document_id=d.id
           WHERE d.id=%s AND d.org_id=%s AND pr.pipeline_version=%s AND
           (pr.status IN ('pending','failed') OR (pr.status='processing' AND pr.lease_until<now()))
           AND pr.attempts<pr.attempt_limit FOR UPDATE OF pr SKIP LOCKED""",
            (document_id, org, version),
        ).fetchone()
        if row is None:
            return "skipped"
        if conn.execute("SELECT status FROM batches WHERE id=%s", (batch_id,)).fetchone()["status"] == "cancelled":
            return "cancelled"
        conn.execute(
            """UPDATE processing_results SET status='processing', attempts=attempts+1,
                     lease_until=now()+interval '5 minutes',claim_token=%s,updated_at=now()
                     WHERE document_id=%s AND pipeline_version=%s""",
            (claim, document_id, version),
        )
    started = time.perf_counter()
    try:
        raw = get(row["raw_key"])
        if digest(raw) != row["file_checksum"]:
            raise ValueError("source checksum mismatch")
        parsed = subprocess.run(
            [sys.executable, "-m", "herbintel.extract_cli", row["source_type"]],
            input=raw,
            capture_output=True,
            timeout=120,
        )
        if parsed.returncode:
            import re

            stderr_lines = parsed.stderr.decode("utf-8", errors="replace").splitlines()[-1:]
            parser_code = (
                stderr_lines[0]
                if stderr_lines and re.fullmatch(r"[A-Za-z_]{1,100}", stderr_lines[0])
                else "ParserError"
            )
            raise ParseRejected(parser_code) if parsed.returncode == 2 else ParserUnavailable(parser_code)
        text, page_count, method, sections = json.loads(parsed.stdout)
        elapsed = time.perf_counter() - started
        EXTRACTION.observe(elapsed)
        if "ocr" in method:
            OCR_TIME.observe(elapsed)
        content_hash = digest(text.encode())
        text_key = (
            f"processed/ingestion_date={row['created_at'].date()}/org={org}/{document_id}/{version}/{content_hash}.txt"
        )
        put(text_key, text.encode(), "text/plain; charset=utf-8")
        lang = language(text)
        flags = ([] if lang else ["unknown_language"]) + ([] if len(text) >= 80 else ["short_text"])
        record = DocumentRecord(
            document_id=document_id,
            source_version=row["source_version"],
            source_id=row["source_id"],
            source_type=row["source_type"],
            original_filename=row["original_filename"],
            file_checksum=row["file_checksum"],
            content_checksum=content_hash,
            document_language=lang,
            document_category=row["category"],
            page_count=page_count,
            extraction_method=method,
            extracted_text_location=text_key,
            quality_flags=flags,
            quality_score=max(0, 1 - len(flags) * 0.25),
            ingestion_timestamp=row["created_at"],
            extraction_timestamp=datetime.now(timezone.utc),
            pipeline_version=version,
            batch_id=batch_id,
            org_id=org,
            approved=row["approved"],
            sections=sections,
        )
        with database() as conn:
            changed = conn.execute(
                """UPDATE processing_results SET status='completed',record=%s,error_code=NULL,
                    lease_until=NULL,claim_token=NULL,updated_at=now()
                    WHERE document_id=%s AND pipeline_version=%s AND claim_token=%s""",
                (jsonb(record.model_dump(mode="json")), document_id, version, claim),
            ).rowcount
        if changed:
            PROCESSED.inc()
        return "completed" if changed else "superseded"
    except Exception as error:
        permanent = isinstance(error, (ValueError, UnicodeError)) or type(error).__module__.startswith(
            ("pypdf", "zipfile", "docx")
        )
        status = "quarantined" if permanent else "failed"
        code = getattr(error, "error_code", type(error).__name__)
        # Logs and registry hold error class only; source text is never logged.
        log.warning(
            json.dumps(
                {
                    "event": "document.failed",
                    "batch_id": batch_id,
                    "document_id": document_id,
                    "error_code": code,
                    "trace_id": claim,
                }
            )
        )
        if permanent:
            put(
                f"quarantine/org={org}/{document_id}/{version}.json",
                canonical({"document_id": document_id, "error_code": code, "raw_key": row["raw_key"]}),
                "application/json",
            )
        with database() as conn:
            conn.execute(
                """UPDATE processing_results SET status=%s,error_code=%s,lease_until=NULL,
                         claim_token=NULL,updated_at=now() WHERE document_id=%s AND pipeline_version=%s AND claim_token=%s""",
                (status, code, document_id, version, claim),
            )
        FAILURES.labels(status).inc()
        return status


@BATCH_TIME.time()
def process(batch_id: str, org: str, version: str = "v1") -> dict:
    validate_manifest(batch_id, org)
    with database() as conn:
        batch = conn.execute("SELECT status FROM batches WHERE id=%s FOR UPDATE", (batch_id,)).fetchone()
        if batch["status"] == "cancelled":
            raise ValueError("batch cancelled")
        conn.execute("UPDATE batches SET status='processing',updated_at=now() WHERE id=%s", (batch_id,))
        ids = [
            str(r["document_id"])
            for r in conn.execute("SELECT document_id FROM batch_documents WHERE batch_id=%s", (batch_id,))
        ]
        conn.execute(
            """INSERT INTO processing_results(document_id,pipeline_version,status)
                     SELECT document_id,%s,'pending' FROM batch_documents WHERE batch_id=%s ON CONFLICT DO NOTHING""",
            (version, batch_id),
        )
    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(lambda doc: _process_one(doc, batch_id, org, version), ids))
    result = results(batch_id, org, version)
    if (
        result["counts"].get("failed", 0)
        and not result["counts"].get("processing", 0)
        and not result["counts"].get("pending", 0)
    ) or result["counts"].get("quarantined", 0) == len(ids):
        with database() as conn:
            conn.execute(
                "UPDATE batches SET status='failed',updated_at=now() WHERE id=%s AND status!='cancelled'", (batch_id,)
            )
    if any(result["counts"].get(s, 0) for s in ("pending", "processing", "failed")):
        raise ValueError("batch has unfinished or retryable work")
    return result


def results(batch_id: str, org: str, version: str = "v1") -> dict:
    batch_info(batch_id, org)
    with database() as conn:
        counts = {
            r["status"]: r["n"]
            for r in conn.execute(
                """SELECT pr.status,count(*) n FROM processing_results pr
                JOIN batch_documents bd ON bd.document_id=pr.document_id
                WHERE bd.batch_id=%s AND pr.pipeline_version=%s GROUP BY pr.status""",
                (batch_id, version),
            )
        }
    return {"batch_id": batch_id, "pipeline_version": version, "counts": counts}


def records(batch_id: str, org: str, version: str = "v1") -> list[dict]:
    batch_info(batch_id, org)
    with database() as conn:
        rows = conn.execute(
            """SELECT pr.record FROM processing_results pr JOIN batch_documents bd ON bd.document_id=pr.document_id
                 WHERE bd.batch_id=%s AND pr.pipeline_version=%s AND pr.status='completed' ORDER BY pr.document_id""",
            (batch_id, version),
        ).fetchall()
    return [DocumentRecord.model_validate(r["record"]).model_dump(mode="json") for r in rows]


def quality(batch_id: str, org: str, version: str = "v1") -> dict:
    counts = results(batch_id, org, version)["counts"]
    rows = records(batch_id, org, version)
    with database() as conn:
        accepted = conn.execute("SELECT count(*) n FROM batch_documents WHERE batch_id=%s", (batch_id,)).fetchone()["n"]
    checks = {
        "accepted": accepted,
        "accounted": sum(counts.values()),
        "counts": counts,
        "unique_versions": len({(r["document_id"], r["pipeline_version"]) for r in rows}) == len(rows),
        "all_terminal": sum(counts.get(s, 0) for s in ("completed", "quarantined")) == accepted,
        "required_metadata": all(r["source_id"] and r["file_checksum"] for r in rows),
        "unknown_language_fraction": sum(r["document_language"] is None for r in rows) / max(1, len(rows)),
        "freshness_seconds": max(
            (datetime.now(timezone.utc) - datetime.fromisoformat(r["extraction_timestamp"])).total_seconds()
            for r in rows
        )
        if rows
        else None,
        "duplicate_content": len(rows) - len({r["content_checksum"] for r in rows}),
    }
    critical = (
        accepted > 0
        and checks["accounted"] == accepted
        and checks["unique_versions"]
        and checks["all_terminal"]
        and checks["required_metadata"]
    )
    outcome = (
        "FAIL"
        if not critical
        else (
            "WARNING"
            if counts.get("quarantined", 0) or checks["duplicate_content"] or checks["unknown_language_fraction"]
            else "PASS"
        )
    )
    if not rows and critical:
        outcome = "INCONCLUSIVE"
    with database() as conn:
        conn.execute(
            "INSERT INTO quality_results(batch_id,run_id,outcome,checks) VALUES(%s,%s,%s,%s)",
            (batch_id, str(uuid.uuid4()), outcome, jsonb(checks)),
        )
    if outcome == "FAIL":
        QUALITY.inc()
    return {"batch_id": batch_id, "outcome": outcome, "checks": checks}


def parquet_bytes(rows: list[dict]) -> bytes:
    # Arrow schema is derived from the validated model and explicit list types.
    schema = pa.schema(
        [
            (
                name,
                pa.list_(pa.string())
                if name in ("quality_flags", "sections")
                else pa.bool_()
                if name == "approved"
                else pa.int64()
                if name in ("page_count", "source_version")
                else pa.float64()
                if name == "quality_score"
                else pa.string(),
            )
            for name in DocumentRecord.model_fields
        ]
    )
    sink = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), sink, compression="zstd")
    return sink.getvalue()


def publish(batch_id: str, org: str, version: str = "v1") -> dict:
    report = quality(batch_id, org, version)
    if report["outcome"] in ("FAIL", "INCONCLUSIVE"):
        raise ValueError("quality gate blocked publication")
    rows = records(batch_id, org, version)
    identity = digest(canonical(rows))
    key = f"processed/ingestion_date={batch_info(batch_id, org)['created_at'].date()}/org={org}/batch={batch_id}/{version}/{identity}.parquet"
    data = parquet_bytes(rows)
    sha = put(key, data)
    result = register_manifest(
        {
            "batch_id": batch_id,
            "pipeline_version": version,
            "schema_version": "1",
            "rows": len(rows),
            "objects": [{"key": key, "sha256": sha, "rows": len(rows)}],
        },
        org,
        batch_id,
        "processed",
    )
    return {
        "batch_id": batch_id,
        "manifest_id": result["manifest_id"],
        "manifest_key": result["manifest_key"],
        "rows": len(rows),
    }


def verify(batch_id: str, org: str, version: str = "v1") -> dict:
    batch_info(batch_id, org)
    with database() as conn:
        row = conn.execute(
            """SELECT * FROM manifests WHERE batch_id=%s AND org_id=%s AND kind='processed'
                         AND body->>'pipeline_version'=%s ORDER BY created_at DESC LIMIT 1""",
            (batch_id, org, version),
        ).fetchone()
    if not row:
        raise ValueError("no published manifest")
    if digest(get(row["object_key"])) != row["checksum"]:
        raise ValueError("manifest checksum mismatch")
    total = 0
    for obj in row["body"]["objects"]:
        data = get(obj["key"])
        if digest(data) != obj["sha256"]:
            raise ValueError("Parquet checksum mismatch")
        table = pq.read_table(io.BytesIO(data))
        if table.num_rows != obj["rows"]:
            raise ValueError("Parquet row mismatch")
        for item in table.to_pylist():
            DocumentRecord.model_validate(item)
        total += table.num_rows
    if total != row["body"]["rows"]:
        raise ValueError("manifest row mismatch")
    return {"batch_id": batch_id, "rows": total, "verified": True}


def finalize(batch_id: str, org: str, version: str = "v1") -> dict:
    verify(batch_id, org, version)
    counts = results(batch_id, org, version)["counts"]
    if counts.get("processing", 0) or counts.get("pending", 0) or counts.get("failed", 0):
        raise ValueError("unfinished work")
    status = "partial" if counts.get("quarantined") else "completed"
    with database() as conn:
        conn.execute(
            "UPDATE batches SET status=%s,updated_at=now() WHERE id=%s AND org_id=%s AND status!='cancelled'",
            (status, batch_id, org),
        )
    return {"batch_id": batch_id, "status": status}


def operational_metrics() -> dict:
    with database() as conn:
        counts = {
            r["status"]: r["n"]
            for r in conn.execute("SELECT status,count(*) n FROM processing_results GROUP BY status")
        }
        last = conn.execute("SELECT max(created_at) ts FROM manifests").fetchone()["ts"]
        versions = conn.execute(
            "SELECT count(DISTINCT body->>'dataset_version') n FROM manifests WHERE kind='ml'"
        ).fetchone()["n"]
    return {
        "counts": counts,
        "dataset_versions": versions,
        "freshness_seconds": (datetime.now(timezone.utc) - last).total_seconds() if last else None,
    }


PENDING = Gauge("herbintel_pending_work", "Durable pending document versions")
COMPLETED = Gauge("herbintel_registered_completed_versions", "Completed versions in the durable registry")
REJECTED = Gauge("herbintel_registered_rejected_versions", "Failed and quarantined versions in the durable registry")
DATASET_VERSIONS = Gauge("herbintel_dataset_versions", "Published ML dataset versions in the durable registry")
FRESHNESS = Gauge("herbintel_dataset_freshness_seconds", "Age of last published manifest; -1 before first publication")
