import base64
import os
import uuid

import pytest
import requests
from herbintel.extraction import extract
from herbintel.infra import database, digest, get, put
from herbintel.synthetic import document, documents, text

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(not os.getenv("RUN_INTEGRATION"), reason="start Compose services and set RUN_INTEGRATION=1"),
]


def create(items):
    response = requests.post(
        os.getenv("INGEST_URL", "http://ingestion:8080") + "/api/v1/batches",
        headers={
            "Authorization": "Bearer " + os.getenv("API_KEY", "local-demo-write-key"),
            "Idempotency-Key": str(uuid.uuid4()),
        },
        json={"documents": items},
        timeout=60,
    )
    assert response.status_code == 202, response.text
    return response.json()["batch_id"]


def test_minio_round_trip_and_parquet_pipeline():
    from herbintel.pipeline import process, publish, quality, verify  # noqa: F401

    key = "reports/integration/" + uuid.uuid4().hex
    sha = put(key, b"synthetic integration data")
    assert digest(get(key)) == sha
    batch = create([document(str(uuid.uuid4()), "reference.txt", text(2, 33).encode())])
    assert process(batch, "demo")["counts"] == {"completed": 1}
    assert quality(batch, "demo")["outcome"] == "PASS"
    assert publish(batch, "demo")["rows"] == 1
    assert verify(batch, "demo")["verified"]


def test_ocr_from_actual_scanned_pdf():
    row = next(r for r in documents(100) if r["filename"] == "scanned.pdf")
    value, pages, method, _ = extract(base64.b64decode(row["content_base64"]), "pdf")
    assert "SYNTHETIC" in value.upper() and "ocr" in method and pages == 1


def test_expired_worker_claim_recovery():
    from herbintel.pipeline import process, publish, quality, verify  # noqa: F401

    batch = create([document(str(uuid.uuid4()), "recovery.txt", text(7, 91).encode())])
    with database() as conn:
        conn.execute(
            """UPDATE processing_results SET status='processing',attempts=1,
                     lease_until=now()-interval '1 second',claim_token=%s
                     WHERE document_id IN (SELECT document_id FROM batch_documents WHERE batch_id=%s)""",
            (str(uuid.uuid4()), batch),
        )
    assert process(batch, "demo")["counts"] == {"completed": 1}
    assert process(batch, "demo")["counts"] == {"completed": 1}
    with database() as conn:
        assert (
            conn.execute(
                "SELECT attempts FROM processing_results WHERE document_id IN (SELECT document_id FROM batch_documents WHERE batch_id=%s)",
                (batch,),
            ).fetchone()["attempts"]
            == 2
        )


def test_publication_rejects_unprocessed_records():
    from herbintel.pipeline import process, publish, quality, verify  # noqa: F401

    batch = create([document(str(uuid.uuid4()), "pending.txt", text(9, 2).encode())])
    assert quality(batch, "demo")["outcome"] == "FAIL"
    with pytest.raises(ValueError, match="quality gate"):
        publish(batch, "demo")


def test_manifest_tampering_stops_publication():
    from herbintel.pipeline import process, publish, quality, verify  # noqa: F401

    batch = create([document(str(uuid.uuid4()), "checksum.txt", text(8, 13).encode())])
    process(batch, "demo")
    result = publish(batch, "demo")
    with database() as conn:
        body = conn.execute("SELECT body FROM manifests WHERE id=%s", (result["manifest_id"],)).fetchone()["body"]
    put(body["objects"][0]["key"], b"corrupt parquet")
    with pytest.raises(ValueError, match="checksum"):
        verify(batch, "demo")
