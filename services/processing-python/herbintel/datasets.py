from datetime import datetime, timezone

from .dataset_algorithms import compare_versions, select_training_records, split_groups  # noqa: F401
from .infra import canonical, database, digest, put, register_manifest
from .models import ExportRequest
from .pipeline import parquet_bytes, quality, records, verify


def export_dataset(request: ExportRequest, org: str) -> dict:
    verify(request.batch_id, org, request.pipeline_version)
    report = quality(request.batch_id, org, request.pipeline_version)
    if report["outcome"] in ("FAIL", "INCONCLUSIVE"):
        raise ValueError("quality gate blocked export")
    candidates = records(request.batch_id, org, request.pipeline_version)
    selected = select_training_records(candidates, request.language, request.category)
    if not selected:
        raise ValueError("no approved documents match filters")
    # Build groups over all candidates BEFORE deduplication to preserve transitive leakage constraints.
    assignments = split_groups(candidates, request.seed)
    lineage = [
        {
            "document_id": r["document_id"],
            "pipeline_version": r["pipeline_version"],
            "source_id": r["source_id"],
            "source_version": r["source_version"],
            "content_checksum": r["content_checksum"],
            "split": assignments[r["document_id"]],
        }
        for r in selected
    ]
    identity = {
        "dataset_id": request.dataset_id,
        "seed": request.seed,
        "org_id": org,
        "pipeline_version": request.pipeline_version,
        "schema_version": "1",
        "filters": {"language": request.language, "category": request.category},
        "lineage": lineage,
    }
    version = digest(canonical(identity))[:20]
    # Repeated identical exports return the original manifest and timestamp.
    with database() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", (org + ":" + version,))
        previous = conn.execute(
            "SELECT object_key,body FROM manifests WHERE org_id=%s AND kind='ml' AND body->>'dataset_version'=%s",
            (org, version),
        ).fetchone()
        if previous:
            return {"manifest_key": previous["object_key"], **previous["body"]}
        source_manifest = conn.execute(
            "SELECT object_key,checksum FROM manifests WHERE batch_id=%s AND org_id=%s AND kind='processed' AND body->>'pipeline_version'=%s ORDER BY created_at DESC LIMIT 1",
            (request.batch_id, org, request.pipeline_version),
        ).fetchone()
        curated_key = f"curated/dataset_version={version}/org={org}/approved.parquet"
        curated_sha = put(curated_key, parquet_bytes([{**r, "dataset_version": version} for r in selected]))
        objects = []
        for split in ("train", "validation", "test"):
            rows = [{**r, "dataset_version": version} for r in selected if assignments[r["document_id"]] == split]
            key = f"exports/ml/org={org}/dataset={request.dataset_id}/dataset_version={version}/{split}.parquet"
            sha = put(key, parquet_bytes(rows))
            objects.append({"split": split, "key": key, "rows": len(rows), "sha256": sha})
        manifest = {
            **identity,
            "dataset_version": version,
            "rows": len(selected),
            "objects": objects,
            "curated_object": {"key": curated_key, "sha256": curated_sha, "rows": len(selected)},
            "export_timestamp": datetime.now(timezone.utc).isoformat(),
            "export_format": "parquet",
            "approval_status": "approved_reference_only",
            "batch_id": request.batch_id,
            "content_checksum": digest(canonical(lineage)),
            "source_lineage_manifest": {"key": source_manifest["object_key"], "sha256": source_manifest["checksum"]},
        }
        return register_manifest(manifest, org, request.batch_id, "ml")
