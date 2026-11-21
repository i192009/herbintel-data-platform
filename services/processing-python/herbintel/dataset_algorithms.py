from .infra import digest


def select_training_records(rows: list[dict], language: str | None = None, category: str | None = None) -> list[dict]:
    """Select current source versions before applying approval and quality policy."""
    latest: dict[str, dict] = {}
    for row in sorted(rows, key=lambda r: (r.get("source_version") or 0, r["ingestion_timestamp"], r["document_id"])):
        latest[row["source_id"]] = row
    unique: dict[str, dict] = {}
    for row in sorted(latest.values(), key=lambda r: r["document_id"]):
        if (
            row["approved"]
            and row["quality_score"] >= 0.75
            and (not language or row["document_language"] == language)
            and (not category or row["document_category"] == category)
        ):
            unique.setdefault(row["content_checksum"], row)
    return sorted(unique.values(), key=lambda r: r["document_id"])


def split_groups(rows: list[dict], seed: int) -> dict[str, str]:
    """Union source and identical-content groups before deterministic splitting."""
    parent: dict[str, str] = {}

    def find(key: str) -> str:
        parent.setdefault(key, key)
        while parent[key] != key:
            parent[key] = parent[parent[key]]
            key = parent[key]
        return key

    for row in sorted(rows, key=lambda r: r["document_id"]):
        a, b = find("source:" + row["source_id"]), find("content:" + row["content_checksum"])
        parent[max(a, b)] = min(a, b)
    result = {}
    for row in rows:
        n = int(digest(f"{seed}:{find('source:' + row['source_id'])}".encode())[:8], 16) % 100
        result[row["document_id"]] = "train" if n < 80 else "validation" if n < 90 else "test"
    return result


def compare_versions(left: dict, right: dict) -> dict:
    a = {(r["document_id"], r["pipeline_version"]) for r in left["lineage"]}
    b = {(r["document_id"], r["pipeline_version"]) for r in right["lineage"]}
    return {"added": sorted(b - a), "removed": sorted(a - b), "unchanged": len(a & b)}
