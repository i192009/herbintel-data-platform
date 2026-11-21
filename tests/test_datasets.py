import io

import pyarrow.parquet as pq
import pytest
from herbintel.dataset_algorithms import compare_versions, split_groups
from pydantic import ValidationError


def test_latest_unapproved_source_does_not_fall_back_to_old_approval():
    from herbintel.dataset_algorithms import select_training_records

    base = {
        "document_language": "en",
        "document_category": "reference",
        "quality_score": 1,
        "ingestion_timestamp": "2026-01-01T00:00:00Z",
        "approved": True,
    }
    rows = [
        {**base, "document_id": "a", "source_id": "old", "source_version": 1, "content_checksum": "one"},
        {
            **base,
            "document_id": "b",
            "source_id": "old",
            "source_version": 2,
            "content_checksum": "two",
            "approved": False,
        },
        {**base, "document_id": "c", "source_id": "current", "source_version": 1, "content_checksum": "three"},
        {**base, "document_id": "d", "source_id": "duplicate", "source_version": 1, "content_checksum": "three"},
    ]
    assert [r["document_id"] for r in select_training_records(rows)] == ["c"]
    assert select_training_records(rows, language="zh") == []


def test_source_and_duplicate_content_cannot_leak():
    rows = [
        {"document_id": "a", "source_id": "source1", "content_checksum": "content1"},
        {"document_id": "b", "source_id": "source2", "content_checksum": "content1"},
        {"document_id": "c", "source_id": "source2", "content_checksum": "content2"},
        {"document_id": "d", "source_id": "source3", "content_checksum": "content3"},
    ]
    for seed in range(100):
        assignments = split_groups(rows, seed)
        assert assignments["a"] == assignments["b"] == assignments["c"]
        assert assignments == split_groups(list(reversed(rows)), seed)


def test_empty_split_preserves_parquet_schema():
    from herbintel.models import DocumentRecord
    from herbintel.pipeline import parquet_bytes

    table = pq.read_table(io.BytesIO(parquet_bytes([])))
    assert table.num_rows == 0
    assert set(DocumentRecord.model_fields) == set(table.column_names)
    assert table.schema.field("quality_flags").type.value_type == table.schema.field("sections").type.value_type


def test_validation_does_not_fabricate_metadata():
    from herbintel.models import DocumentRecord

    with pytest.raises(ValidationError):
        DocumentRecord.model_validate({"document_id": "missing-metadata"})


def test_version_comparison():
    result = compare_versions(
        {"lineage": [{"document_id": "a", "pipeline_version": "v1"}]},
        {"lineage": [{"document_id": "a", "pipeline_version": "v2"}]},
    )
    assert result == {"added": [("a", "v2")], "removed": [("a", "v1")], "unchanged": 0}
