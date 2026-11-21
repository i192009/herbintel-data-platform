from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

PIPELINE_VERSION = "v1"
SCHEMA_VERSION = "1"


class DocumentRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")
    document_id: str = Field(pattern=r"^[a-f0-9-]{36}$")
    source_id: str = Field(min_length=1, max_length=200)
    source_type: str
    source_version: int | None = Field(default=None, ge=1)
    original_filename: str
    file_checksum: str = Field(pattern=r"^[a-f0-9]{64}$")
    content_checksum: str = Field(pattern=r"^[a-f0-9]{64}$")
    document_language: str | None = Field(pattern=r"^[a-z]{2,3}$")
    document_category: str
    page_count: int = Field(ge=1)
    extraction_method: str
    extracted_text_location: str
    processing_status: Literal["completed"] = "completed"
    quality_flags: list[str]
    quality_score: float = Field(ge=0, le=1)
    ingestion_timestamp: datetime
    extraction_timestamp: datetime
    dataset_version: str | None = None
    pipeline_version: str
    schema_version: str = SCHEMA_VERSION
    batch_id: str = Field(pattern=r"^[a-f0-9-]{36}$")
    org_id: str
    approved: bool
    sections: list[str]


class ProcessRequest(BaseModel):
    batch_id: str = Field(pattern=r"^[a-f0-9-]{36}$")
    pipeline_version: str = Field(default=PIPELINE_VERSION, pattern=r"^v[0-9]+$")


class ExportRequest(ProcessRequest):
    dataset_id: str = Field(default="synthetic-reference", pattern=r"^[a-zA-Z0-9_-]{1,80}$")
    seed: int = 42
    language: str | None = None
    category: str | None = None
