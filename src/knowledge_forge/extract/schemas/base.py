"""Shared extraction schema building blocks."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ExtractionSchemaModel(BaseModel):
    """Base model for extraction records with strict validation."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    @classmethod
    def json_schema(cls) -> dict[str, Any]:
        """Expose the JSON Schema used for prompt-constrained extraction."""
        return cls.model_json_schema()


class SourcePageRange(ExtractionSchemaModel):
    """Inclusive source page range for an extracted record."""

    start_page: int = Field(ge=1)
    end_page: int = Field(ge=1)

    @field_validator("end_page")
    @classmethod
    def validate_page_order(cls, value: int, info: Any) -> int:
        """Ensure the page range is not inverted."""
        start_page = info.data.get("start_page")
        if start_page is not None and value < start_page:
            raise ValueError("end_page must be greater than or equal to start_page")
        return value


class BucketContext(ExtractionSchemaModel):
    """Bucket metadata that scopes contradiction and supersession analysis."""

    bucket_id: str = Field(min_length=1)
    dimension: str = Field(min_length=1)
    value: str = Field(min_length=1)

    @field_validator("bucket_id", "dimension", "value")
    @classmethod
    def validate_text(cls, value: str) -> str:
        """Trim text fields and reject blanks."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("value must not be blank")
        return cleaned


class ProvenancedRecord(ExtractionSchemaModel):
    """Common provenance fields required on every extracted record."""

    source_doc_id: str = Field(min_length=1)
    source_page_range: SourcePageRange
    source_heading: str = Field(min_length=1)
    parser_version: str = Field(min_length=1)
    extraction_version: str = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    bucket_context: list[BucketContext] = Field(min_length=1)

    @field_validator("source_doc_id", "source_heading", "parser_version", "extraction_version")
    @classmethod
    def validate_text(cls, value: str) -> str:
        """Trim string identifiers and headings."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("value must not be blank")
        return cleaned
