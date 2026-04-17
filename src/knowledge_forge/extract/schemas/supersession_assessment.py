"""Supersession assessment models used by contradiction analysis."""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import Field, field_validator

from knowledge_forge.extract.schemas.base import ExtractionSchemaModel


class SupersessionRecordMetadata(ExtractionSchemaModel):
    """Comparable document metadata required for supersession decisions."""

    record_id: str = Field(min_length=1)
    source_doc_id: str = Field(min_length=1)
    document_type: str = Field(min_length=1)
    document_class: str = Field(min_length=1)
    revision: str = Field(min_length=1)
    publication_date: date | None = None
    precedence_level: int = Field(ge=1, le=8)
    precedence_label: str = Field(min_length=1)

    @field_validator("record_id", "source_doc_id", "document_type", "document_class", "revision", "precedence_label")
    @classmethod
    def validate_text(cls, value: str) -> str:
        """Trim text fields."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("value must not be blank")
        return cleaned


class SupersessionAssessment(ExtractionSchemaModel):
    """A supersession decision for one contradiction candidate."""

    superseding_record_id: str = Field(min_length=1)
    superseded_record_id: str = Field(min_length=1)
    confidence: Literal["high", "medium", "low"]
    reason: str = Field(min_length=1)
    precedence_rule_applied: str = Field(min_length=1)
    document_types_compared: list[str] = Field(min_length=2, max_length=2)

    @field_validator("superseding_record_id", "superseded_record_id", "reason", "precedence_rule_applied")
    @classmethod
    def validate_text(cls, value: str) -> str:
        """Trim text fields."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("value must not be blank")
        return cleaned

    @field_validator("document_types_compared")
    @classmethod
    def validate_document_types_compared(cls, value: list[str]) -> list[str]:
        """Require exactly two non-empty document types."""
        cleaned = [item.strip() for item in value if item.strip()]
        if len(cleaned) != 2:
            raise ValueError("document_types_compared must include exactly two document types")
        return cleaned
