"""Revision note extraction schema."""

from __future__ import annotations

from datetime import date as date_type

from pydantic import Field, field_validator

from knowledge_forge.extract.schemas.base import ProvenancedRecord


class RevisionNote(ProvenancedRecord):
    """A normalized revision or change-log entry."""

    revision_id: str = Field(min_length=1)
    date: date_type | None = None
    changes: list[str] = Field(min_length=1)
    supersedes: str | None = None

    @field_validator("revision_id")
    @classmethod
    def validate_revision_id(cls, value: str) -> str:
        """Trim revision identifiers."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("revision_id must not be blank")
        return cleaned

    @field_validator("changes")
    @classmethod
    def validate_changes(cls, value: list[str]) -> list[str]:
        """Trim revision notes."""
        cleaned = [item.strip() for item in value if item.strip()]
        if not cleaned:
            raise ValueError("changes must include at least one value")
        return cleaned

    @field_validator("supersedes")
    @classmethod
    def validate_supersedes(cls, value: str | None) -> str | None:
        """Trim optional superseded revision identifiers."""
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None
