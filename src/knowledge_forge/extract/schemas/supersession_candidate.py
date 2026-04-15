"""Supersession candidate extraction schema."""

from __future__ import annotations

from pydantic import Field, field_validator

from knowledge_forge.extract.schemas.base import ProvenancedRecord


class SupersessionCandidate(ProvenancedRecord):
    """A candidate relationship where one record supersedes another."""

    superseding_record_id: str = Field(min_length=1)
    superseded_record_id: str = Field(min_length=1)
    rationale: str = Field(min_length=1)
    precedence_basis: str = Field(min_length=1)

    @field_validator("superseding_record_id", "superseded_record_id", "rationale", "precedence_basis")
    @classmethod
    def validate_text(cls, value: str) -> str:
        """Trim candidate fields."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("value must not be blank")
        return cleaned
