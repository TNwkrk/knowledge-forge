"""Contradiction candidate extraction schema."""

from __future__ import annotations

from pydantic import Field, field_validator

from knowledge_forge.extract.schemas.base import ProvenancedRecord


class ContradictionCandidate(ProvenancedRecord):
    """A candidate contradiction between two extracted records."""

    record_ids: list[str] = Field(min_length=2)
    conflicting_claim: str = Field(min_length=1)
    rationale: str = Field(min_length=1)
    review_status: str = Field(default="pending", min_length=1)

    @field_validator("record_ids")
    @classmethod
    def validate_record_ids(cls, value: list[str]) -> list[str]:
        """Require at least two non-empty record identifiers."""
        cleaned = [item.strip() for item in value if item.strip()]
        if len(cleaned) < 2:
            raise ValueError("record_ids must include at least two record ids")
        return cleaned

    @field_validator("conflicting_claim", "rationale", "review_status")
    @classmethod
    def validate_text(cls, value: str) -> str:
        """Trim contradiction fields."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("value must not be blank")
        return cleaned
