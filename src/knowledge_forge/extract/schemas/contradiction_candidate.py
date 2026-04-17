"""Contradiction candidate extraction schema."""

from __future__ import annotations

from pydantic import Field, field_validator, model_validator

from knowledge_forge.extract.schemas.base import ProvenancedRecord
from knowledge_forge.extract.schemas.supersession_assessment import (
    SupersessionAssessment,
    SupersessionRecordMetadata,
)


class ContradictionCandidate(ProvenancedRecord):
    """A candidate contradiction between two extracted records."""

    record_ids: list[str] = Field(min_length=2)
    conflicting_claim: str = Field(min_length=1)
    rationale: str = Field(min_length=1)
    review_status: str = Field(default="pending", min_length=1)
    compared_records: list[SupersessionRecordMetadata] = Field(min_length=2, max_length=2)
    supersession: SupersessionAssessment | None = None

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

    @model_validator(mode="after")
    def validate_compared_records(self) -> "ContradictionCandidate":
        """Keep embedded comparison metadata aligned with record_ids."""
        compared_ids = {record.record_id for record in self.compared_records}
        if compared_ids != set(self.record_ids):
            raise ValueError("compared_records must match the contradiction record_ids")
        return self
