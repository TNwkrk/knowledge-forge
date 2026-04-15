"""Part reference extraction schema."""

from __future__ import annotations

from pydantic import Field, field_validator

from knowledge_forge.extract.schemas.applicability import Applicability
from knowledge_forge.extract.schemas.base import ProvenancedRecord


class PartReference(ProvenancedRecord):
    """A normalized part reference or bill-of-materials row."""

    part_number: str = Field(min_length=1)
    description: str = Field(min_length=1)
    quantity: float = Field(gt=0)
    applicability: Applicability | None = None

    @field_validator("part_number", "description")
    @classmethod
    def validate_text(cls, value: str) -> str:
        """Trim part fields."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("value must not be blank")
        return cleaned
