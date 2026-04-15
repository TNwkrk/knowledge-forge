"""Specification value extraction schema."""

from __future__ import annotations

from pydantic import Field, field_validator

from knowledge_forge.extract.schemas.applicability import Applicability
from knowledge_forge.extract.schemas.base import ProvenancedRecord


class SpecValue(ProvenancedRecord):
    """A normalized technical specification value."""

    parameter: str = Field(min_length=1)
    value: str = Field(min_length=1)
    unit: str | None = None
    conditions: str | None = None
    applicability: Applicability | None = None

    @field_validator("parameter", "value")
    @classmethod
    def validate_text(cls, value: str) -> str:
        """Trim required spec fields."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("value must not be blank")
        return cleaned

    @field_validator("unit", "conditions")
    @classmethod
    def validate_optional_text(cls, value: str | None) -> str | None:
        """Trim optional spec fields."""
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None
