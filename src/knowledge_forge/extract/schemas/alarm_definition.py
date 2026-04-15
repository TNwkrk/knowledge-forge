"""Alarm definition extraction schema."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, field_validator

from knowledge_forge.extract.schemas.base import ProvenancedRecord

Severity = Literal["info", "warning", "critical"]


class AlarmDefinition(ProvenancedRecord):
    """A normalized alarm or error code definition."""

    code: str = Field(min_length=1)
    description: str = Field(min_length=1)
    cause: str = Field(min_length=1)
    remedy: str = Field(min_length=1)
    severity: Severity

    @field_validator("code", "description", "cause", "remedy")
    @classmethod
    def validate_text(cls, value: str) -> str:
        """Trim required alarm fields."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("value must not be blank")
        return cleaned
