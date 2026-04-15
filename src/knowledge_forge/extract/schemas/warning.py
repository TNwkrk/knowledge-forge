"""Warning extraction schema."""

from __future__ import annotations

from typing import Literal

from pydantic import Field, field_validator

from knowledge_forge.extract.schemas.applicability import Applicability
from knowledge_forge.extract.schemas.base import ProvenancedRecord

Severity = Literal["info", "warning", "caution", "danger", "critical"]


class Warning(ProvenancedRecord):
    """Safety warning or caution extracted from a manual."""

    severity: Severity
    text: str = Field(min_length=1)
    context: str | None = None
    applicability: Applicability | None = None

    @field_validator("text")
    @classmethod
    def validate_text(cls, value: str) -> str:
        """Trim warning text."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("text must not be blank")
        return cleaned

    @field_validator("context")
    @classmethod
    def validate_optional_text(cls, value: str | None) -> str | None:
        """Trim optional warning context."""
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None
