"""Procedure extraction schema."""

from __future__ import annotations

from pydantic import Field, field_validator

from knowledge_forge.extract.schemas.applicability import Applicability
from knowledge_forge.extract.schemas.base import ProvenancedRecord
from knowledge_forge.extract.schemas.procedure_step import ProcedureStep
from knowledge_forge.extract.schemas.warning import Warning


class Procedure(ProvenancedRecord):
    """Ordered procedure extracted from a manual section."""

    title: str = Field(min_length=1)
    steps: list[ProcedureStep] = Field(min_length=1)
    applicability: Applicability | None = None
    warnings: list[Warning] = Field(default_factory=list)
    tools_required: list[str] = Field(default_factory=list)

    @field_validator("title")
    @classmethod
    def validate_title(cls, value: str) -> str:
        """Trim procedure titles."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("title must not be blank")
        return cleaned

    @field_validator("tools_required")
    @classmethod
    def validate_tools(cls, value: list[str]) -> list[str]:
        """Normalize tool names while preserving order."""
        return [item.strip() for item in value if item.strip()]
