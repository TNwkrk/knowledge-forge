"""Procedure step extraction schema."""

from __future__ import annotations

from pydantic import Field, field_validator

from knowledge_forge.extract.schemas.base import ProvenancedRecord


class ProcedureStep(ProvenancedRecord):
    """One ordered step within a procedure."""

    step_number: int = Field(ge=1)
    instruction: str = Field(min_length=1)
    note: str | None = None
    caution: str | None = None
    figure_ref: str | None = None

    @field_validator("instruction")
    @classmethod
    def validate_instruction(cls, value: str) -> str:
        """Trim step instructions."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("instruction must not be blank")
        return cleaned

    @field_validator("note", "caution", "figure_ref")
    @classmethod
    def validate_optional_text(cls, value: str | None) -> str | None:
        """Trim optional text fields."""
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None
