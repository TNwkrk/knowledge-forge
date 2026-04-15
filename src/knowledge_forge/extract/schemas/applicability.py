"""Applicability extraction schema."""

from __future__ import annotations

from pydantic import Field, field_validator

from knowledge_forge.extract.schemas.base import ProvenancedRecord


class Applicability(ProvenancedRecord):
    """Product applicability for a manual section or extracted fact."""

    manufacturer: str = Field(min_length=1)
    family: str = Field(min_length=1)
    models: list[str] = Field(min_length=1)
    serial_range: str | None = None
    revision: str | None = None

    @field_validator("manufacturer", "family")
    @classmethod
    def validate_text(cls, value: str) -> str:
        """Trim required text fields."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("value must not be blank")
        return cleaned

    @field_validator("models")
    @classmethod
    def validate_models(cls, value: list[str]) -> list[str]:
        """Normalize model identifiers while preserving order."""
        cleaned = [item.strip() for item in value if item.strip()]
        if not cleaned:
            raise ValueError("models must include at least one model")
        return cleaned

    @field_validator("serial_range", "revision")
    @classmethod
    def validate_optional_text(cls, value: str | None) -> str | None:
        """Trim optional text fields."""
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None
