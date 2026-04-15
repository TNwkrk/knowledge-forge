"""Troubleshooting entry extraction schema."""

from __future__ import annotations

from pydantic import Field, field_validator

from knowledge_forge.extract.schemas.base import ProvenancedRecord


class TroubleshootingEntry(ProvenancedRecord):
    """A symptom-to-causes-to-remedies troubleshooting mapping."""

    symptom: str = Field(min_length=1)
    possible_causes: list[str] = Field(min_length=1)
    remedies: list[str] = Field(min_length=1)

    @field_validator("symptom")
    @classmethod
    def validate_symptom(cls, value: str) -> str:
        """Trim the troubleshooting symptom."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("symptom must not be blank")
        return cleaned

    @field_validator("possible_causes", "remedies")
    @classmethod
    def validate_items(cls, value: list[str]) -> list[str]:
        """Trim required list items."""
        cleaned = [item.strip() for item in value if item.strip()]
        if not cleaned:
            raise ValueError("list must include at least one value")
        return cleaned
