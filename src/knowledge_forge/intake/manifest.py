"""Manifest data models and serialization helpers for manual intake."""

from __future__ import annotations

from datetime import date, datetime, timezone
from enum import Enum
from hashlib import sha256
from pathlib import Path
from re import sub
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator
from yaml import safe_dump, safe_load


def slugify(value: str) -> str:
    """Convert a free-form string into a stable filesystem-safe slug."""
    normalized = sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-")
    return normalized or "unknown"


def derive_doc_id(
    manufacturer: str,
    family: str,
    document_type: str,
    revision: str,
) -> str:
    """Derive the canonical document identifier from manifest fields."""
    return "-".join(
        [
            slugify(manufacturer),
            slugify(family),
            slugify(document_type),
            slugify(revision),
        ]
    )


def compute_sha256(path: str | Path) -> str:
    """Compute the SHA-256 digest for a source file."""
    file_path = Path(path)
    digest = sha256()
    with file_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class DocumentStatus(str, Enum):
    """Pipeline lifecycle for a manifest-backed document."""

    REGISTERED = "registered"
    BUCKETED = "bucketed"
    NORMALIZED = "normalized"
    PARSED = "parsed"
    EXTRACTED = "extracted"
    COMPILED = "compiled"
    PUBLISHED = "published"


class ManifestModel(BaseModel):
    """Base model with shared serialization helpers."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the model into JSON-safe Python values."""
        return self.model_dump(mode="json", exclude_computed_fields=True)

    def to_json(self) -> str:
        """Serialize the model into canonical JSON."""
        return self.model_dump_json(indent=2, exclude_computed_fields=True)

    @classmethod
    def from_json(cls, payload: str) -> "ManifestModel":
        """Deserialize the model from JSON text."""
        return cls.model_validate_json(payload)

    def to_yaml(self) -> str:
        """Serialize the model into YAML."""
        return safe_dump(self.to_dict(), sort_keys=False)

    @classmethod
    def from_yaml(cls, payload: str) -> "ManifestModel":
        """Deserialize the model from YAML text."""
        return cls.model_validate(safe_load(payload))


class Document(ManifestModel):
    """Registered source document metadata."""

    source_path: Path
    checksum: str = Field(min_length=64, max_length=64)
    manufacturer: str = Field(min_length=1)
    family: str = Field(min_length=1)
    model_applicability: list[str] = Field(min_length=1)
    document_type: str = Field(min_length=1)
    revision: str = Field(min_length=1)
    publication_date: date | None = None
    language: str = Field(min_length=1)
    priority: int = Field(default=3, ge=1)
    status: DocumentStatus = DocumentStatus.REGISTERED

    @field_validator("checksum")
    @classmethod
    def validate_checksum(cls, value: str) -> str:
        """Accept lowercase or uppercase digests and normalize them."""
        normalized = value.strip().lower()
        if len(normalized) != 64 or any(char not in "0123456789abcdef" for char in normalized):
            raise ValueError("checksum must be a 64-character hexadecimal SHA-256 digest")
        return normalized

    @field_validator("manufacturer", "family", "document_type", "revision")
    @classmethod
    def validate_named_field(cls, value: str) -> str:
        """Trim user-facing name fields."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("value must not be blank")
        return cleaned

    @field_validator("model_applicability")
    @classmethod
    def validate_model_applicability(cls, value: list[str]) -> list[str]:
        """Normalize applicability values while preserving order."""
        cleaned = [item.strip() for item in value if item.strip()]
        if not cleaned:
            raise ValueError("model_applicability must include at least one model")
        return cleaned

    @field_validator("language")
    @classmethod
    def validate_language(cls, value: str) -> str:
        """Normalize ISO 639-1 language tags."""
        normalized = value.strip().lower()
        if len(normalized) != 2 or not normalized.isalpha():
            raise ValueError("language must be a two-letter ISO 639-1 code")
        return normalized

    @computed_field  # type: ignore[misc]
    @property
    def doc_id(self) -> str:
        """Stable canonical document identifier."""
        return derive_doc_id(
            manufacturer=self.manufacturer,
            family=self.family,
            document_type=self.document_type,
            revision=self.revision,
        )


class DocumentVersion(ManifestModel):
    """Version metadata for a specific document revision."""

    doc_id: str = Field(min_length=1)
    revision: str = Field(min_length=1)
    checksum: str = Field(min_length=64, max_length=64)
    source_path: Path
    publication_date: date | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("doc_id", "revision")
    @classmethod
    def validate_identifier_fields(cls, value: str) -> str:
        """Trim string identifiers."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("value must not be blank")
        return cleaned

    @field_validator("checksum")
    @classmethod
    def validate_checksum(cls, value: str) -> str:
        """Reuse the digest validation rules for versions."""
        return Document.validate_checksum(value)

    @computed_field  # type: ignore[misc]
    @property
    def version_id(self) -> str:
        """Stable identifier for the concrete document revision."""
        return f"{self.doc_id}--{slugify(self.revision)}"


class BucketAssignment(ManifestModel):
    """Association between a document and a derived processing bucket."""

    doc_id: str = Field(min_length=1)
    bucket_id: str = Field(min_length=1)
    dimension: str = Field(min_length=1)
    value: str = Field(min_length=1)
    assigned_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("doc_id", "bucket_id", "dimension", "value")
    @classmethod
    def validate_required_strings(cls, value: str) -> str:
        """Trim identifier fields while forbidding blanks."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("value must not be blank")
        return cleaned


class ManifestEntry(ManifestModel):
    """Full manifest record persisted for an intake document."""

    document: Document
    document_version: DocumentVersion
    bucket_assignments: list[BucketAssignment] = Field(default_factory=list)

    @computed_field  # type: ignore[misc]
    @property
    def doc_id(self) -> str:
        """Expose the canonical document identifier at the manifest level."""
        return self.document.doc_id
