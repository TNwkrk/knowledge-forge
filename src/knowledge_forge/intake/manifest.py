"""Manifest data models and serialization helpers for manual intake."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timezone
from enum import Enum
from hashlib import sha256
from pathlib import Path
from re import sub
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator, model_validator
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


DOCUMENT_CLASS_VALUES: tuple[str, ...] = (
    "authoritative-technical",
    "operational",
    "contextual",
)

CANONICAL_DOCUMENT_TYPE_VALUES: tuple[str, ...] = (
    "installation-manual",
    "operation-manual",
    "service-manual",
    "startup-procedure",
    "shutdown-procedure",
    "winterization-procedure",
    "pm-procedure",
    "sop",
    "checklist",
    "datasheet",
    "specification-sheet",
    "selection-guide",
    "certification",
    "service-bulletin",
    "revision-history",
    "firmware-release-note",
    "supersession-notice",
    "parts-list",
    "bom",
    "spare-parts-catalog",
    "engineering-drawing",
    "wiring-diagram",
    "pid",
    "safety-procedure",
    "loto-sheet",
    "permit-reference",
    "field-form",
    "inspection-template",
    "commissioning-sheet",
    "training-material",
    "technician-reference",
    "best-practice",
    "bulletin",
    "addendum",
    "quick-start",
    "supplemental-guide",
)

LEGACY_DOCUMENT_TYPE_VALUES: tuple[str, ...] = (
    "Installation Manual",
    "Operation Manual",
    "Service Manual",
    "Startup Procedure",
    "Shutdown Procedure",
    "Winterization Procedure",
    "PM Procedure",
    "SOP",
    "Checklist",
    "Datasheet",
    "Specification Sheet",
    "Selection Guide",
    "Certification",
    "Service Bulletin",
    "Revision History",
    "Firmware Release Note",
    "Supersession Notice",
    "Parts List",
    "BOM",
    "Spare Parts Catalog",
    "Engineering Drawing",
    "Wiring Diagram",
    "P&ID",
    "Safety Procedure",
    "LOTO Sheet",
    "Permit Reference",
    "Field Form",
    "Inspection Template",
    "Commissioning Sheet",
    "Training Material",
    "Technician Reference",
    "Best Practice",
    "Bulletin",
    "Addendum",
    "Quick Start",
    "Quick Start Guide",
    "Supplemental Guide",
)


def _normalized_allowed_values(values: Iterable[str]) -> set[str]:
    return {value.strip().casefold() for value in values}


ALLOWED_DOCUMENT_TYPE_VALUES: frozenset[str] = frozenset(
    _normalized_allowed_values(CANONICAL_DOCUMENT_TYPE_VALUES) | _normalized_allowed_values(LEGACY_DOCUMENT_TYPE_VALUES)
)


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
    document_class: str = Field(default="authoritative-technical", min_length=1)
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

    @field_validator("manufacturer", "family", "revision")
    @classmethod
    def validate_named_field(cls, value: str) -> str:
        """Trim user-facing name fields."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("value must not be blank")
        return cleaned

    @field_validator("document_class")
    @classmethod
    def validate_document_class(cls, value: str) -> str:
        """Validate the canonical document class vocabulary."""
        cleaned = value.strip().casefold()
        if cleaned not in DOCUMENT_CLASS_VALUES:
            allowed = ", ".join(DOCUMENT_CLASS_VALUES)
            raise ValueError(f"document_class must be one of: {allowed}")
        return cleaned

    @field_validator("document_type")
    @classmethod
    def validate_document_type(cls, value: str) -> str:
        """Allow the expanded canonical vocabulary plus legacy persisted values."""
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("value must not be blank")
        if cleaned.casefold() not in ALLOWED_DOCUMENT_TYPE_VALUES:
            allowed = ", ".join(CANONICAL_DOCUMENT_TYPE_VALUES)
            raise ValueError(f"document_type must be one of the canonical values or legacy aliases: {allowed}")
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
    version_number: int = Field(default=1, ge=1)
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
        return f"{self.doc_id}--v{self.version_number:03d}"


class StatusTransition(ManifestModel):
    """Audit entry for a document lifecycle change."""

    from_status: DocumentStatus | None = None
    to_status: DocumentStatus
    changed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    reason: str | None = None


STATUS_ORDER: tuple[DocumentStatus, ...] = (
    DocumentStatus.REGISTERED,
    DocumentStatus.BUCKETED,
    DocumentStatus.NORMALIZED,
    DocumentStatus.PARSED,
    DocumentStatus.EXTRACTED,
    DocumentStatus.COMPILED,
    DocumentStatus.PUBLISHED,
)


def can_transition_status(current: DocumentStatus, target: DocumentStatus) -> bool:
    """Return True when the requested lifecycle transition moves forward."""
    return STATUS_ORDER.index(target) >= STATUS_ORDER.index(current)


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
    document_versions: list[DocumentVersion] = Field(default_factory=list)
    status_history: list[StatusTransition] = Field(default_factory=list)
    bucket_assignments: list[BucketAssignment] = Field(default_factory=list)

    @model_validator(mode="after")
    def hydrate_derived_lists(self) -> "ManifestEntry":
        """Backfill legacy manifests into the current multi-version shape."""
        if not self.document_versions:
            self.document_versions = [self.document_version]
        elif self.document_version.version_id != self.document_versions[-1].version_id:
            self.document_versions = [*self.document_versions, self.document_version]

        if not self.status_history:
            self.status_history = [
                StatusTransition(
                    from_status=None,
                    to_status=self.document.status,
                    reason="initial registration",
                )
            ]

        return self

    @computed_field  # type: ignore[misc]
    @property
    def doc_id(self) -> str:
        """Expose the canonical document identifier at the manifest level."""
        return self.document.doc_id

    def next_version_number(self) -> int:
        """Return the next available version number for this document."""
        return max((version.version_number for version in self.document_versions), default=0) + 1

    def transition_status(
        self,
        target_status: DocumentStatus,
        *,
        reason: str | None = None,
        force: bool = False,
    ) -> "ManifestEntry":
        """Return a manifest with an updated lifecycle status and audit entry."""
        current_status = self.document.status
        if target_status == current_status and not force:
            return self

        if not force and not can_transition_status(current_status, target_status):
            raise ValueError(f"cannot move status backward from '{current_status.value}' to '{target_status.value}'")

        return self.model_copy(
            update={
                "document": self.document.model_copy(update={"status": target_status}),
                "status_history": [
                    *self.status_history,
                    StatusTransition(
                        from_status=current_status,
                        to_status=target_status,
                        reason=reason,
                    ),
                ],
            }
        )
