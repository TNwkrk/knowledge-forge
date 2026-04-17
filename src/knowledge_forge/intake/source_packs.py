"""Source-pack manifests for repeatable multi-document onboarding."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator
from yaml import safe_load

from knowledge_forge.intake.importer import RegistrationRequest, RegistrationResult, register_document

IncludeMode = Literal["core", "conditional"]


class SourcePackDocument(BaseModel):
    """One source document entry in a checked-in onboarding manifest."""

    model_config = ConfigDict(extra="forbid")

    filename: str = Field(min_length=1)
    family: str = Field(min_length=1)
    model_applicability: list[str] = Field(min_length=1)
    document_type: str = Field(min_length=1)
    revision: str = Field(min_length=1)
    publication_date: date | None = None
    language: str = Field(default="en", min_length=2, max_length=2)
    priority: int = Field(default=3, ge=1)
    document_class: str = Field(default="authoritative-technical", min_length=1)
    include: IncludeMode = "core"
    notes: str | None = None

    @field_validator("filename", "family", "document_type", "revision", "document_class")
    @classmethod
    def validate_required_text(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("value must not be blank")
        return cleaned

    @field_validator("language")
    @classmethod
    def validate_language(cls, value: str) -> str:
        return value.strip().lower()

    @field_validator("model_applicability")
    @classmethod
    def validate_model_applicability(cls, value: list[str]) -> list[str]:
        cleaned = [item.strip() for item in value if item.strip()]
        if not cleaned:
            raise ValueError("model_applicability must include at least one model")
        return cleaned


class SourcePack(BaseModel):
    """Checked-in definition for a repeatable first-corpus onboarding pack."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    manufacturer: str = Field(min_length=1)
    bucket: str = Field(min_length=1)
    scope: str = Field(min_length=1)
    goal: str = Field(min_length=1)
    source_dir: Path
    documents: list[SourcePackDocument] = Field(min_length=1)

    @field_validator("name", "manufacturer", "bucket", "scope", "goal")
    @classmethod
    def validate_required_text(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("value must not be blank")
        return cleaned


@dataclass(frozen=True)
class RegisteredSourcePack:
    """Outcome of attempting to register one source pack."""

    pack: SourcePack
    source_dir: Path
    registered: list[RegistrationResult]
    missing_files: list[Path]
    skipped_conditionals: list[str]


def load_source_pack(path: str | Path) -> SourcePack:
    """Load one source-pack YAML manifest from disk."""
    manifest_path = Path(path).expanduser().resolve()
    payload = safe_load(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"source pack must deserialize to a mapping: {manifest_path}")

    if "source_dir" in payload:
        payload["source_dir"] = (manifest_path.parent / str(payload["source_dir"])).resolve()
    return SourcePack.model_validate(payload)


def register_source_pack(
    pack: SourcePack,
    *,
    data_dir: Path,
    include_conditionals: bool = False,
    allow_missing: bool = False,
    source_dir: Path | None = None,
    force: bool = False,
) -> RegisteredSourcePack:
    """Register every selected source in a manifest-backed source pack."""
    resolved_source_dir = (source_dir or pack.source_dir).expanduser().resolve()
    registered: list[RegistrationResult] = []
    missing_files: list[Path] = []
    skipped_conditionals: list[str] = []

    for document in pack.documents:
        if document.include == "conditional" and not include_conditionals:
            skipped_conditionals.append(document.filename)
            continue

        pdf_path = resolved_source_dir / document.filename
        if not pdf_path.exists():
            missing_files.append(pdf_path)

    if missing_files and not allow_missing:
        missing = ", ".join(str(path) for path in missing_files)
        raise FileNotFoundError(f"missing source-pack files: {missing}")

    missing_filenames = {path.name for path in missing_files}
    for document in pack.documents:
        if document.include == "conditional" and not include_conditionals:
            continue

        pdf_path = resolved_source_dir / document.filename
        if pdf_path.name in missing_filenames:
            continue

        registered.append(
            register_document(
                RegistrationRequest(
                    pdf_path=pdf_path,
                    manufacturer=pack.manufacturer,
                    family=document.family,
                    model_applicability=document.model_applicability,
                    document_type=document.document_type,
                    revision=document.revision,
                    publication_date=document.publication_date,
                    language=document.language,
                    priority=document.priority,
                    document_class=document.document_class,
                    curated_bucket=pack.bucket,
                    force=force,
                ),
                data_dir=data_dir,
            )
        )

    return RegisteredSourcePack(
        pack=pack,
        source_dir=resolved_source_dir,
        registered=registered,
        missing_files=missing_files,
        skipped_conditionals=skipped_conditionals,
    )
