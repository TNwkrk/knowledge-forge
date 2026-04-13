"""Helpers for registering manuals into the local manifest store."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from shutil import copy2
from typing import Iterable

from knowledge_forge.intake.manifest import (
    Document,
    DocumentStatus,
    DocumentVersion,
    ManifestEntry,
    compute_sha256,
)


@dataclass(frozen=True)
class RegistrationRequest:
    """Inputs required to register a manual."""

    pdf_path: Path
    manufacturer: str
    family: str
    model_applicability: list[str]
    document_type: str
    revision: str
    publication_date: date | None
    language: str
    priority: int


@dataclass(frozen=True)
class RegistrationResult:
    """Outcome of a registration attempt."""

    manifest: ManifestEntry
    manifest_path: Path
    raw_path: Path
    created: bool


def get_data_dir(explicit_path: str | Path | None = None) -> Path:
    """Resolve the Knowledge Forge data directory."""
    if explicit_path is not None:
        return Path(explicit_path).expanduser().resolve()

    configured = Path.cwd() / "data"
    env_value = os.environ.get("KNOWLEDGE_FORGE_DATA_DIR")
    if env_value:
        configured = Path(env_value).expanduser()

    return configured.resolve()


def ensure_data_directories(data_dir: Path) -> None:
    """Create the minimum directory structure needed for intake."""
    for subdir in ("manifests", "raw"):
        (data_dir / subdir).mkdir(parents=True, exist_ok=True)


def iter_manifests(data_dir: Path) -> Iterable[tuple[Path, ManifestEntry]]:
    """Yield all manifest files sorted by path."""
    manifests_dir = data_dir / "manifests"
    if not manifests_dir.exists():
        return

    for path in sorted(manifests_dir.glob("*.yaml")):
        yield path, ManifestEntry.from_yaml(path.read_text(encoding="utf-8"))


def list_manifests(data_dir: Path) -> list[ManifestEntry]:
    """Load every persisted manifest entry."""
    return [manifest for _, manifest in iter_manifests(data_dir)]


def load_manifest(data_dir: Path, doc_id: str) -> ManifestEntry:
    """Load a single manifest by canonical document identifier."""
    manifest_path = data_dir / "manifests" / f"{doc_id}.yaml"
    if not manifest_path.exists():
        raise FileNotFoundError(f"manifest not found for doc_id '{doc_id}'")
    return ManifestEntry.from_yaml(manifest_path.read_text(encoding="utf-8"))


def find_manifest_by_checksum(data_dir: Path, checksum: str) -> tuple[Path, ManifestEntry] | None:
    """Return the existing manifest that already references a source checksum."""
    for path, manifest in iter_manifests(data_dir):
        if manifest.document.checksum == checksum:
            return path, manifest
    return None


def register_document(
    request: RegistrationRequest,
    *,
    data_dir: Path | None = None,
) -> RegistrationResult:
    """Register a source manual and persist its manifest and local raw copy."""
    resolved_data_dir = get_data_dir(data_dir)
    ensure_data_directories(resolved_data_dir)

    source_path = request.pdf_path.expanduser().resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"source file not found: {source_path}")
    if not source_path.is_file():
        raise IsADirectoryError(f"source path is not a file: {source_path}")

    checksum = compute_sha256(source_path)
    existing = find_manifest_by_checksum(resolved_data_dir, checksum)
    if existing is not None:
        manifest_path, manifest = existing
        raw_path = _derive_raw_path(resolved_data_dir, manifest, source_path.suffix or ".pdf")
        return RegistrationResult(manifest=manifest, manifest_path=manifest_path, raw_path=raw_path, created=False)

    document = Document(
        source_path=source_path,
        checksum=checksum,
        manufacturer=request.manufacturer,
        family=request.family,
        model_applicability=request.model_applicability,
        document_type=request.document_type,
        revision=request.revision,
        publication_date=request.publication_date,
        language=request.language,
        priority=request.priority,
        status=DocumentStatus.REGISTERED,
    )
    manifest = ManifestEntry(
        document=document,
        document_version=DocumentVersion(
            doc_id=document.doc_id,
            revision=document.revision,
            checksum=document.checksum,
            source_path=document.source_path,
            publication_date=document.publication_date,
        ),
    )
    manifest_path = resolved_data_dir / "manifests" / f"{document.doc_id}.yaml"
    raw_path = _derive_raw_path(resolved_data_dir, manifest, source_path.suffix or ".pdf")

    if manifest_path.exists():
        raise FileExistsError(f"manifest already exists for doc_id '{document.doc_id}'")
    if raw_path.exists():
        raise FileExistsError(f"raw file destination already exists: {raw_path}")

    manifest_path.write_text(manifest.to_yaml(), encoding="utf-8")
    copy2(source_path, raw_path)

    return RegistrationResult(manifest=manifest, manifest_path=manifest_path, raw_path=raw_path, created=True)


def _derive_raw_path(data_dir: Path, manifest: ManifestEntry, suffix: str) -> Path:
    """Derive the storage path for a copied source document."""
    normalized_suffix = suffix if suffix.startswith(".") else f".{suffix}"
    return data_dir / "raw" / f"{manifest.doc_id}{normalized_suffix.lower()}"
