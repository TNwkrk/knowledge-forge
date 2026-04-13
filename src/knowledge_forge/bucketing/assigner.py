"""Bucket assignment helpers for manifest-backed documents."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from knowledge_forge.bucketing.taxonomy import BUCKET_DIMENSION_KEYS
from knowledge_forge.intake.importer import get_data_dir, iter_manifests, load_manifest
from knowledge_forge.intake.manifest import (
    BucketAssignment,
    DocumentStatus,
    ManifestEntry,
    slugify,
)


@dataclass(frozen=True)
class BucketingResult:
    """Outcome of applying bucket assignments to a manifest."""

    manifest: ManifestEntry
    manifest_path: Path
    updated: bool


def assign_buckets(manifest: ManifestEntry) -> list[BucketAssignment]:
    """Derive deterministic bucket assignments from manifest fields."""
    existing_by_key = {
        (assignment.dimension, assignment.bucket_id, assignment.value): assignment
        for assignment in manifest.bucket_assignments
    }
    assignments: list[BucketAssignment] = []

    for dimension in BUCKET_DIMENSION_KEYS:
        bucket_id = derive_bucket_id(
            manufacturer=manifest.document.manufacturer,
            family=manifest.document.family,
            dimension=dimension,
        )
        for value in _values_for_dimension(manifest, dimension):
            key = (dimension, bucket_id, value)
            existing = existing_by_key.get(key)
            if existing is not None:
                assignments.append(existing)
                continue

            assignments.append(
                BucketAssignment(
                    doc_id=manifest.doc_id,
                    bucket_id=bucket_id,
                    dimension=dimension,
                    value=value,
                )
            )

    return assignments


def derive_bucket_id(*, manufacturer: str, family: str, dimension: str) -> str:
    """Build the stable bucket identifier path for a dimension."""
    return "/".join((slugify(manufacturer), slugify(family), slugify(dimension)))


def bucket_manifest(data_dir: Path, doc_id: str) -> BucketingResult:
    """Apply bucket assignments to a single manifest and persist the result."""
    resolved_data_dir = get_data_dir(data_dir)
    manifest_path = resolved_data_dir / "manifests" / f"{doc_id}.yaml"
    manifest = load_manifest(resolved_data_dir, doc_id)
    updated_manifest = _apply_bucket_state(manifest)
    changed = updated_manifest != manifest
    if changed:
        manifest_path.write_text(updated_manifest.to_yaml(), encoding="utf-8")

    return BucketingResult(manifest=updated_manifest, manifest_path=manifest_path, updated=changed)


def bucket_unassigned_manifests(data_dir: Path) -> list[BucketingResult]:
    """Assign buckets to all manifests that do not yet have bucket assignments."""
    resolved_data_dir = get_data_dir(data_dir)
    results: list[BucketingResult] = []

    for path, manifest in iter_manifests(resolved_data_dir):
        if manifest.bucket_assignments:
            continue
        updated_manifest = _apply_bucket_state(manifest)
        path.write_text(updated_manifest.to_yaml(), encoding="utf-8")
        results.append(BucketingResult(manifest=updated_manifest, manifest_path=path, updated=True))

    return results


def _apply_bucket_state(manifest: ManifestEntry) -> ManifestEntry:
    """Return a manifest with canonical bucket assignments and status."""
    assignments = assign_buckets(manifest)
    updated_manifest = manifest
    if assignments:
        updated_manifest = manifest.transition_status(
            DocumentStatus.BUCKETED,
            reason="bucket assignments generated",
        )

    if updated_manifest.bucket_assignments == assignments and manifest.bucket_assignments == assignments:
        return manifest

    return updated_manifest.model_copy(
        update={
            "bucket_assignments": assignments,
        }
    )


def _values_for_dimension(manifest: ManifestEntry, dimension: str) -> list[str]:
    """Map manifest fields into taxonomy values with stable fallback values."""
    document = manifest.document
    if dimension == "manufacturer":
        return [_fallback_value(document.manufacturer, "unknown-manufacturer")]
    if dimension == "product_family":
        return [_fallback_value(document.family, "unknown-family")]
    if dimension == "model_applicability":
        return _normalize_values(document.model_applicability, fallback="unknown-model")
    if dimension == "document_type":
        return [_fallback_value(document.document_type, "unknown-document-type")]
    if dimension == "revision_authority":
        return [_fallback_value(document.revision, "unknown-revision")]
    if dimension == "publication_date":
        return [document.publication_date.isoformat() if document.publication_date else "undated"]

    raise ValueError(f"unsupported bucket dimension '{dimension}'")


def _fallback_value(value: str, fallback: str) -> str:
    cleaned = value.strip()
    return cleaned or fallback


def _normalize_values(values: list[str], *, fallback: str) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []

    for value in values:
        cleaned = value.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)

    return normalized or [fallback]
