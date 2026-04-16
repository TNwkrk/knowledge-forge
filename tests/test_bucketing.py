"""Tests for bucket taxonomy assignment and persistence."""

from __future__ import annotations

from datetime import date
from pathlib import Path

from knowledge_forge.bucketing.assigner import (
    assign_buckets,
    bucket_manifest,
    bucket_unassigned_manifests,
    derive_bucket_id,
)
from knowledge_forge.intake.manifest import Document, DocumentStatus, DocumentVersion, ManifestEntry


def _build_manifest(
    *,
    models: list[str] | None = None,
    publication_date: date | None = date(2024, 1, 15),
) -> ManifestEntry:
    document = Document(
        source_path=Path("/tmp/manual.pdf"),
        checksum="a" * 64,
        manufacturer="Honeywell",
        family="DC1000",
        model_applicability=models or ["DC1000"],
        document_class="authoritative-technical",
        document_type="Service Manual",
        revision="Rev 3",
        publication_date=publication_date,
        language="en",
        priority=2,
        status=DocumentStatus.REGISTERED,
    )
    return ManifestEntry(
        document=document,
        document_version=DocumentVersion(
            doc_id=document.doc_id,
            revision=document.revision,
            checksum=document.checksum,
            source_path=document.source_path,
            publication_date=document.publication_date,
        ),
    )


def test_assign_buckets_is_deterministic_for_same_manifest() -> None:
    manifest = _build_manifest(models=["DC1000", "DC1100"])

    first = assign_buckets(manifest)
    second = assign_buckets(manifest)

    assert [(item.dimension, item.bucket_id, item.value) for item in first] == [
        (item.dimension, item.bucket_id, item.value) for item in second
    ]
    assert [item.value for item in first if item.dimension == "model_applicability"] == ["DC1000", "DC1100"]


def test_assign_buckets_supports_multiple_models() -> None:
    manifest = _build_manifest(models=["DC1000", "DC1100", "DC1100"])

    assignments = assign_buckets(manifest)
    model_assignments = [item for item in assignments if item.dimension == "model_applicability"]

    assert len(model_assignments) == 2
    assert [item.value for item in model_assignments] == ["DC1000", "DC1100"]
    assert all(item.bucket_id == "honeywell/dc1000/model-applicability" for item in model_assignments)


def test_assign_buckets_uses_fallback_for_missing_publication_date() -> None:
    manifest = _build_manifest(publication_date=None)

    assignments = assign_buckets(manifest)
    publication_bucket = next(item for item in assignments if item.dimension == "publication_date")

    assert publication_bucket.value == "undated"


def test_assign_buckets_include_document_class_dimension() -> None:
    manifest = _build_manifest()

    assignments = assign_buckets(manifest)
    document_class_bucket = next(item for item in assignments if item.dimension == "document_class")

    assert document_class_bucket.bucket_id == "honeywell/dc1000/document-class"
    assert document_class_bucket.value == "authoritative-technical"


def test_bucket_manifest_persists_assignments_and_status(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    manifests_dir = data_dir / "manifests"
    manifests_dir.mkdir(parents=True)

    manifest = _build_manifest(models=["DC1000", "DC1100"])
    manifest_path = manifests_dir / f"{manifest.doc_id}.yaml"
    manifest_path.write_text(manifest.to_yaml(), encoding="utf-8")

    result = bucket_manifest(data_dir, manifest.doc_id)

    assert result.updated is True
    assert result.manifest.document.status == DocumentStatus.BUCKETED
    assert len(result.manifest.bucket_assignments) == 8

    restored = ManifestEntry.from_yaml(manifest_path.read_text(encoding="utf-8"))
    assert restored.document.status == DocumentStatus.BUCKETED
    assert len(restored.bucket_assignments) == 8


def test_bucket_unassigned_manifests_only_updates_missing_assignments(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    manifests_dir = data_dir / "manifests"
    manifests_dir.mkdir(parents=True)

    unbucketed = _build_manifest()
    bucketed_source = _build_manifest(models=["DC1100"])
    bucketed = bucketed_source.model_copy(
        update={
            "document": bucketed_source.document.model_copy(update={"status": DocumentStatus.BUCKETED}),
            "bucket_assignments": assign_buckets(bucketed_source),
        }
    )

    (manifests_dir / f"{unbucketed.doc_id}.yaml").write_text(unbucketed.to_yaml(), encoding="utf-8")
    (manifests_dir / "bucketed-doc.yaml").write_text(bucketed.to_yaml(), encoding="utf-8")

    results = bucket_unassigned_manifests(data_dir)

    assert [result.manifest.doc_id for result in results] == [unbucketed.doc_id]


def test_derive_bucket_id_slugifies_components() -> None:
    assert (
        derive_bucket_id(
            manufacturer="Honeywell / Home",
            family="DC 1000+",
            dimension="product_family",
        )
        == "honeywell-home/dc-1000/product-family"
    )
