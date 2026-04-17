"""Tests for intake manifest models and serialization helpers."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from knowledge_forge.intake.manifest import (
    CANONICAL_DOCUMENT_TYPE_VALUES,
    BucketAssignment,
    Document,
    DocumentStatus,
    DocumentVersion,
    ManifestEntry,
    compute_sha256,
    derive_doc_id,
)


def build_document() -> Document:
    return Document(
        source_path=Path("/tmp/Honeywell Manual.pdf"),
        checksum="A" * 64,
        manufacturer="Honeywell",
        family="DC1000",
        model_applicability=["DC1000", "DC1100"],
        document_class="authoritative-technical",
        document_type="Service Manual",
        revision="Rev 3",
        publication_date=date(2024, 1, 15),
        language="EN",
        priority=2,
        status=DocumentStatus.REGISTERED,
    )


def build_manifest_entry() -> ManifestEntry:
    document = build_document()
    return ManifestEntry(
        document=document,
        document_version=DocumentVersion(
            doc_id=document.doc_id,
            revision=document.revision,
            checksum=document.checksum,
            source_path=document.source_path,
            publication_date=document.publication_date,
        ),
        bucket_assignments=[
            BucketAssignment(
                doc_id=document.doc_id,
                bucket_id="honeywell/dc1000/service-manual",
                dimension="document_type",
                value=document.document_type,
            )
        ],
    )


def test_document_validation_normalizes_fields() -> None:
    document = build_document()

    assert document.checksum == "a" * 64
    assert document.language == "en"
    assert document.doc_id == "honeywell-dc1000-service-manual-rev-3"
    assert document.document_class == "authoritative-technical"


def test_document_validation_normalizes_optional_curated_bucket() -> None:
    payload = build_document().model_dump(mode="python")
    payload.pop("doc_id", None)
    payload["curated_bucket"] = "  Pump Station Control Stack  "
    document = Document.model_validate(payload)

    assert document.curated_bucket == "Pump Station Control Stack"


def test_document_validation_rejects_invalid_language() -> None:
    with pytest.raises(ValueError, match="two-letter ISO 639-1 code"):
        Document(
            source_path=Path("/tmp/manual.pdf"),
            checksum="b" * 64,
            manufacturer="Honeywell",
            family="DC1000",
            model_applicability=["DC1000"],
            document_type="Service Manual",
            revision="Rev 3",
            language="eng",
        )


def test_document_validation_rejects_invalid_document_class() -> None:
    with pytest.raises(ValueError, match="document_class must be one of"):
        Document(
            source_path=Path("/tmp/manual.pdf"),
            checksum="b" * 64,
            manufacturer="Honeywell",
            family="DC1000",
            model_applicability=["DC1000"],
            document_class="not-a-class",
            document_type="Service Manual",
            revision="Rev 3",
            language="en",
        )


def test_document_validation_rejects_invalid_document_type() -> None:
    with pytest.raises(ValueError, match="document_type must be one of"):
        Document(
            source_path=Path("/tmp/manual.pdf"),
            checksum="b" * 64,
            manufacturer="Honeywell",
            family="DC1000",
            model_applicability=["DC1000"],
            document_type="Not A Real Type",
            revision="Rev 3",
            language="en",
        )


def test_manifest_round_trip_yaml() -> None:
    manifest = build_manifest_entry()

    restored = ManifestEntry.from_yaml(manifest.to_yaml())

    assert restored == manifest
    assert restored.bucket_assignments[0].bucket_id == "honeywell/dc1000/service-manual"


def test_manifest_round_trip_json() -> None:
    manifest = build_manifest_entry()
    payload = json.loads(manifest.to_json())

    restored = ManifestEntry.from_json(json.dumps(payload))

    assert restored == manifest
    assert "doc_id" not in payload
    assert restored.doc_id == manifest.doc_id


def test_manifest_hydrates_legacy_version_and_status_history() -> None:
    manifest = build_manifest_entry()

    assert [version.version_number for version in manifest.document_versions] == [1]
    assert len(manifest.status_history) == 1
    assert manifest.status_history[0].from_status is None
    assert manifest.status_history[0].to_status == DocumentStatus.REGISTERED
    assert manifest.status_history[0].reason == "initial registration"


def test_manifest_hydrates_legacy_document_class_default() -> None:
    manifest = build_manifest_entry()
    payload = manifest.to_dict()
    payload["document"].pop("document_class")

    restored = ManifestEntry.model_validate(payload)

    assert restored.document.document_class == "authoritative-technical"


def test_document_type_accepts_expanded_canonical_vocabulary() -> None:
    document = build_document().model_copy(update={"document_type": CANONICAL_DOCUMENT_TYPE_VALUES[-1]})

    assert document.document_type == "supplemental-guide"


def test_transition_status_rejects_backward_changes_without_force() -> None:
    manifest = build_manifest_entry().transition_status(DocumentStatus.BUCKETED, reason="bucketed")

    with pytest.raises(ValueError, match="cannot move status backward"):
        manifest.transition_status(DocumentStatus.REGISTERED, reason="reset")


def test_doc_id_derivation_is_stable_and_slug_safe() -> None:
    assert (
        derive_doc_id(
            manufacturer="Honeywell / Home",
            family="DC 1000+",
            document_type="Quick Start Guide",
            revision="Rev. B",
        )
        == "honeywell-home-dc-1000-quick-start-guide-rev-b"
    )


def test_compute_sha256_matches_expected_digest(tmp_path: Path) -> None:
    source = tmp_path / "fixture.txt"
    source.write_text("knowledge-forge\n", encoding="utf-8")

    assert compute_sha256(source) == "be2325faa73eac9661c74553092d95995ea400633f78563157812b0462a110e7"
