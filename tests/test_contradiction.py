"""Tests for contradiction and supersession analysis."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from knowledge_forge.cli import cli
from knowledge_forge.extract import analyze_contradictions, find_contradiction_candidates
from knowledge_forge.intake.importer import RegistrationRequest, register_document
from knowledge_forge.intake.manifest import BucketAssignment, DocumentStatus


def _write_pdf(path: Path) -> Path:
    payload = b"%PDF-1.4\n1 0 obj\n<< /Title (" + path.stem.encode("utf-8") + b") >>\nendobj\ntrailer\n<<>>\n%%EOF\n"
    path.write_bytes(payload)
    return path


def _register_fixture_doc(
    pdf_path: Path,
    data_dir: Path,
    *,
    document_type: str,
    document_class: str,
    revision: str,
    bucket_id: str = "honeywell/dc1000/family",
    models: list[str] | None = None,
) -> str:
    request = RegistrationRequest(
        pdf_path=pdf_path,
        manufacturer="Honeywell",
        family="DC1000",
        model_applicability=models or ["DC1000"],
        document_type=document_type,
        revision=revision,
        publication_date=None,
        language="en",
        priority=1,
        document_class=document_class,
    )
    result = register_document(request, data_dir=data_dir)
    manifest_path = data_dir / "manifests" / f"{result.manifest.doc_id}.yaml"
    manifest = result.manifest.transition_status(DocumentStatus.EXTRACTED, reason="test extracted fixture")
    manifest_path.write_text(
        manifest.model_copy(
            update={
                "bucket_assignments": [
                    BucketAssignment(
                        doc_id=result.manifest.doc_id,
                        bucket_id=bucket_id,
                        dimension="family",
                        value="DC1000",
                    )
                ]
            }
        ).to_yaml(),
        encoding="utf-8",
    )
    return result.manifest.doc_id


def _base_payload(doc_id: str, *, heading: str, start_page: int, end_page: int, bucket_id: str) -> dict[str, object]:
    return {
        "source_doc_id": doc_id,
        "source_page_range": {"start_page": start_page, "end_page": end_page},
        "source_heading": heading,
        "parser_version": "docling-1.2.0",
        "extraction_version": "extraction/procedure@v1:gpt-4o-mini",
        "confidence": 0.91,
        "bucket_context": [
            {"bucket_id": bucket_id, "dimension": "family", "value": "DC1000"},
        ],
    }


def _applicability_payload(doc_id: str, bucket_id: str, *, models: list[str] | None = None) -> dict[str, object]:
    return {
        **_base_payload(doc_id, heading="Applicability", start_page=1, end_page=1, bucket_id=bucket_id),
        "manufacturer": "Honeywell",
        "family": "DC1000",
        "models": models or ["DC1000"],
        "serial_range": None,
        "revision": None,
    }


def _write_record(data_dir: Path, doc_id: str, record_type: str, record_id: str, payload: dict[str, object]) -> None:
    path = data_dir / "extracted" / doc_id / record_type / f"{record_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def test_find_contradiction_candidates_stays_bucket_scoped_and_ignores_non_overlapping_records(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    family_bucket = "honeywell/dc1000/family"
    other_bucket = "honeywell/dc1000/revision-authority"
    manual_doc_id = _register_fixture_doc(
        _write_pdf(tmp_path / "manual.pdf"),
        data_dir,
        document_type="Service Manual",
        document_class="authoritative-technical",
        revision="Rev 3",
        bucket_id=family_bucket,
    )
    other_doc_id = _register_fixture_doc(
        _write_pdf(tmp_path / "other.pdf"),
        data_dir,
        document_type="Service Bulletin",
        document_class="authoritative-technical",
        revision="Rev B",
        bucket_id=other_bucket,
    )

    _write_record(
        data_dir,
        manual_doc_id,
        "spec_value",
        "spec-001",
        {
            **_base_payload(
                manual_doc_id,
                heading="Electrical Specifications",
                start_page=8,
                end_page=8,
                bucket_id=family_bucket,
            ),
            "parameter": "Supply voltage",
            "value": "24",
            "unit": "VDC",
            "conditions": "Nominal input",
            "applicability": _applicability_payload(manual_doc_id, family_bucket),
        },
    )
    _write_record(
        data_dir,
        other_doc_id,
        "spec_value",
        "spec-002",
        {
            **_base_payload(
                other_doc_id,
                heading="Electrical Specifications",
                start_page=3,
                end_page=3,
                bucket_id=other_bucket,
            ),
            "parameter": "Supply voltage",
            "value": "48",
            "unit": "VDC",
            "conditions": "Nominal input",
            "applicability": _applicability_payload(other_doc_id, other_bucket),
        },
    )

    candidates = find_contradiction_candidates(family_bucket, data_dir=data_dir)

    assert candidates == []


def test_analyze_contradictions_builds_cross_document_type_supersession_candidates(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    bucket_id = "honeywell/dc1000/family"
    manual_doc_id = _register_fixture_doc(
        _write_pdf(tmp_path / "manual.pdf"),
        data_dir,
        document_type="Service Manual",
        document_class="authoritative-technical",
        revision="Rev 3",
    )
    sop_doc_id = _register_fixture_doc(
        _write_pdf(tmp_path / "sop.pdf"),
        data_dir,
        document_type="SOP",
        document_class="operational",
        revision="Current",
    )
    unrelated_doc_id = _register_fixture_doc(
        _write_pdf(tmp_path / "datasheet.pdf"),
        data_dir,
        document_type="Datasheet",
        document_class="authoritative-technical",
        revision="Rev 1",
        models=["DC1200"],
    )

    _write_record(
        data_dir,
        manual_doc_id,
        "procedure",
        "startup-001",
        {
            **_base_payload(
                manual_doc_id,
                heading="Startup Procedure",
                start_page=18,
                end_page=20,
                bucket_id=bucket_id,
            ),
            "title": "Prime the pump",
            "steps": [
                {
                    **_base_payload(
                        manual_doc_id,
                        heading="Startup Procedure",
                        start_page=18,
                        end_page=18,
                        bucket_id=bucket_id,
                    ),
                    "step_number": 1,
                    "instruction": "Open the discharge valve before energizing the motor.",
                    "note": None,
                    "caution": None,
                    "figure_ref": None,
                }
            ],
            "applicability": _applicability_payload(manual_doc_id, bucket_id),
            "warnings": [],
            "tools_required": [],
        },
    )
    _write_record(
        data_dir,
        sop_doc_id,
        "procedure",
        "startup-002",
        {
            **_base_payload(
                sop_doc_id,
                heading="Startup Procedure",
                start_page=4,
                end_page=5,
                bucket_id=bucket_id,
            ),
            "title": "Prime the pump",
            "steps": [
                {
                    **_base_payload(
                        sop_doc_id,
                        heading="Startup Procedure",
                        start_page=4,
                        end_page=4,
                        bucket_id=bucket_id,
                    ),
                    "step_number": 1,
                    "instruction": "Keep the discharge valve closed before energizing the motor.",
                    "note": None,
                    "caution": None,
                    "figure_ref": None,
                }
            ],
            "applicability": _applicability_payload(sop_doc_id, bucket_id),
            "warnings": [],
            "tools_required": [],
        },
    )
    _write_record(
        data_dir,
        unrelated_doc_id,
        "procedure",
        "startup-003",
        {
            **_base_payload(
                unrelated_doc_id,
                heading="Startup Procedure",
                start_page=2,
                end_page=2,
                bucket_id=bucket_id,
            ),
            "title": "Prime the pump",
            "steps": [
                {
                    **_base_payload(
                        unrelated_doc_id,
                        heading="Startup Procedure",
                        start_page=2,
                        end_page=2,
                        bucket_id=bucket_id,
                    ),
                    "step_number": 1,
                    "instruction": "Vent the casing before energizing the motor.",
                    "note": None,
                    "caution": None,
                    "figure_ref": None,
                }
            ],
            "applicability": _applicability_payload(unrelated_doc_id, bucket_id, models=["DC1200"]),
            "warnings": [],
            "tools_required": [],
        },
    )

    report = analyze_contradictions(bucket_id, data_dir=data_dir)

    assert len(report.contradictions) == 1
    contradiction = report.contradictions[0]
    assert contradiction.record_ids == ["startup-001::step-001", "startup-002::step-001"]
    assert contradiction.review_status == "pending"
    assert "Service Manual" in contradiction.conflicting_claim
    assert "SOP" in contradiction.conflicting_claim

    assert len(report.supersessions) == 1
    supersession = report.supersessions[0]
    assert supersession.superseding_record_id == "startup-001::step-001"
    assert supersession.superseded_record_id == "startup-002::step-001"
    assert "level 2" in supersession.precedence_basis
    assert "level 5" in supersession.precedence_basis


def test_analyze_contradictions_cli_reports_candidates_and_supersession(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    bucket_id = "honeywell/dc1000/family"
    bulletin_doc_id = _register_fixture_doc(
        _write_pdf(tmp_path / "bulletin.pdf"),
        data_dir,
        document_type="Service Bulletin",
        document_class="authoritative-technical",
        revision="Rev C",
    )
    sop_doc_id = _register_fixture_doc(
        _write_pdf(tmp_path / "ops.pdf"),
        data_dir,
        document_type="Best Practice",
        document_class="operational",
        revision="Current",
    )

    _write_record(
        data_dir,
        bulletin_doc_id,
        "warning",
        "warning--warning--001",
        {
            **_base_payload(bulletin_doc_id, heading="Safety Notice", start_page=2, end_page=2, bucket_id=bucket_id),
            "severity": "critical",
            "text": "Do not bypass the purge cycle during restart.",
            "context": "Restart sequence",
            "applicability": _applicability_payload(bulletin_doc_id, bucket_id),
        },
    )
    _write_record(
        data_dir,
        sop_doc_id,
        "warning",
        "warning--warning--002",
        {
            **_base_payload(sop_doc_id, heading="Safety Notice", start_page=2, end_page=2, bucket_id=bucket_id),
            "severity": "warning",
            "text": "Bypass the purge cycle during restart to save time.",
            "context": "Restart sequence",
            "applicability": _applicability_payload(sop_doc_id, bucket_id),
        },
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["analyze", "contradictions", bucket_id],
        env={"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)},
    )

    assert result.exit_code == 0
    assert f"Bucket: {bucket_id}" in result.output
    assert "Contradictions: 1" in result.output
    assert "Supersessions: 1" in result.output
    assert "CONTRADICTIONS" in result.output
    assert "SUPERSESSIONS" in result.output
