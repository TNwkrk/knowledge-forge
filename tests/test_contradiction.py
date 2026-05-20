"""Tests for contradiction and supersession analysis."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from click.testing import CliRunner

from knowledge_forge.cli import cli
from knowledge_forge.compile import render_contradiction_review_report
from knowledge_forge.extract import (
    analyze_contradictions,
    find_contradiction_candidates,
    find_supersession_assessments,
)
from knowledge_forge.intake.importer import RegistrationRequest, register_document
from knowledge_forge.intake.manifest import BucketAssignment, DocumentStatus, slugify


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
    publication_date: str | None = None,
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
        publication_date=(date.fromisoformat(publication_date) if publication_date is not None else None),
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
    assert contradiction.review_status == "unreviewed"
    assert "Service Manual" in contradiction.conflicting_claim
    assert "SOP" in contradiction.conflicting_claim

    assert len(report.supersessions) == 1
    supersession = report.supersessions[0]
    assert supersession.superseding_record_id == "startup-001::step-001"
    assert supersession.superseded_record_id == "startup-002::step-001"
    assert supersession.confidence == "high"
    assert "level 2" in supersession.precedence_rule_applied
    assert "level 5" in supersession.precedence_rule_applied
    assert contradiction.supersession == supersession
    assert contradiction.supersession.document_types_compared == ["Service Manual", "SOP"]


def test_analyze_contradictions_disambiguates_duplicate_step_numbers_within_one_procedure(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    bucket_id = "honeywell/dc1000/family"
    manual_doc_id = _register_fixture_doc(
        _write_pdf(tmp_path / "manual.pdf"),
        data_dir,
        document_type="Service Manual",
        document_class="authoritative-technical",
        revision="Rev 3",
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
                    "instruction": "Verify the pump casing is vented.",
                    "note": None,
                    "caution": None,
                    "figure_ref": None,
                },
                {
                    **_base_payload(
                        manual_doc_id,
                        heading="Startup Procedure",
                        start_page=19,
                        end_page=19,
                        bucket_id=bucket_id,
                    ),
                    "step_number": 2,
                    "instruction": "Open the discharge valve before energizing the motor.",
                    "note": None,
                    "caution": None,
                    "figure_ref": None,
                },
                {
                    **_base_payload(
                        manual_doc_id,
                        heading="Startup Procedure",
                        start_page=20,
                        end_page=20,
                        bucket_id=bucket_id,
                    ),
                    "step_number": 2,
                    "instruction": "Keep the discharge valve closed before energizing the motor.",
                    "note": None,
                    "caution": None,
                    "figure_ref": None,
                },
            ],
            "applicability": _applicability_payload(manual_doc_id, bucket_id),
            "warnings": [],
            "tools_required": [],
        },
    )

    report = analyze_contradictions(bucket_id, data_dir=data_dir)

    assert len(report.contradictions) == 1
    contradiction = report.contradictions[0]
    assert contradiction.record_ids == ["startup-001::step-002", "startup-001::step-002--occurrence-002"]
    assert contradiction.record_ids[0] != contradiction.record_ids[1]


def test_find_supersession_assessments_flags_same_tier_revision_changes(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    bucket_id = "honeywell/dc1000/family"
    older_doc_id = _register_fixture_doc(
        _write_pdf(tmp_path / "manual-rev2.pdf"),
        data_dir,
        document_type="Service Manual",
        document_class="authoritative-technical",
        revision="Rev 2",
        publication_date="2024-01-15",
    )
    newer_doc_id = _register_fixture_doc(
        _write_pdf(tmp_path / "manual-rev3.pdf"),
        data_dir,
        document_type="Service Manual",
        document_class="authoritative-technical",
        revision="Rev 3",
        publication_date="2025-03-20",
    )

    for doc_id, instruction in (
        (older_doc_id, "Set parameter P-14 to 60 seconds before startup."),
        (newer_doc_id, "Set parameter P-14 to 90 seconds before startup."),
    ):
        _write_record(
            data_dir,
            doc_id,
            "procedure",
            f"startup-{doc_id[-4:]}",
            {
                **_base_payload(
                    doc_id,
                    heading="Startup Procedure",
                    start_page=12,
                    end_page=13,
                    bucket_id=bucket_id,
                ),
                "title": "Configure startup delay",
                "steps": [
                    {
                        **_base_payload(
                            doc_id,
                            heading="Startup Procedure",
                            start_page=12,
                            end_page=12,
                            bucket_id=bucket_id,
                        ),
                        "step_number": 1,
                        "instruction": instruction,
                        "note": None,
                        "caution": None,
                        "figure_ref": None,
                    }
                ],
                "applicability": _applicability_payload(doc_id, bucket_id),
                "warnings": [],
                "tools_required": [],
            },
        )

    assessments = find_supersession_assessments(bucket_id, data_dir=data_dir)

    assert len(assessments) == 1
    assessment = assessments[0]
    assert assessment.confidence == "medium"
    assert assessment.document_types_compared == ["Service Manual", "Service Manual"]
    assert "newer revision `Rev 3` supersedes `Rev 2`" in assessment.precedence_rule_applied


def test_find_supersession_assessments_marks_same_tier_cross_type_cases_low_confidence(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    bucket_id = "honeywell/dc1000/family"
    bulletin_doc_id = _register_fixture_doc(
        _write_pdf(tmp_path / "bulletin.pdf"),
        data_dir,
        document_type="Service Bulletin",
        document_class="authoritative-technical",
        revision="Rev A",
    )
    addendum_doc_id = _register_fixture_doc(
        _write_pdf(tmp_path / "addendum.pdf"),
        data_dir,
        document_type="Addendum",
        document_class="authoritative-technical",
        revision="Rev A",
    )

    for doc_id, document_type, text in (
        (bulletin_doc_id, "Service Bulletin", "Torque the terminal lugs to 14 N m."),
        (addendum_doc_id, "Addendum", "Torque the terminal lugs to 12 N m."),
    ):
        _write_record(
            data_dir,
            doc_id,
            "warning",
            f"warning-{slugify(document_type)}",
            {
                **_base_payload(doc_id, heading="Electrical Notice", start_page=2, end_page=2, bucket_id=bucket_id),
                "severity": "warning",
                "text": text,
                "context": "Terminal lugs",
                "applicability": _applicability_payload(doc_id, bucket_id),
            },
        )

    assessments = find_supersession_assessments(bucket_id, data_dir=data_dir)

    assert len(assessments) == 1
    assessment = assessments[0]
    assert assessment.confidence == "low"
    assert set(assessment.document_types_compared) == {"Addendum", "Service Bulletin"}
    assert "same precedence tier" in assessment.precedence_rule_applied


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
    assert "\thigh\t" in result.output


def test_render_contradiction_review_report_writes_markdown_and_decision_template(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    bucket_id = "honeywell/dc1000/family"
    bulletin_doc_id = _register_fixture_doc(
        _write_pdf(tmp_path / "bulletin.pdf"),
        data_dir,
        document_type="Service Bulletin",
        document_class="authoritative-technical",
        revision="Rev A",
    )
    addendum_doc_id = _register_fixture_doc(
        _write_pdf(tmp_path / "addendum.pdf"),
        data_dir,
        document_type="Addendum",
        document_class="authoritative-technical",
        revision="Rev A",
    )

    for doc_id, document_type, text in (
        (bulletin_doc_id, "Service Bulletin", "Torque the terminal lugs to 14 N m."),
        (addendum_doc_id, "Addendum", "Torque the terminal lugs to 12 N m."),
    ):
        _write_record(
            data_dir,
            doc_id,
            "warning",
            f"warning-{slugify(document_type)}",
            {
                **_base_payload(doc_id, heading="Electrical Notice", start_page=2, end_page=2, bucket_id=bucket_id),
                "severity": "warning",
                "text": text,
                "context": "Terminal lugs",
                "applicability": _applicability_payload(doc_id, bucket_id),
            },
        )

    artifacts = render_contradiction_review_report(bucket_id, data_dir=data_dir)

    assert artifacts.report_path == data_dir / "compiled" / "contradiction-notes" / "honeywell-dc1000-family-review.md"
    assert (
        artifacts.decision_path
        == data_dir / "compiled" / "contradiction-notes" / "honeywell-dc1000-family-review-status.json"
    )
    report_text = artifacts.report_path.read_text(encoding="utf-8")
    assert "Service Bulletin" in report_text
    assert "Addendum" in report_text
    assert "Review status: `unreviewed`" in report_text
    assert "p.2" in report_text
    assert "Review Required" in report_text
    assert "same precedence tier" in report_text

    decision_payload = json.loads(artifacts.decision_path.read_text(encoding="utf-8"))
    assert decision_payload["bucket_id"] == bucket_id
    assert decision_payload["candidates"][0]["review_status"] == "unreviewed"


def test_render_contradiction_review_report_preserves_saved_decisions(tmp_path: Path) -> None:
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

    for doc_id, instruction in (
        (manual_doc_id, "Open the discharge valve before energizing the motor."),
        (sop_doc_id, "Keep the discharge valve closed before energizing the motor."),
    ):
        _write_record(
            data_dir,
            doc_id,
            "procedure",
            f"startup-{doc_id[-4:]}",
            {
                **_base_payload(doc_id, heading="Startup Procedure", start_page=4, end_page=4, bucket_id=bucket_id),
                "title": "Prime the pump",
                "steps": [
                    {
                        **_base_payload(
                            doc_id,
                            heading="Startup Procedure",
                            start_page=4,
                            end_page=4,
                            bucket_id=bucket_id,
                        ),
                        "step_number": 1,
                        "instruction": instruction,
                        "note": None,
                        "caution": None,
                        "figure_ref": None,
                    }
                ],
                "applicability": _applicability_payload(doc_id, bucket_id),
                "warnings": [],
                "tools_required": [],
            },
        )

    first_artifacts = render_contradiction_review_report(bucket_id, data_dir=data_dir)
    decision_payload = json.loads(first_artifacts.decision_path.read_text(encoding="utf-8"))
    decision_payload["candidates"][0]["review_status"] = "approved"
    decision_payload["candidates"][0]["reviewer"] = "Taylor"
    decision_payload["candidates"][0]["reviewed_at"] = "2026-04-16T12:00:00Z"
    decision_payload["candidates"][0]["notes"] = "Service manual takes precedence."
    first_artifacts.decision_path.write_text(json.dumps(decision_payload, indent=2), encoding="utf-8")

    second_artifacts = render_contradiction_review_report(bucket_id, data_dir=data_dir)
    report_text = second_artifacts.report_path.read_text(encoding="utf-8")

    assert "Review status: `approved`" in report_text
    assert "Reviewer: Taylor" in report_text
    assert "Service manual takes precedence." in report_text


def test_analyze_review_cli_generates_review_report(tmp_path: Path) -> None:
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

    for doc_id, instruction in (
        (manual_doc_id, "Open the discharge valve before energizing the motor."),
        (sop_doc_id, "Keep the discharge valve closed before energizing the motor."),
    ):
        _write_record(
            data_dir,
            doc_id,
            "procedure",
            f"startup-{doc_id[-4:]}",
            {
                **_base_payload(doc_id, heading="Startup Procedure", start_page=4, end_page=4, bucket_id=bucket_id),
                "title": "Prime the pump",
                "steps": [
                    {
                        **_base_payload(
                            doc_id,
                            heading="Startup Procedure",
                            start_page=4,
                            end_page=4,
                            bucket_id=bucket_id,
                        ),
                        "step_number": 1,
                        "instruction": instruction,
                        "note": None,
                        "caution": None,
                        "figure_ref": None,
                    }
                ],
                "applicability": _applicability_payload(doc_id, bucket_id),
                "warnings": [],
                "tools_required": [],
            },
        )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["analyze", "review", bucket_id],
        env={"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)},
    )

    assert result.exit_code == 0
    assert f"Bucket: {bucket_id}" in result.output
    assert "Review report:" in result.output
    assert "Review decisions:" in result.output
    assert "Candidates: 1" in result.output


def test_find_supersession_assessments_uses_publication_date_when_revisions_are_equal(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    bucket_id = "honeywell/dc1000/family"
    # Use revision strings whose numeric sort key is identical (both parse to (2, 3))
    # so _revision_or_date_supersession skips the revision path and falls through to
    # publication_date.  Distinct revision strings are required because doc_id is
    # derived from revision, and two identical doc_ids would collide during registration.
    older_doc_id = _register_fixture_doc(
        _write_pdf(tmp_path / "manual-2023.pdf"),
        data_dir,
        document_type="Service Manual",
        document_class="authoritative-technical",
        revision="Rev-3-2023",
        publication_date="2023-06-01",
    )
    newer_doc_id = _register_fixture_doc(
        _write_pdf(tmp_path / "manual-2025.pdf"),
        data_dir,
        document_type="Service Manual",
        document_class="authoritative-technical",
        revision="Rev-3-2025",
        publication_date="2025-01-15",
    )

    for doc_id, instruction in (
        (older_doc_id, "Set parameter P-22 to 45 seconds before initiating shutdown."),
        (newer_doc_id, "Set parameter P-22 to 60 seconds before initiating shutdown."),
    ):
        _write_record(
            data_dir,
            doc_id,
            "procedure",
            f"shutdown-{doc_id[-4:]}",
            {
                **_base_payload(doc_id, heading="Shutdown Procedure", start_page=14, end_page=15, bucket_id=bucket_id),
                "title": "Configure shutdown delay",
                "steps": [
                    {
                        **_base_payload(
                            doc_id,
                            heading="Shutdown Procedure",
                            start_page=14,
                            end_page=14,
                            bucket_id=bucket_id,
                        ),
                        "step_number": 1,
                        "instruction": instruction,
                        "note": None,
                        "caution": None,
                        "figure_ref": None,
                    }
                ],
                "applicability": _applicability_payload(doc_id, bucket_id),
                "warnings": [],
                "tools_required": [],
            },
        )

    assessments = find_supersession_assessments(bucket_id, data_dir=data_dir)

    assert len(assessments) == 1
    assessment = assessments[0]
    assert assessment.confidence == "medium"
    assert assessment.document_types_compared == ["Service Manual", "Service Manual"]
    assert "newer publication date" in assessment.precedence_rule_applied
    assert "2025-01-15" in assessment.precedence_rule_applied
    assert "2023-06-01" in assessment.precedence_rule_applied


def test_analyze_supersession_cli_reports_assessments(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    bucket_id = "honeywell/dc1000/family"
    bulletin_doc_id = _register_fixture_doc(
        _write_pdf(tmp_path / "bulletin.pdf"),
        data_dir,
        document_type="Service Bulletin",
        document_class="authoritative-technical",
        revision="Rev C",
    )
    manual_doc_id = _register_fixture_doc(
        _write_pdf(tmp_path / "manual.pdf"),
        data_dir,
        document_type="Service Manual",
        document_class="authoritative-technical",
        revision="Original",
    )

    for doc_id, text in (
        (bulletin_doc_id, "Replace fuse F2 with a 4A slow-blow fuse."),
        (manual_doc_id, "Replace fuse F2 with a 2A slow-blow fuse."),
    ):
        _write_record(
            data_dir,
            doc_id,
            "warning",
            f"fuse-{doc_id[-4:]}",
            {
                **_base_payload(doc_id, heading="Fuse Service", start_page=6, end_page=6, bucket_id=bucket_id),
                "severity": "warning",
                "text": text,
                "context": "Fuse replacement",
                "applicability": _applicability_payload(doc_id, bucket_id),
            },
        )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["analyze", "supersession", bucket_id],
        env={"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)},
    )

    assert result.exit_code == 0
    assert f"Bucket: {bucket_id}" in result.output
    assert "Supersession assessments: 1" in result.output
    assert "SUPERSESSION ASSESSMENTS" in result.output
    assert "\thigh\t" in result.output
