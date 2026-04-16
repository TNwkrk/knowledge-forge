"""Tests for extraction provenance helpers and CLI audit flow."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from knowledge_forge.cli import cli
from knowledge_forge.extract import (
    BucketContext,
    Procedure,
    ProcedureStep,
    attach_provenance,
    audit_document_provenance,
)
from knowledge_forge.extract.engine import save_records
from knowledge_forge.extract.provenance import ExtractionMetadata
from knowledge_forge.intake.importer import RegistrationRequest, register_document
from knowledge_forge.intake.manifest import BucketAssignment, DocumentStatus
from knowledge_forge.parse.quality import ParseMetadata
from knowledge_forge.parse.sectioning import Section


def _write_pdf(path: Path) -> Path:
    path.write_bytes(b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\ntrailer\n<<>>\n%%EOF\n")
    return path


def _register_bucketed_doc(data_dir: Path, pdf_path: Path) -> str:
    request = RegistrationRequest(
        pdf_path=pdf_path,
        manufacturer="Honeywell",
        family="DC1000",
        model_applicability=["DC1000"],
        document_type="Service Manual",
        revision="Rev 3",
        publication_date=None,
        language="en",
        priority=1,
    )
    result = register_document(request, data_dir=data_dir)
    manifest_path = data_dir / "manifests" / f"{result.manifest.doc_id}.yaml"
    manifest = result.manifest.transition_status(DocumentStatus.PARSED, reason="test parsed fixture")
    manifest = manifest.model_copy(
        update={
            "bucket_assignments": [
                BucketAssignment(
                    doc_id=result.manifest.doc_id,
                    bucket_id="honeywell/dc1000/family",
                    dimension="family",
                    value="DC1000",
                )
            ]
        }
    )
    manifest_path.write_text(manifest.to_yaml(), encoding="utf-8")
    return result.manifest.doc_id


def test_attach_provenance_overwrites_nested_records() -> None:
    section = Section(
        doc_id="honeywell-dc1000-service-manual-rev3",
        section_id="honeywell-dc1000-service-manual-rev3--startup--001",
        section_type="startup",
        title="Startup Procedure",
        content="Verify the valve is open.",
        page_range=(18, 20),
        heading_path=["Startup Procedure"],
    )
    parse_meta = ParseMetadata(
        doc_id=section.doc_id,
        parser="docling",
        parser_version="docling-2.0.0",
        processed_at="2026-04-16T00:00:00Z",
        processing_time_seconds=0.5,
        page_count=40,
        status="success",
        input_path="/tmp/manual.pdf",
        input_checksum="a" * 64,
        document_hash=None,
        timings={},
        confidence=None,
        errors=[],
    )
    procedure = Procedure.model_validate(
        {
            "source_doc_id": "wrong-doc",
            "source_page_range": {"start_page": 1, "end_page": 1},
            "source_heading": "Wrong",
            "parser_version": "old",
            "extraction_version": "old",
            "confidence": 0.1,
            "bucket_context": [{"bucket_id": "wrong", "dimension": "family", "value": "wrong"}],
            "title": "Start controller",
            "steps": [
                {
                    "source_doc_id": "wrong-doc",
                    "source_page_range": {"start_page": 1, "end_page": 1},
                    "source_heading": "Wrong",
                    "parser_version": "old",
                    "extraction_version": "old",
                    "confidence": 0.1,
                    "bucket_context": [{"bucket_id": "wrong", "dimension": "family", "value": "wrong"}],
                    "step_number": 1,
                    "instruction": "Verify the valve is open.",
                    "note": None,
                    "caution": None,
                    "figure_ref": None,
                }
            ],
            "applicability": None,
            "warnings": [],
            "tools_required": [],
        }
    )

    attached = attach_provenance(
        procedure,
        section,
        parse_meta,
        ExtractionMetadata(
            model="gpt-4o-mini",
            prompt_template="extraction/procedure",
            prompt_version="v1",
            confidence=0.83,
            bucket_context=[BucketContext(bucket_id="honeywell/dc1000/family", dimension="family", value="DC1000")],
        ),
    )

    assert attached.source_doc_id == section.doc_id
    assert attached.source_heading == "Startup Procedure"
    assert attached.parser_version == "docling-2.0.0"
    assert attached.extraction_version == "extraction/procedure@v1:gpt-4o-mini"
    assert attached.steps[0].source_doc_id == section.doc_id
    assert attached.steps[0].source_page_range.start_page == 18


def test_attach_provenance_raises_on_missing_page_range() -> None:
    section = Section(
        doc_id="honeywell-dc1000-service-manual-rev3",
        section_id="honeywell-dc1000-service-manual-rev3--startup--001",
        section_type="startup",
        title="Startup Procedure",
        content="Verify the valve is open.",
        page_range=(None, None),
        heading_path=["Startup Procedure"],
    )
    parse_meta = ParseMetadata(
        doc_id=section.doc_id,
        parser="docling",
        parser_version="docling-2.0.0",
        processed_at="2026-04-16T00:00:00Z",
        processing_time_seconds=0.5,
        page_count=40,
        status="success",
        input_path="/tmp/manual.pdf",
        input_checksum="a" * 64,
        document_hash=None,
        timings={},
        confidence=None,
        errors=[],
    )
    procedure = Procedure.model_validate(
        {
            "source_doc_id": section.doc_id,
            "source_page_range": {"start_page": 1, "end_page": 1},
            "source_heading": "Startup Procedure",
            "parser_version": "docling-2.0.0",
            "extraction_version": "extraction/procedure@v1:gpt-4o-mini",
            "confidence": 0.8,
            "bucket_context": [{"bucket_id": "honeywell/dc1000/family", "dimension": "family", "value": "DC1000"}],
            "title": "Start controller",
            "steps": [
                {
                    "source_doc_id": section.doc_id,
                    "source_page_range": {"start_page": 1, "end_page": 1},
                    "source_heading": "Startup Procedure",
                    "parser_version": "docling-2.0.0",
                    "extraction_version": "extraction/procedure@v1:gpt-4o-mini",
                    "confidence": 0.8,
                    "bucket_context": [
                        {"bucket_id": "honeywell/dc1000/family", "dimension": "family", "value": "DC1000"}
                    ],
                    "step_number": 1,
                    "instruction": "Verify the valve is open.",
                    "note": None,
                    "caution": None,
                    "figure_ref": None,
                }
            ],
            "applicability": None,
            "warnings": [],
            "tools_required": [],
        }
    )

    with pytest.raises(ValueError, match="missing page_range"):
        attach_provenance(
            procedure,
            section,
            parse_meta,
            ExtractionMetadata(
                model="gpt-4o-mini",
                prompt_template="extraction/procedure",
                prompt_version="v1",
                confidence=0.8,
                bucket_context=[BucketContext(bucket_id="honeywell/dc1000/family", dimension="family", value="DC1000")],
            ),
        )


def test_save_records_rejects_missing_provenance(tmp_path: Path) -> None:
    section = Section(
        doc_id="doc-001",
        section_id="doc-001--startup--001",
        section_type="startup",
        title="Startup Procedure",
        content="Verify the valve is open.",
        page_range=(1, 1),
        heading_path=["Startup Procedure"],
    )
    record = ProcedureStep.model_construct(
        source_doc_id="",
        source_page_range={"start_page": 1, "end_page": 1},
        source_heading="",
        parser_version="",
        extraction_version="",
        confidence=0.5,
        bucket_context=[],
        step_number=1,
        instruction="Verify the valve is open.",
        note=None,
        caution=None,
        figure_ref=None,
    )

    with pytest.raises(ValueError, match="record has invalid or incomplete provenance"):
        save_records(section=section, record_type="procedure_step", records=[record], data_dir=tmp_path / "data")


def test_audit_document_provenance_reports_invalid_records(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_bucketed_doc(data_dir, _write_pdf(tmp_path / "manual.pdf"))
    record_dir = data_dir / "extracted" / doc_id / "procedure_step"
    record_dir.mkdir(parents=True, exist_ok=True)
    record_path = record_dir / f"{doc_id}--startup--001--procedure_step--001.json"
    record_path.write_text(
        json.dumps(
            {
                "source_doc_id": doc_id,
                "source_page_range": {"start_page": 1, "end_page": 1},
                "source_heading": "",
                "parser_version": "docling",
                "extraction_version": "extraction/procedure@v1:gpt-4o-mini",
                "confidence": 0.7,
                "bucket_context": [{"bucket_id": "honeywell/dc1000/family", "dimension": "family", "value": "DC1000"}],
                "step_number": 1,
                "instruction": "Verify the valve is open.",
                "note": None,
                "caution": None,
                "figure_ref": None,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    report = audit_document_provenance(doc_id, data_dir=data_dir)

    assert report.total_records == 1
    assert report.invalid_records == 1
    assert report.rows[0].record_type == "procedure_step"


def test_extract_provenance_cli_reports_summary(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    runner = CliRunner()

    monkeypatch.setattr(
        "knowledge_forge.cli.audit_document_provenance",
        lambda doc_id, data_dir=None: type(
            "AuditReport",
            (),
            {
                "doc_id": doc_id,
                "total_records": 2,
                "valid_records": 1,
                "invalid_records": 1,
                "rows": [
                    type(
                        "Row",
                        (),
                        {
                            "valid": False,
                            "record_type": "procedure",
                            "record_id": "rec-001",
                            "errors": ["missing heading"],
                        },
                    )()
                ],
            },
        )(),
    )

    result = runner.invoke(cli, ["extract", "provenance", "doc-001"], env={"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)})

    assert result.exit_code == 0
    assert "Document: doc-001" in result.output
    assert "Invalid provenance: 1" in result.output
    assert "procedure\trec-001\tmissing heading" in result.output
