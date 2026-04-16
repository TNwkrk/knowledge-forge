"""Tests for source-page compilation."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from knowledge_forge.cli import cli
from knowledge_forge.compile import compile_source_page
from knowledge_forge.intake.importer import RegistrationRequest, load_manifest, register_document
from knowledge_forge.intake.manifest import BucketAssignment, DocumentStatus
from knowledge_forge.parse.sectioning import Section


def _write_pdf(path: Path) -> Path:
    payload = b"%PDF-1.4\n1 0 obj\n<< /Title (" + path.stem.encode("utf-8") + b") >>\nendobj\ntrailer\n<<>>\n%%EOF\n"
    path.write_bytes(payload)
    return path


def _register_extracted_fixture(pdf_path: Path, data_dir: Path, *, revision: str = "Rev 3") -> str:
    request = RegistrationRequest(
        pdf_path=pdf_path,
        manufacturer="Honeywell",
        family="DC1000",
        model_applicability=["DC1000", "DC1200"],
        document_type="Service Manual",
        revision=revision,
        publication_date=None,
        language="en",
        priority=1,
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
                        bucket_id="honeywell/dc1000/family",
                        dimension="family",
                        value="DC1000",
                    )
                ]
            }
        ).to_yaml(),
        encoding="utf-8",
    )
    return result.manifest.doc_id


def _write_section(
    data_dir: Path,
    doc_id: str,
    *,
    section_id: str,
    title: str,
    section_type: str,
    page_range: tuple[int, int],
) -> None:
    path = data_dir / "sections" / doc_id / f"{section_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        Section(
            doc_id=doc_id,
            section_id=section_id,
            section_type=section_type,
            title=title,
            content=f"{title} content",
            page_range=page_range,
            heading_path=["DC1000 Service Manual", title],
        ).model_dump_json(indent=2),
        encoding="utf-8",
    )


def _base_record(doc_id: str, *, heading: str, start_page: int, end_page: int, confidence: float) -> dict[str, object]:
    return {
        "source_doc_id": doc_id,
        "source_page_range": {"start_page": start_page, "end_page": end_page},
        "source_heading": heading,
        "parser_version": "docling-1.2.0",
        "extraction_version": "extraction/procedure@v1:gpt-4o-mini",
        "confidence": confidence,
        "bucket_context": [
            {"bucket_id": "honeywell/dc1000/family", "dimension": "family", "value": "DC1000"},
        ],
    }


def _write_record(data_dir: Path, doc_id: str, record_type: str, record_id: str, payload: dict[str, object]) -> None:
    path = data_dir / "extracted" / doc_id / record_type / f"{record_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_review_flag(data_dir: Path, doc_id: str, section_id: str) -> None:
    path = data_dir / "extracted" / doc_id / "reviews" / f"{section_id}--procedure.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "doc_id": doc_id,
                "section_id": section_id,
                "record_type": "procedure",
                "reasons": ["below_min_confidence"],
                "min_confidence": 0.8,
                "record_ids": [f"{section_id}--procedure--001"],
                "record_confidences": [0.61],
                "repair_attempts": 1,
                "errors": ["schema repair applied"],
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def test_compile_source_page_renders_frontmatter_sections_summary_and_quality_notes(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_extracted_fixture(_write_pdf(tmp_path / "manual.pdf"), data_dir)
    startup_section = f"{doc_id}--startup--001"
    safety_section = f"{doc_id}--safety--001"
    _write_section(
        data_dir,
        doc_id,
        section_id=startup_section,
        title="Startup Procedure",
        section_type="startup",
        page_range=(18, 20),
    )
    _write_section(
        data_dir,
        doc_id,
        section_id=safety_section,
        title="Safety Warnings",
        section_type="safety",
        page_range=(4, 5),
    )
    _write_record(
        data_dir,
        doc_id,
        "procedure",
        f"{startup_section}--procedure--001",
        {
            **_base_record(doc_id, heading="Startup Procedure", start_page=18, end_page=20, confidence=0.61),
            "title": "Start the controller",
            "steps": [
                {
                    **_base_record(doc_id, heading="Startup Procedure", start_page=18, end_page=18, confidence=0.61),
                    "step_number": 1,
                    "instruction": "Verify the discharge valve is open.",
                    "note": None,
                    "caution": None,
                    "figure_ref": None,
                }
            ],
            "applicability": None,
            "warnings": [],
            "tools_required": ["multimeter"],
        },
    )
    _write_record(
        data_dir,
        doc_id,
        "warning",
        f"{safety_section}--warning--001",
        {
            **_base_record(doc_id, heading="Safety Warnings", start_page=4, end_page=5, confidence=0.92),
            "severity": "warning",
            "text": "Disconnect power before servicing.",
            "context": "Electrical hazard",
            "applicability": None,
        },
    )
    _write_review_flag(data_dir, doc_id, startup_section)

    page = compile_source_page(doc_id, data_dir=data_dir)
    rendered = page.render()

    assert page.output_path == data_dir / "compiled" / "source-pages" / f"{doc_id}.md"
    assert page.output_path.exists()
    assert "title: 'Source Manual: Honeywell DC1000 Service Manual (Rev 3)'" in rendered
    assert "publish_run: unpublished" in rendered
    assert "## Section Index" in rendered
    assert "[Startup Procedure](#startup-procedure)" in rendered
    assert "`procedure`: 1" in rendered
    assert "`warning`: 1" in rendered
    assert "Low-confidence records (threshold: 0.80):" in rendered
    assert "Review flags:" in rendered
    assert "schema repair applied" in rendered
    assert "## Provenance Chain" in rendered
    assert f"[artifact](../../extracted/{doc_id}/" in rendered

    manifest = load_manifest(data_dir, doc_id)
    assert manifest.document.status == DocumentStatus.COMPILED


def test_compile_source_page_cli_supports_single_document_and_all(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    first_doc_id = _register_extracted_fixture(_write_pdf(tmp_path / "manual-1.pdf"), data_dir)
    second_doc_id = _register_extracted_fixture(_write_pdf(tmp_path / "manual-2.pdf"), data_dir, revision="Rev 4")
    for doc_id, title in ((first_doc_id, "Startup Procedure"), (second_doc_id, "Safety Warnings")):
        section_id = f"{doc_id}--other--001"
        _write_section(
            data_dir,
            doc_id,
            section_id=section_id,
            title=title,
            section_type="other",
            page_range=(1, 1),
        )
        _write_record(
            data_dir,
            doc_id,
            "warning",
            f"{section_id}--warning--001",
            {
                **_base_record(doc_id, heading=title, start_page=1, end_page=1, confidence=0.9),
                "severity": "warning",
                "text": f"{title} warning",
                "context": None,
                "applicability": None,
            },
        )

    runner = CliRunner()
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)}

    single = runner.invoke(cli, ["compile", "source-pages", first_doc_id], env=env)
    assert single.exit_code == 0
    assert f"Compiled source page for {first_doc_id}" in single.output

    every = runner.invoke(cli, ["compile", "source-pages", "--all"], env=env)
    assert every.exit_code == 0
    assert "Compiled 2 source page(s)." in every.output
    assert first_doc_id in every.output
    assert second_doc_id in every.output
