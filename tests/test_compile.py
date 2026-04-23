"""Tests for source-page compilation."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from knowledge_forge.cli import cli
from knowledge_forge.compile import (
    compile_all_overviews,
    compile_bucket_topic_pages,
    compile_family_overview,
    compile_manufacturer_index,
    compile_source_page,
    compile_topic_page,
    render_contradiction_notes,
)
from knowledge_forge.intake.importer import RegistrationRequest, load_manifest, register_document
from knowledge_forge.intake.manifest import BucketAssignment, DocumentStatus
from knowledge_forge.parse.sectioning import Section


def _write_pdf(path: Path) -> Path:
    payload = b"%PDF-1.4\n1 0 obj\n<< /Title (" + path.stem.encode("utf-8") + b") >>\nendobj\ntrailer\n<<>>\n%%EOF\n"
    path.write_bytes(payload)
    return path


def _register_extracted_fixture(
    pdf_path: Path,
    data_dir: Path,
    *,
    revision: str = "Rev 3",
    document_type: str = "Service Manual",
    document_class: str = "authoritative-technical",
    bucket_dimension: str = "family",
) -> str:
    request = RegistrationRequest(
        pdf_path=pdf_path,
        manufacturer="Honeywell",
        family="DC1000",
        model_applicability=["DC1000", "DC1200"],
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
                        bucket_id="honeywell/dc1000/family",
                        dimension=bucket_dimension,
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


def _ensure_extracted_doc_dir(data_dir: Path, doc_id: str) -> None:
    (data_dir / "extracted" / doc_id).mkdir(parents=True, exist_ok=True)


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


class _FakeCompileConfig:
    compilation_model = "gpt-4o-mini"


class _FakeCompileClient:
    def __init__(self, response_text: str) -> None:
        self.response_text = response_text
        self.calls: list[dict[str, object]] = []
        self.config = _FakeCompileConfig()

    def complete(
        self,
        prompt: str,
        system: str,
        model: str | None = None,
        schema: dict[str, object] | None = None,
        **kwargs: object,
    ):
        self.calls.append(
            {
                "prompt": prompt,
                "system": system,
                "model": model,
                "schema": schema,
                **kwargs,
            }
        )
        return type(
            "FakeResult",
            (),
            {
                "response_text": self.response_text,
            },
        )()


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


def test_compile_source_page_suppresses_non_reviewable_sections_from_reviewer_facing_output(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_extracted_fixture(_write_pdf(tmp_path / "manual.pdf"), data_dir)
    startup_section = f"{doc_id}--startup--001"
    noisy_section = f"{doc_id}--continued--001"
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
        section_id=noisy_section,
        title="continued",
        section_type="other",
        page_range=(21, 21),
    )
    _write_record(
        data_dir,
        doc_id,
        "procedure",
        f"{startup_section}--procedure--001",
        {
            **_base_record(doc_id, heading="Startup Procedure", start_page=18, end_page=20, confidence=0.92),
            "title": "Start the controller",
            "steps": [
                {
                    **_base_record(doc_id, heading="Startup Procedure", start_page=18, end_page=18, confidence=0.92),
                    "step_number": 1,
                    "instruction": "Apply control power.",
                    "note": None,
                    "caution": None,
                    "figure_ref": None,
                }
            ],
            "applicability": None,
            "warnings": [],
            "tools_required": [],
        },
    )

    page = compile_source_page(doc_id, data_dir=data_dir)
    rendered = page.render()

    section_index = rendered.split("## Section Index", 1)[1].split("## Extraction Summary", 1)[0]
    assert "Startup Procedure" in section_index
    assert "continued" not in section_index
    assert "Suppressed non-reviewable sections from reviewer-facing output: 1" in rendered


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


def test_compile_topic_page_renders_citations_applicability_notes_and_conflicts(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    service_doc_id = _register_extracted_fixture(
        _write_pdf(tmp_path / "manual-service.pdf"), data_dir, revision="Rev 3"
    )
    bulletin_doc_id = _register_extracted_fixture(
        _write_pdf(tmp_path / "manual-bulletin.pdf"),
        data_dir,
        document_type="SOP",
        document_class="operational",
        revision="Rev 3",
    )
    startup_section = f"{service_doc_id}--startup--001"
    startup_bulletin_section = f"{bulletin_doc_id}--startup--001"
    spec_section = f"{bulletin_doc_id}--specifications--001"

    _write_section(
        data_dir,
        service_doc_id,
        section_id=startup_section,
        title="Startup Procedure",
        section_type="startup",
        page_range=(18, 20),
    )
    _write_section(
        data_dir,
        bulletin_doc_id,
        section_id=startup_bulletin_section,
        title="Startup Procedure",
        section_type="startup",
        page_range=(21, 22),
    )
    _write_section(
        data_dir,
        bulletin_doc_id,
        section_id=spec_section,
        title="Technical Data",
        section_type="specifications",
        page_range=(30, 31),
    )
    _write_record(
        data_dir,
        service_doc_id,
        "procedure",
        f"{startup_section}--procedure--001",
        {
            **_base_record(
                service_doc_id,
                heading="Startup Procedure",
                start_page=18,
                end_page=20,
                confidence=0.9,
            ),
            "title": "Start the controller",
            "steps": [
                {
                    **_base_record(
                        service_doc_id, heading="Startup Procedure", start_page=18, end_page=18, confidence=0.9
                    ),
                    "step_number": 1,
                    "instruction": "Open the discharge valve before energizing the unit.",
                    "note": None,
                    "caution": None,
                    "figure_ref": None,
                }
            ],
            "applicability": {
                **_base_record(
                    service_doc_id,
                    heading="Startup Procedure",
                    start_page=18,
                    end_page=20,
                    confidence=0.9,
                ),
                "manufacturer": "Honeywell",
                "family": "DC1000",
                "models": ["DC1000"],
                "serial_range": None,
                "revision": "Rev 3",
            },
            "warnings": [],
            "tools_required": ["multimeter"],
        },
    )
    _write_record(
        data_dir,
        bulletin_doc_id,
        "procedure",
        f"{startup_bulletin_section}--procedure--001",
        {
            **_base_record(
                bulletin_doc_id,
                heading="Startup Procedure",
                start_page=21,
                end_page=22,
                confidence=0.94,
            ),
            "title": "Start the controller",
            "steps": [
                {
                    **_base_record(
                        bulletin_doc_id, heading="Startup Procedure", start_page=21, end_page=21, confidence=0.94
                    ),
                    "step_number": 1,
                    "instruction": "Do not open the discharge valve before energizing the unit.",
                    "note": None,
                    "caution": None,
                    "figure_ref": None,
                }
            ],
            "applicability": {
                **_base_record(
                    bulletin_doc_id, heading="Startup Procedure", start_page=21, end_page=22, confidence=0.94
                ),
                "manufacturer": "Honeywell",
                "family": "DC1000",
                "models": ["DC1000", "DC1200"],
                "serial_range": None,
                "revision": "Rev 3",
            },
            "warnings": [],
            "tools_required": [],
        },
    )
    _write_record(
        data_dir,
        bulletin_doc_id,
        "spec_value",
        f"{spec_section}--spec_value--001",
        {
            **_base_record(
                bulletin_doc_id,
                heading="Technical Data",
                start_page=30,
                end_page=31,
                confidence=0.95,
            ),
            "parameter": "Operating pressure",
            "value": "15",
            "unit": "PSI",
            "conditions": "startup mode",
            "applicability": None,
        },
    )

    client = _FakeCompileClient("- Consolidated startup flow [Source: example, p.1]")
    startup_page = compile_topic_page(
        "honeywell/dc1000/family",
        "startup_procedure",
        client=client,
        data_dir=data_dir,
    )
    rendered_startup = startup_page.render()

    assert (
        startup_page.output_path
        == data_dir / "compiled" / "topic-pages" / "honeywell-dc1000-family" / "startup_procedure.md"
    )
    assert "title: Honeywell DC1000 Startup Procedure" in rendered_startup
    assert "topic: startup_procedure" in rendered_startup
    assert "## Draft Synthesis" in rendered_startup
    assert (
        "Open the discharge valve before energizing the unit. "
        "[Source: honeywell-dc1000-service-manual-rev-3, p.18]" in rendered_startup
    )
    assert (
        "Do not open the discharge valve before energizing the unit. "
        "[Source: honeywell-dc1000-sop-rev-3, p.21]" in rendered_startup
    )
    assert "## Applicability Differences" in rendered_startup
    assert "models: DC1000" in rendered_startup
    assert "models: DC1000, DC1200" in rendered_startup
    assert "## Potential Contradictions" in rendered_startup
    assert "> [!WARNING] Contradiction" in rendered_startup
    assert 'section "Startup Procedure", p.18, revised manual, level 2' in rendered_startup
    assert 'section "Startup Procedure", p.21, internal SOP or best practice, level 5' in rendered_startup
    assert "Recommended resolution: Retain `honeywell-dc1000-service-manual-rev-3`" in rendered_startup
    assert client.calls[0]["prompt_template"] == "compilation/topic_page"

    specs_page = compile_topic_page(
        "honeywell/dc1000/family",
        "specifications",
        client=_FakeCompileClient(""),
        data_dir=data_dir,
    )
    rendered_specs = specs_page.render()
    assert "Operating pressure: 15 PSI (startup mode) [Source: honeywell-dc1000-sop-rev-3, pp.30-31]" in rendered_specs


def test_compile_alarm_reference_draft_synthesis_keeps_prose_when_citations_wrap(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_extracted_fixture(_write_pdf(tmp_path / "manual-alarm.pdf"), data_dir)
    section_id = f"{doc_id}--alarm-reference--001"

    _write_section(
        data_dir,
        doc_id,
        section_id=section_id,
        title="Alarm Reference",
        section_type="troubleshooting",
        page_range=(40, 41),
    )
    _write_record(
        data_dir,
        doc_id,
        "alarm_definition",
        f"{section_id}--alarm_definition--001",
        {
            **_base_record(doc_id, heading="Alarm Reference", start_page=40, end_page=41, confidence=0.92),
            "extraction_version": "extraction/alarm_definition@v1:gpt-4o-mini",
            "code": "RUN",
            "description": "Indicates the controller is in run mode.",
            "cause": "The controller is executing the user program.",
            "remedy": "No action required if operation is normal.",
            "severity": "info",
        },
    )

    page = compile_topic_page(
        "honeywell/dc1000/family",
        "alarm_reference",
        client=_FakeCompileClient(
            "- **RUN** indicates the controller is executing the user program.\n"
            "  [Source: honeywell-dc1000-service-manual-rev-3, pp.40-41]"
        ),
        data_dir=data_dir,
    )

    rendered = page.render()
    draft_synthesis = rendered.split("## Draft Synthesis", 1)[1].split("## Source-backed Claims", 1)[0]

    assert (
        "- **RUN** indicates the controller is executing the user program. "
        "[Source: honeywell-dc1000-service-manual-rev-3, pp.40-41]" in draft_synthesis
    )
    assert "## Source-backed Claims" in rendered
    assert draft_synthesis.strip() != "[Source: honeywell-dc1000-service-manual-rev-3, pp.40-41]"
    assert "\n  [Source:" not in draft_synthesis


def test_compile_topic_page_filters_low_signal_other_sections(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_extracted_fixture(_write_pdf(tmp_path / "manual-specs.pdf"), data_dir)
    valid_section = f"{doc_id}--specifications--001"
    noisy_section = f"{doc_id}--continued--001"
    low_confidence_other = f"{doc_id}--technical-data--002"

    _write_section(
        data_dir,
        doc_id,
        section_id=valid_section,
        title="Technical Data",
        section_type="specifications",
        page_range=(30, 31),
    )
    _write_section(
        data_dir,
        doc_id,
        section_id=noisy_section,
        title="continued",
        section_type="other",
        page_range=(32, 32),
    )
    _write_section(
        data_dir,
        doc_id,
        section_id=low_confidence_other,
        title="Technical Data",
        section_type="other",
        page_range=(33, 33),
    )
    _write_record(
        data_dir,
        doc_id,
        "spec_value",
        f"{valid_section}--spec_value--001",
        {
            **_base_record(doc_id, heading="Technical Data", start_page=30, end_page=31, confidence=0.95),
            "extraction_version": "extraction/spec_value@v1:gpt-4o-mini",
            "parameter": "Operating pressure",
            "value": "15",
            "unit": "PSI",
            "conditions": "startup mode",
            "applicability": None,
        },
    )
    _write_record(
        data_dir,
        doc_id,
        "spec_value",
        f"{noisy_section}--spec_value--001",
        {
            **_base_record(doc_id, heading="continued", start_page=32, end_page=32, confidence=0.99),
            "extraction_version": "extraction/spec_value@v1:gpt-4o-mini",
            "parameter": "Noise row",
            "value": "999",
            "unit": "PSI",
            "conditions": None,
            "applicability": None,
        },
    )
    _write_record(
        data_dir,
        doc_id,
        "spec_value",
        f"{low_confidence_other}--spec_value--001",
        {
            **_base_record(doc_id, heading="Technical Data", start_page=33, end_page=33, confidence=0.61),
            "extraction_version": "extraction/spec_value@v1:gpt-4o-mini",
            "parameter": "Ignored low-confidence row",
            "value": "12",
            "unit": "PSI",
            "conditions": None,
            "applicability": None,
        },
    )

    page = compile_topic_page(
        "honeywell/dc1000/family",
        "specifications",
        client=_FakeCompileClient(""),
        data_dir=data_dir,
    )
    rendered = page.render()

    assert "Operating pressure: 15 PSI (startup mode)" in rendered
    assert "Noise row: 999 PSI" not in rendered
    assert "Ignored low-confidence row: 12 PSI" not in rendered


def test_compile_topic_page_filters_ui_admin_shutdown_procedures(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    power_doc_id = _register_extracted_fixture(_write_pdf(tmp_path / "power.pdf"), data_dir)
    hmi_doc_id = _register_extracted_fixture(
        _write_pdf(tmp_path / "hmi.pdf"),
        data_dir,
        document_type="Operation Manual",
    )
    shutdown_section = f"{power_doc_id}--shutdown--001"
    hmi_section = f"{hmi_doc_id}--shutdown--001"

    _write_section(
        data_dir,
        power_doc_id,
        section_id=shutdown_section,
        title="Shut-down Input",
        section_type="shutdown",
        page_range=(8, 8),
    )
    _write_section(
        data_dir,
        hmi_doc_id,
        section_id=hmi_section,
        title="Shutdown buttons",
        section_type="shutdown",
        page_range=(182, 182),
    )
    _write_record(
        data_dir,
        power_doc_id,
        "procedure",
        f"{shutdown_section}--procedure--001",
        {
            **_base_record(power_doc_id, heading="Shut-down Input", start_page=8, end_page=8, confidence=0.96),
            "extraction_version": "extraction/procedure@v1:gpt-4o-mini",
            "title": "Shut-down Input Procedure",
            "steps": [
                {
                    **_base_record(power_doc_id, heading="Shut-down Input", start_page=8, end_page=8, confidence=0.96),
                    "step_number": 1,
                    "instruction": "Connect the signal switch or external voltage to shut down the power supply.",
                    "note": None,
                    "caution": None,
                    "figure_ref": None,
                }
            ],
            "applicability": None,
            "warnings": [],
            "tools_required": [],
        },
    )
    _write_record(
        data_dir,
        hmi_doc_id,
        "procedure",
        f"{hmi_section}--procedure--001",
        {
            **_base_record(hmi_doc_id, heading="Shutdown buttons", start_page=182, end_page=182, confidence=0.98),
            "extraction_version": "extraction/procedure@v1:gpt-4o-mini",
            "title": "Use Security Codes to Control Shutdown Button Visibility",
            "steps": [
                {
                    **_base_record(
                        hmi_doc_id,
                        heading="Shutdown buttons",
                        start_page=182,
                        end_page=182,
                        confidence=0.98,
                    ),
                    "step_number": 1,
                    "instruction": (
                        "Assign security codes so only authorized users can see the shutdown "
                        "button in the runtime display."
                    ),
                    "note": None,
                    "caution": None,
                    "figure_ref": None,
                }
            ],
            "applicability": None,
            "warnings": [],
            "tools_required": [],
        },
    )

    page = compile_topic_page(
        "honeywell/dc1000/family",
        "shutdown_procedure",
        client=_FakeCompileClient(""),
        data_dir=data_dir,
    )
    rendered = page.render()

    assert "Connect the signal switch or external voltage to shut down the power supply." in rendered
    assert "shutdown button" not in rendered
    assert "runtime display" not in rendered


def test_compile_bucket_topic_pages_skips_mixed_family_specifications_without_coherent_scope(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    compact_doc_id = _register_extracted_fixture(_write_pdf(tmp_path / "compact.pdf"), data_dir)
    control_doc_id = _register_extracted_fixture(_write_pdf(tmp_path / "control.pdf"), data_dir, revision="Rev 4")

    control_manifest = load_manifest(data_dir, control_doc_id)
    control_path = data_dir / "manifests" / f"{control_doc_id}.yaml"
    control_path.write_text(
        control_manifest.model_copy(
            update={
                "document": control_manifest.document.model_copy(
                    update={"family": "ControlLogix", "model_applicability": ["1756-L81E"]}
                )
            }
        ).to_yaml(),
        encoding="utf-8",
    )
    compact_manifest = load_manifest(data_dir, compact_doc_id)
    compact_path = data_dir / "manifests" / f"{compact_doc_id}.yaml"
    compact_path.write_text(
        compact_manifest.model_copy(
            update={
                "document": compact_manifest.document.model_copy(
                    update={"family": "CompactLogix", "model_applicability": ["1769-L30ER"]}
                )
            }
        ).to_yaml(),
        encoding="utf-8",
    )

    compact_section = f"{compact_doc_id}--technical-data--001"
    control_section = f"{control_doc_id}--technical-data--001"
    _write_section(
        data_dir,
        compact_doc_id,
        section_id=compact_section,
        title="Technical Data",
        section_type="specifications",
        page_range=(10, 10),
    )
    _write_section(
        data_dir,
        control_doc_id,
        section_id=control_section,
        title="Technical Data",
        section_type="specifications",
        page_range=(20, 20),
    )
    _write_record(
        data_dir,
        compact_doc_id,
        "spec_value",
        f"{compact_section}--spec_value--001",
        {
            **_base_record(compact_doc_id, heading="Technical Data", start_page=10, end_page=10, confidence=0.96),
            "extraction_version": "extraction/spec_value@v1:gpt-4o-mini",
            "parameter": "Supply voltage",
            "value": "24",
            "unit": "VDC",
            "conditions": None,
            "applicability": {
                **_base_record(compact_doc_id, heading="Technical Data", start_page=10, end_page=10, confidence=0.96),
                "manufacturer": "Honeywell",
                "family": "CompactLogix",
                "models": ["1769-L30ER"],
                "serial_range": None,
                "revision": "Rev 3",
            },
        },
    )
    _write_record(
        data_dir,
        control_doc_id,
        "spec_value",
        f"{control_section}--spec_value--001",
        {
            **_base_record(control_doc_id, heading="Technical Data", start_page=20, end_page=20, confidence=0.95),
            "extraction_version": "extraction/spec_value@v1:gpt-4o-mini",
            "parameter": "Backplane current",
            "value": "5",
            "unit": "A",
            "conditions": None,
            "applicability": {
                **_base_record(control_doc_id, heading="Technical Data", start_page=20, end_page=20, confidence=0.95),
                "manufacturer": "Honeywell",
                "family": "ControlLogix",
                "models": ["1756-L81E"],
                "serial_range": None,
                "revision": "Rev 3",
            },
        },
    )

    pages = compile_bucket_topic_pages(
        "honeywell/dc1000/family",
        client=_FakeCompileClient(""),
        data_dir=data_dir,
    )

    topics = {page.frontmatter["topic"] for page in pages if "topic" in page.frontmatter}
    assert "specifications" not in topics


def test_render_contradiction_notes_generates_bucket_summary_page_and_cli(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    service_doc_id = _register_extracted_fixture(
        _write_pdf(tmp_path / "manual-service.pdf"), data_dir, revision="Rev 3"
    )
    sop_doc_id = _register_extracted_fixture(
        _write_pdf(tmp_path / "manual-sop.pdf"),
        data_dir,
        document_type="SOP",
        document_class="operational",
        revision="Rev 3",
    )
    service_section = f"{service_doc_id}--startup--001"
    sop_section = f"{sop_doc_id}--startup--001"
    for doc_id, section_id, page_range in (
        (service_doc_id, service_section, (18, 20)),
        (sop_doc_id, sop_section, (21, 22)),
    ):
        _write_section(
            data_dir,
            doc_id,
            section_id=section_id,
            title="Startup Procedure",
            section_type="startup",
            page_range=page_range,
        )
    _write_record(
        data_dir,
        service_doc_id,
        "procedure",
        f"{service_section}--procedure--001",
        {
            **_base_record(
                service_doc_id,
                heading="Startup Procedure",
                start_page=18,
                end_page=20,
                confidence=0.9,
            ),
            "title": "Start the controller",
            "steps": [
                {
                    **_base_record(
                        service_doc_id,
                        heading="Startup Procedure",
                        start_page=18,
                        end_page=18,
                        confidence=0.9,
                    ),
                    "step_number": 1,
                    "instruction": "Open the discharge valve before energizing the unit.",
                    "note": None,
                    "caution": None,
                    "figure_ref": None,
                }
            ],
            "applicability": None,
            "warnings": [],
            "tools_required": [],
        },
    )
    _write_record(
        data_dir,
        sop_doc_id,
        "procedure",
        f"{sop_section}--procedure--001",
        {
            **_base_record(
                sop_doc_id,
                heading="Startup Procedure",
                start_page=21,
                end_page=22,
                confidence=0.92,
            ),
            "title": "Start the controller",
            "steps": [
                {
                    **_base_record(
                        sop_doc_id,
                        heading="Startup Procedure",
                        start_page=21,
                        end_page=21,
                        confidence=0.92,
                    ),
                    "step_number": 1,
                    "instruction": "Do not open the discharge valve before energizing the unit.",
                    "note": None,
                    "caution": None,
                    "figure_ref": None,
                }
            ],
            "applicability": None,
            "warnings": [],
            "tools_required": [],
        },
    )

    pages = render_contradiction_notes("honeywell/dc1000/family", data_dir=data_dir)
    assert len(pages) == 1
    page = pages[0]
    rendered = page.render()

    assert page.output_path == data_dir / "compiled" / "contradiction-notes" / "honeywell-dc1000-family.md"
    assert "title: 'Contradiction Notes: honeywell/dc1000/family'" in rendered
    assert "## Summary" in rendered
    assert "## Field Guidance" in rendered
    assert "### Candidate 1: Start the controller step 1" in rendered
    assert 'section "Startup Procedure", p.18, revised manual, level 2' in rendered
    assert 'section "Startup Procedure", p.21, internal SOP or best practice, level 5' in rendered
    assert "Retained guidance: Prefer `honeywell-dc1000-service-manual-rev-3`" in rendered
    assert "Human review still needed:" in rendered
    assert 'locator: section "Startup Procedure" (p.18)' in rendered
    assert "analysis_version: contradiction-analysis@v1" in rendered
    assert "contradiction-analysis@v1" in rendered  # always in extraction_version

    runner = CliRunner()
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)}
    result = runner.invoke(cli, ["compile", "contradiction-notes", "honeywell/dc1000/family"], env=env)
    assert result.exit_code == 0
    assert "Compiled 1 contradiction note page(s) for honeywell/dc1000/family" in result.output


def test_compile_bucket_topic_pages_and_cli_support_single_bucket_and_all(
    monkeypatch,
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    first_doc_id = _register_extracted_fixture(_write_pdf(tmp_path / "manual-1.pdf"), data_dir)
    second_doc_id = _register_extracted_fixture(_write_pdf(tmp_path / "manual-2.pdf"), data_dir, revision="Rev 4")
    for doc_id, section_type, title, record_type in (
        (first_doc_id, "startup", "Startup Procedure", "procedure"),
        (second_doc_id, "troubleshooting", "Troubleshooting", "troubleshooting_entry"),
    ):
        section_id = f"{doc_id}--{section_type}--001"
        _write_section(
            data_dir,
            doc_id,
            section_id=section_id,
            title=title,
            section_type=section_type,
            page_range=(1, 2),
        )
        if record_type == "procedure":
            payload = {
                **_base_record(doc_id, heading=title, start_page=1, end_page=2, confidence=0.9),
                "title": "Start the controller",
                "steps": [
                    {
                        **_base_record(doc_id, heading=title, start_page=1, end_page=1, confidence=0.9),
                        "step_number": 1,
                        "instruction": "Apply control power.",
                        "note": None,
                        "caution": None,
                        "figure_ref": None,
                    }
                ],
                "applicability": None,
                "warnings": [],
                "tools_required": [],
            }
        else:
            payload = {
                **_base_record(doc_id, heading=title, start_page=1, end_page=2, confidence=0.91),
                "symptom": "Unit will not start",
                "possible_causes": ["Missing line voltage"],
                "remedies": ["Restore line voltage"],
            }
        _write_record(data_dir, doc_id, record_type, f"{section_id}--{record_type}--001", payload)

    pages = compile_bucket_topic_pages(
        "honeywell/dc1000/family",
        client=_FakeCompileClient("- Cited summary [Source: test, p.1]"),
        data_dir=data_dir,
    )
    topics = {page.frontmatter["topic"] for page in pages}
    assert topics == {"startup_procedure", "troubleshooting"}

    class _CliConfig:
        compilation_model = "gpt-4o-mini"

    monkeypatch.setattr("knowledge_forge.cli.InferenceConfig.load", lambda _=None: _CliConfig())
    monkeypatch.setattr("knowledge_forge.cli.InferenceClient", lambda config, data_dir=None: _FakeCompileClient(""))

    runner = CliRunner()
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir), "OPENAI_API_KEY": "test-secret"}

    single = runner.invoke(
        cli,
        ["compile", "topic-pages", "honeywell/dc1000/family"],
        env=env,
    )
    assert single.exit_code == 0
    assert "Compiled 2 topic page(s) for honeywell/dc1000/family" in single.output
    assert "startup_procedure" in single.output
    assert "troubleshooting" in single.output

    every = runner.invoke(
        cli,
        ["compile", "topic-pages", "--all"],
        env=env,
    )
    assert every.exit_code == 0
    assert "Compiled 2 topic page(s)." in every.output
    assert "honeywell/dc1000/family" in every.output


def test_compile_family_overview_and_manufacturer_index_cover_mixed_document_types(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    manual_doc_id = _register_extracted_fixture(_write_pdf(tmp_path / "manual.pdf"), data_dir)
    sop_doc_id = _register_extracted_fixture(
        _write_pdf(tmp_path / "sop.pdf"),
        data_dir,
        revision="Rev SOP",
        document_type="SOP",
        document_class="operational",
    )
    topic_dir = data_dir / "compiled" / "topic-pages" / "honeywell-dc1000-family"
    topic_dir.mkdir(parents=True, exist_ok=True)
    (topic_dir / "startup_procedure.md").write_text("# Startup\n", encoding="utf-8")
    (topic_dir / "troubleshooting.md").write_text("# Troubleshooting\n", encoding="utf-8")
    _ensure_extracted_doc_dir(data_dir, manual_doc_id)
    _ensure_extracted_doc_dir(data_dir, sop_doc_id)

    family_page = compile_family_overview("honeywell/dc1000/family", data_dir=data_dir)
    manufacturer_page = compile_manufacturer_index("Honeywell", data_dir=data_dir)
    every_page = compile_all_overviews(data_dir=data_dir)

    family_rendered = family_page.render()
    manufacturer_rendered = manufacturer_page.render()

    assert (
        family_page.output_path
        == data_dir / "compiled" / "overview-pages" / "manufacturers" / "honeywell" / "dc1000" / "_index.md"
    )
    assert "title: Honeywell DC1000 Family Overview" in family_rendered
    assert "- Models covered: DC1000, DC1200" in family_rendered
    assert "- `SOP`: 1" in family_rendered
    assert "- `Service Manual`: 1" in family_rendered
    assert "## Quality Summary" in family_rendered
    assert "- `authoritative-technical`: 1" in family_rendered
    assert "- `operational`: 1" in family_rendered
    assert "[Startup Procedure](../../../topic-pages/honeywell-dc1000-family/startup_procedure.md)" in family_rendered

    assert (
        manufacturer_page.output_path
        == data_dir / "compiled" / "overview-pages" / "manufacturers" / "honeywell" / "_index.md"
    )
    assert "title: Honeywell Manufacturer Index" in manufacturer_rendered
    assert "[DC1000](dc1000/_index.md)" in manufacturer_rendered
    assert "topics: Startup Procedure, Troubleshooting" in manufacturer_rendered

    assert {page.frontmatter["page_type"] for page in every_page} == {"family_overview", "manufacturer_index"}
    assert {entry["doc_id"] for entry in family_page.frontmatter["source_documents"]} == {manual_doc_id, sop_doc_id}


def test_compile_family_overview_uses_curated_bucket_label_when_present(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_extracted_fixture(_write_pdf(tmp_path / "manual.pdf"), data_dir)
    manifest_path = data_dir / "manifests" / f"{doc_id}.yaml"
    manifest = load_manifest(data_dir, doc_id)
    manifest_path.write_text(
        manifest.model_copy(
            update={
                "bucket_assignments": [
                    BucketAssignment(
                        doc_id=doc_id,
                        bucket_id="honeywell/pump-station-control-stack/curated-bucket",
                        dimension="curated_bucket",
                        value="Pump Station Control Stack",
                    )
                ]
            }
        ).to_yaml(),
        encoding="utf-8",
    )
    topic_dir = data_dir / "compiled" / "topic-pages" / "honeywell-pump-station-control-stack-curated-bucket"
    topic_dir.mkdir(parents=True, exist_ok=True)
    (topic_dir / "startup_procedure.md").write_text("# Startup\n", encoding="utf-8")
    _ensure_extracted_doc_dir(data_dir, doc_id)

    family_page = compile_family_overview("honeywell/pump-station-control-stack/curated-bucket", data_dir=data_dir)

    assert "title: Honeywell Pump Station Control Stack Family Overview" in family_page.render()
    assert family_page.output_path == (
        data_dir
        / "compiled"
        / "overview-pages"
        / "manufacturers"
        / "honeywell"
        / "pump-station-control-stack"
        / "_index.md"
    )


def test_compile_overviews_cli_supports_family_bucket_manufacturer_and_all(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_extracted_fixture(_write_pdf(tmp_path / "manual.pdf"), data_dir)
    topic_dir = data_dir / "compiled" / "topic-pages" / "honeywell-dc1000-family"
    topic_dir.mkdir(parents=True, exist_ok=True)
    (topic_dir / "startup_procedure.md").write_text("# Startup\n", encoding="utf-8")
    _ensure_extracted_doc_dir(data_dir, doc_id)

    runner = CliRunner()
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)}

    family = runner.invoke(cli, ["compile", "overviews", "honeywell/dc1000/family"], env=env)
    assert family.exit_code == 0
    assert "Compiled family overview for honeywell/dc1000/family" in family.output

    manufacturer = runner.invoke(cli, ["compile", "overviews", "--manufacturer", "Honeywell"], env=env)
    assert manufacturer.exit_code == 0
    assert "Compiled manufacturer index for Honeywell" in manufacturer.output

    every = runner.invoke(cli, ["compile", "overviews", "--all"], env=env)
    assert every.exit_code == 0
    assert "Compiled 2 overview page(s)." in every.output
    assert "family_overview" in every.output
    assert "manufacturer_index" in every.output
