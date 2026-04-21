"""Tests for the section-to-record extraction engine."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from knowledge_forge.cli import cli
from knowledge_forge.extract.engine import (
    SECTION_RECORD_TYPE_MAP,
    build_record_id,
    extract_document,
    extract_section,
    load_prompt_template,
)
from knowledge_forge.extract.reviewability import assess_section_reviewability
from knowledge_forge.intake.importer import RegistrationRequest, load_manifest, register_document
from knowledge_forge.intake.manifest import BucketAssignment, DocumentStatus
from knowledge_forge.parse.sectioning import Section


def _register_parsed_fixture(pdf_path: Path, data_dir: Path) -> str:
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
    manifest_path.write_text(manifest.to_yaml(), encoding="utf-8")
    return result.manifest.doc_id


def _write_pdf(path: Path) -> Path:
    path.write_bytes(b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\ntrailer\n<<>>\n%%EOF\n")
    return path


def _write_section(path: Path, section: Section) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(section.model_dump_json(indent=2), encoding="utf-8")


def _write_parse_meta(data_dir: Path, doc_id: str, *, parser_version: str = "docling-9.9.9") -> None:
    meta_path = data_dir / "parsed" / doc_id / "meta.json"
    meta_path.parent.mkdir(parents=True, exist_ok=True)
    meta_path.write_text(
        json.dumps(
            {
                "doc_id": doc_id,
                "parser": "docling",
                "parser_version": parser_version,
                "processed_at": "2026-04-16T00:00:00Z",
                "processing_time_seconds": 1.2,
                "page_count": 99,
                "status": "success",
                "input_path": str(data_dir / "normalized" / f"{doc_id}.pdf"),
                "input_checksum": "a" * 64,
                "document_hash": None,
                "timings": {},
                "confidence": None,
                "errors": [],
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_bucket_assignments(data_dir: Path, doc_id: str) -> None:
    manifest_path = data_dir / "manifests" / f"{doc_id}.yaml"
    manifest = load_manifest(data_dir, doc_id)
    updated_manifest = manifest.model_copy(
        update={
            "bucket_assignments": [
                BucketAssignment(
                    doc_id=doc_id,
                    bucket_id="honeywell/dc1000/family",
                    dimension="family",
                    value="DC1000",
                )
            ]
        }
    )
    manifest_path.write_text(updated_manifest.to_yaml(), encoding="utf-8")


def _base_record() -> dict[str, object]:
    return {
        "source_doc_id": "honeywell-dc1000-service-manual-rev3",
        "source_page_range": {"start_page": 18, "end_page": 20},
        "source_heading": "Startup Procedure",
        "parser_version": "docling-1.2.0",
        "extraction_version": "f2",
        "confidence": 0.94,
        "bucket_context": [
            {"bucket_id": "honeywell/dc1000/family", "dimension": "family", "value": "DC1000"},
        ],
    }


class _FakeConfig:
    extraction_model: str = "gpt-4o-mini"
    max_tokens: int = 4096


class _FakeClient:
    def __init__(
        self,
        responses: dict[str, list[dict[str, object]] | Exception],
        *,
        output_tokens: int = 32,
    ) -> None:
        self.responses = responses
        self.calls: list[dict[str, object]] = []
        self.config = _FakeConfig()
        self.output_tokens = output_tokens

    def complete(
        self,
        prompt: str,
        system: str,
        model: str | None = None,
        schema: dict[str, object] | None = None,
        **kwargs: object,
    ):
        parts = str(kwargs["prompt_template"]).split("/")
        # Map repair templates like "extraction/spec_value/reprompt" back to "spec_value"
        if len(parts) > 1 and parts[-1] in ("reprompt", "relaxed"):
            record_type = parts[-2]
        else:
            record_type = parts[-1]
        self.calls.append(
            {
                "prompt": prompt,
                "system": system,
                "model": model,
                "schema": schema,
                **kwargs,
            }
        )
        response = self.responses[record_type]
        if isinstance(response, Exception):
            raise response
        return type(
            "FakeResult",
            (),
            {
                "parsed_json": {"records": response},
                "input_tokens": 128,
                "output_tokens": self.output_tokens,
            },
        )()


class _SequentialFakeClient:
    """Fake client that returns responses in call order, popping from a list."""

    def __init__(self, responses: list[object], *, output_tokens: int = 32) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, object]] = []
        self.config = _FakeConfig()
        self.output_tokens = output_tokens

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
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return type(
            "FakeResult",
            (),
            {
                "parsed_json": {"records": response},
                "input_tokens": 128,
                "output_tokens": self.output_tokens,
            },
        )()


def test_extract_section_builds_prompt_parses_records_and_writes_files(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_parsed_fixture(_write_pdf(tmp_path / "manual.pdf"), data_dir)
    _write_parse_meta(data_dir, doc_id)
    _write_bucket_assignments(data_dir, doc_id)
    section = Section(
        doc_id=doc_id,
        section_id=f"{doc_id}--startup--001",
        section_type="startup",
        title="Startup Procedure",
        content="1. Verify the discharge valve is open.\n2. Apply control power.",
        page_range=(18, 20),
        heading_path=["DC1000 Service Manual", "Startup Procedure"],
    )
    client = _FakeClient(
        {
            "procedure": [
                {
                    **_base_record(),
                    "title": "Start the controller",
                    "steps": [
                        {
                            **_base_record(),
                            "source_page_range": {"start_page": 18, "end_page": 18},
                            "step_number": 1,
                            "instruction": "Verify the discharge valve is open.",
                            "note": "Use local lockout procedure before inspection.",
                            "caution": None,
                            "figure_ref": None,
                        }
                    ],
                    "applicability": None,
                    "warnings": [],
                    "tools_required": ["multimeter"],
                }
            ],
            "warning": [
                {
                    **_base_record(),
                    "severity": "warning",
                    "text": "Do not energize the motor with the casing dry.",
                    "context": "Startup",
                    "applicability": None,
                }
            ],
        }
    )

    records = extract_section(section, client=client, data_dir=data_dir)

    assert [type(record).__name__ for record in records] == ["Procedure", "Warning"]
    assert len(client.calls) == 2
    assert "Startup Procedure" in str(client.calls[0]["prompt"])
    assert "Apply control power." in str(client.calls[0]["prompt"])
    assert client.calls[0]["prompt_template"] == "extraction/procedure"
    assert client.calls[0]["source_section_id"] == section.section_id
    assert client.calls[0]["schema"]["properties"]["records"]["type"] == "array"

    procedure_path = (
        data_dir
        / "extracted"
        / section.doc_id
        / "procedure"
        / f"{build_record_id(section.section_id, 'procedure', 1)}.json"
    )
    warning_path = (
        data_dir
        / "extracted"
        / section.doc_id
        / "warning"
        / f"{build_record_id(section.section_id, 'warning', 1)}.json"
    )
    assert procedure_path.exists()
    assert warning_path.exists()
    procedure_payload = json.loads(procedure_path.read_text(encoding="utf-8"))
    warning_payload = json.loads(warning_path.read_text(encoding="utf-8"))
    assert procedure_payload["title"] == "Start the controller"
    assert warning_payload["severity"] == "warning"
    assert procedure_payload["parser_version"] == "docling-9.9.9"
    assert procedure_payload["extraction_version"] == "extraction/procedure@v1:gpt-4o-mini"
    assert procedure_payload["source_doc_id"] == doc_id
    assert procedure_payload["steps"][0]["parser_version"] == "docling-9.9.9"
    assert warning_payload["bucket_context"][0]["bucket_id"] == "honeywell/dc1000/family"


def test_extract_document_loads_sections_and_marks_manifest_extracted(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_parsed_fixture(_write_pdf(tmp_path / "manual.pdf"), data_dir)
    _write_parse_meta(data_dir, doc_id, parser_version="docling-1.2.0")
    _write_bucket_assignments(data_dir, doc_id)
    section = Section(
        doc_id=doc_id,
        section_id=f"{doc_id}--specifications--001",
        section_type="specifications",
        title="Electrical Specifications",
        content="Supply voltage: 24 VDC",
        page_range=(42, 42),
        heading_path=["Electrical Specifications"],
    )
    _write_section(data_dir / "sections" / doc_id / f"{section.section_id}.json", section)
    client = _FakeClient(
        {
            "spec_value": [
                {
                    **_base_record(),
                    "source_doc_id": doc_id,
                    "source_heading": "Electrical Specifications",
                    "parameter": "Supply voltage",
                    "value": "24",
                    "unit": "VDC",
                    "conditions": "Nominal input",
                    "applicability": None,
                }
            ]
        }
    )

    records = extract_document(doc_id, client=client, data_dir=data_dir)

    assert len(records) == 1
    manifest = load_manifest(data_dir, doc_id)
    assert manifest.document.status == DocumentStatus.EXTRACTED
    record_path = data_dir / "extracted" / doc_id / "spec_value" / f"{section.section_id}--spec_value--001.json"
    payload = json.loads(record_path.read_text(encoding="utf-8"))
    assert payload["parameter"] == "Supply voltage"
    assert payload["parser_version"] == "docling-1.2.0"
    assert payload["bucket_context"][0]["value"] == "DC1000"


@pytest.mark.parametrize(
    ("title", "reason_code"),
    [
        ("continued", "generic_carryover_title"),
        ("D", "single_letter_title"),
        ("2.", "numeric_fragment_title"),
        (
            "B-1 local I/O backplane memory use 3-4 CompactBus 3-5 configuring 3-7 generic profile 3-17 overview 3-1",
            "toc_fragment_title",
        ),
    ],
)
def test_assess_section_reviewability_flags_rockwell_like_malformed_titles(
    title: str,
    reason_code: str,
) -> None:
    assessment = assess_section_reviewability(
        Section(
            doc_id="rockwell-doc",
            section_id="rockwell-doc--section--001",
            section_type="other",
            title=title,
            content="Placeholder content",
            page_range=(1, 1),
            heading_path=[title],
        )
    )

    assert assessment.reviewable is False
    assert reason_code in assessment.reason_codes


def test_extract_section_skips_non_reviewable_titles_before_inference(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_parsed_fixture(_write_pdf(tmp_path / "manual.pdf"), data_dir)
    _write_parse_meta(data_dir, doc_id)
    _write_bucket_assignments(data_dir, doc_id)
    section = Section(
        doc_id=doc_id,
        section_id=f"{doc_id}--continued--001",
        section_type="troubleshooting",
        title="continued",
        content="Carryover heading with low-value spillover content.",
        page_range=(18, 18),
        heading_path=["Troubleshooting", "continued"],
    )
    client = _FakeClient({"troubleshooting_entry": [], "alarm_definition": []})

    records = extract_section(section, client=client, data_dir=data_dir)

    assert records == []
    assert client.calls == []
    troubleshooting_flag = (
        data_dir / "extracted" / doc_id / "reviews" / f"{section.section_id}--troubleshooting_entry.json"
    )
    alarm_flag = data_dir / "extracted" / doc_id / "reviews" / f"{section.section_id}--alarm_definition.json"
    assert troubleshooting_flag.exists()
    assert alarm_flag.exists()
    payload = json.loads(troubleshooting_flag.read_text(encoding="utf-8"))
    assert payload["reasons"] == ["section_not_reviewable", "generic_carryover_title"]
    assert "generic carryover heading" in payload["errors"][0]


def test_section_type_mapping_covers_all_canonical_section_types() -> None:
    assert set(SECTION_RECORD_TYPE_MAP) == {
        "addendum",
        "bulletin",
        "checklist",
        "commissioning",
        "configuration",
        "diagram",
        "drawing",
        "inspection",
        "installation",
        "maintenance",
        "other",
        "parts",
        "revision_notes",
        "safety",
        "seasonal-procedure",
        "shutdown",
        "sop",
        "specifications",
        "startup",
        "troubleshooting",
        "wiring",
        "workflow",
    }
    assert SECTION_RECORD_TYPE_MAP["maintenance"] == ["procedure", "warning"]
    assert SECTION_RECORD_TYPE_MAP["troubleshooting"] == ["troubleshooting_entry", "alarm_definition"]
    assert SECTION_RECORD_TYPE_MAP["checklist"] == ["procedure", "warning"]
    assert SECTION_RECORD_TYPE_MAP["wiring"] == ["applicability"]


def test_load_prompt_template_reads_yaml_template() -> None:
    template = load_prompt_template("procedure")

    assert "Return only JSON" in template.system
    assert template.schema_ref == "procedure"
    assert "{section_content}" in template.user


def test_load_prompt_template_raises_for_non_mapping_yaml(tmp_path: Path) -> None:
    bad = tmp_path / "bad_type.yaml"
    bad.write_text("- item1\n- item2\n", encoding="utf-8")

    with pytest.raises(ValueError, match="not a YAML mapping"):
        load_prompt_template("bad_type", base_dir=tmp_path)


def test_load_prompt_template_raises_for_missing_required_keys(tmp_path: Path) -> None:
    partial = tmp_path / "partial.yaml"
    partial.write_text("system: hello\n", encoding="utf-8")

    with pytest.raises(ValueError, match="missing required keys"):
        load_prompt_template("partial", base_dir=tmp_path)


def test_extract_cli_supports_document_and_single_section(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    config_path = tmp_path / "inference.yaml"
    config_path.write_text("openai:\n  api_key_env: OPENAI_API_KEY\n", encoding="utf-8")
    runner = CliRunner()
    calls: list[dict[str, object]] = []
    config_sentinel = object()

    monkeypatch.setattr("knowledge_forge.cli.InferenceConfig.load", lambda _: config_sentinel)

    def fake_start_extraction_run(
        doc_ids: list[str],
        *,
        config: object | None = None,
        data_dir: Path | None = None,
        section_ids: list[str] | None = None,
        min_confidence: float = 0.0,
        max_repair_attempts: int = 2,
    ):
        calls.append(
            {
                "doc_ids": doc_ids,
                "section_ids": section_ids,
                "config": config,
                "data_dir": data_dir,
                "min_confidence": min_confidence,
                "max_repair_attempts": max_repair_attempts,
            }
        )
        return type(
            "Execution",
            (),
            {
                "run": type(
                    "Run",
                    (),
                    {
                        "run_id": "er-20260417-001",
                        "status": type("Status", (), {"value": "completed"})(),
                    },
                )(),
                "run_path": data_dir / "extraction_runs" / "er-20260417-001.json",
                "records_emitted": 2,
            },
        )()

    monkeypatch.setattr("knowledge_forge.cli.start_extraction_run", fake_start_extraction_run)
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)}

    single = runner.invoke(
        cli,
        ["extract", "doc-001", "--min-confidence", "0.8", "--max-repair-attempts", "1", "--config", str(config_path)],
        env=env,
    )
    assert single.exit_code == 0
    assert "Extracted 2 record(s) for doc-001" in single.output
    assert "Run: er-20260417-001" in single.output

    one_section = runner.invoke(
        cli,
        ["extract", "doc-001", "--section", "doc-001--startup--001", "--config", str(config_path)],
        env=env,
    )
    assert one_section.exit_code == 0
    assert "Section: doc-001--startup--001" in one_section.output
    assert calls == [
        {
            "doc_ids": ["doc-001"],
            "section_ids": None,
            "config": config_sentinel,
            "data_dir": data_dir,
            "min_confidence": 0.8,
            "max_repair_attempts": 1,
        },
        {
            "doc_ids": ["doc-001"],
            "section_ids": ["doc-001--startup--001"],
            "config": config_sentinel,
            "data_dir": data_dir,
            "min_confidence": 0.0,
            "max_repair_attempts": 2,
        },
    ]


def test_extract_section_flags_low_confidence_records_for_review(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_parsed_fixture(_write_pdf(tmp_path / "manual.pdf"), data_dir)
    _write_parse_meta(data_dir, doc_id)
    _write_bucket_assignments(data_dir, doc_id)
    section = Section(
        doc_id=doc_id,
        section_id=f"{doc_id}--startup--001",
        section_type="startup",
        title="Startup Procedure",
        content="1. Verify the discharge valve is open.\n2. Apply control power.",
        page_range=(18, 20),
        heading_path=["DC1000 Service Manual", "Startup Procedure"],
    )
    client = _FakeClient(
        {
            "procedure": [
                {
                    **_base_record(),
                    "title": "Start the controller",
                    "steps": [
                        {
                            **_base_record(),
                            "source_page_range": {"start_page": 18, "end_page": 18},
                            "step_number": 1,
                            "instruction": "Verify the discharge valve is open.",
                            "note": None,
                            "caution": None,
                            "figure_ref": None,
                        }
                    ],
                    "applicability": None,
                    "warnings": [],
                    "tools_required": [],
                }
            ],
            "warning": [
                {
                    **_base_record(),
                    "severity": "warning",
                    "text": "Do not energize the motor with the casing dry.",
                    "context": "Startup",
                    "applicability": None,
                }
            ],
        },
        output_tokens=4096,
    )

    records = extract_section(section, client=client, data_dir=data_dir, min_confidence=0.8)

    assert len(records) == 2
    review_path = data_dir / "extracted" / section.doc_id / "reviews" / f"{section.section_id}--procedure.json"
    assert review_path.exists()
    payload = json.loads(review_path.read_text(encoding="utf-8"))
    assert payload["reasons"] == ["below_min_confidence"]


def test_extract_section_persists_procedure_when_tools_required_is_null(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_parsed_fixture(_write_pdf(tmp_path / "manual.pdf"), data_dir)
    _write_parse_meta(data_dir, doc_id)
    _write_bucket_assignments(data_dir, doc_id)
    section = Section(
        doc_id=doc_id,
        section_id=f"{doc_id}--installation--001",
        section_type="installation",
        title="Complete the Physical Connections of the Network",
        content=(
            "After you configure a ring supervisor, complete the physical connection between all nodes. "
            "Do not fully connect the DLR network until a supervisor is configured."
        ),
        page_range=(15, 16),
        heading_path=["Complete the Physical Connections of the Network"],
    )
    client = _FakeClient(
        {
            "procedure": [
                {
                    **_base_record(),
                    "title": "Complete the Physical Connections of the Network",
                    "steps": [
                        {
                            **_base_record(),
                            "source_page_range": {"start_page": 15, "end_page": 16},
                            "step_number": 1,
                            "instruction": "Complete the last physical connection after the supervisor is configured.",
                            "note": None,
                            "caution": None,
                            "figure_ref": None,
                        }
                    ],
                    "applicability": None,
                    "warnings": [],
                    "tools_required": None,
                }
            ],
            "warning": [
                {
                    **_base_record(),
                    "severity": "warning",
                    "text": "Do not fully connect the DLR network until a supervisor is configured.",
                    "context": "Physical network completion",
                    "applicability": None,
                }
            ],
        }
    )

    records = extract_section(section, client=client, data_dir=data_dir)

    assert [type(record).__name__ for record in records] == ["Procedure", "Warning"]
    procedure = next(record for record in records if type(record).__name__ == "Procedure")
    assert procedure.tools_required == []
    procedure_path = (
        data_dir / "extracted" / doc_id / "procedure" / f"{build_record_id(section.section_id, 'procedure', 1)}.json"
    )
    payload = json.loads(procedure_path.read_text(encoding="utf-8"))
    assert payload["tools_required"] == []


def test_extract_section_replaces_stale_record_files_and_review_flags(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_parsed_fixture(_write_pdf(tmp_path / "manual.pdf"), data_dir)
    _write_parse_meta(data_dir, doc_id)
    _write_bucket_assignments(data_dir, doc_id)
    section = Section(
        doc_id=doc_id,
        section_id=f"{doc_id}--specifications--001",
        section_type="specifications",
        title="Electrical Specifications",
        content="Supply voltage: 24 VDC\nCurrent draw: 3 A",
        page_range=(42, 42),
        heading_path=["Electrical Specifications"],
    )
    first_client = _FakeClient(
        {
            "spec_value": [
                {
                    **_base_record(),
                    "source_doc_id": doc_id,
                    "source_heading": "Electrical Specifications",
                    "parameter": "Supply voltage",
                    "value": "24",
                    "unit": "VDC",
                    "conditions": None,
                    "applicability": None,
                },
                {
                    **_base_record(),
                    "source_doc_id": doc_id,
                    "source_heading": "Electrical Specifications",
                    "parameter": "Current draw",
                    "value": "3",
                    "unit": "A",
                    "conditions": None,
                    "applicability": None,
                },
            ]
        },
        output_tokens=4096,
    )

    extract_section(section, client=first_client, data_dir=data_dir, min_confidence=0.8)

    second_client = _FakeClient(
        {
            "spec_value": [
                {
                    **_base_record(),
                    "source_doc_id": doc_id,
                    "source_heading": "Electrical Specifications",
                    "parameter": "Supply voltage",
                    "value": "24",
                    "unit": "VDC",
                    "conditions": "Nominal input",
                    "applicability": None,
                }
            ]
        },
        output_tokens=32,
    )

    records = extract_section(section, client=second_client, data_dir=data_dir, min_confidence=0.8)

    assert len(records) == 1
    kept_path = data_dir / "extracted" / doc_id / "spec_value" / f"{section.section_id}--spec_value--001.json"
    stale_path = data_dir / "extracted" / doc_id / "spec_value" / f"{section.section_id}--spec_value--002.json"
    review_path = data_dir / "extracted" / doc_id / "reviews" / f"{section.section_id}--spec_value.json"
    assert kept_path.exists()
    assert not stale_path.exists()
    assert not review_path.exists()


def test_extract_section_flags_unrepairable_response_for_review(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_parsed_fixture(_write_pdf(tmp_path / "manual.pdf"), data_dir)
    _write_parse_meta(data_dir, doc_id)
    _write_bucket_assignments(data_dir, doc_id)
    section = Section(
        doc_id=doc_id,
        section_id=f"{doc_id}--specifications--001",
        section_type="specifications",
        title="Electrical Specifications",
        content="Supply voltage: 24 VDC",
        page_range=(42, 42),
        heading_path=["Electrical Specifications"],
    )
    client = _FakeClient({"spec_value": ValueError("response did not satisfy schema: $.records: missing title")})

    records = extract_section(section, client=client, data_dir=data_dir, max_repair_attempts=1)

    assert records == []
    review_path = data_dir / "extracted" / section.doc_id / "reviews" / f"{section.section_id}--spec_value.json"
    assert review_path.exists()
    payload = json.loads(review_path.read_text(encoding="utf-8"))
    assert payload["reasons"] == ["repair_failed"]


def test_extract_section_succeeds_after_repair(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_parsed_fixture(_write_pdf(tmp_path / "manual.pdf"), data_dir)
    _write_parse_meta(data_dir, doc_id)
    _write_bucket_assignments(data_dir, doc_id)
    section = Section(
        doc_id=doc_id,
        section_id=f"{doc_id}--specifications--001",
        section_type="specifications",
        title="Electrical Specifications",
        content="Supply voltage: 24 VDC",
        page_range=(42, 42),
        heading_path=["Electrical Specifications"],
    )
    valid_spec = {
        **_base_record(),
        "source_doc_id": doc_id,
        "source_heading": "Electrical Specifications",
        "parameter": "Supply voltage",
        "value": "24",
        "unit": "VDC",
        "conditions": None,
        "applicability": None,
    }
    # First call raises a schema ValueError; second call (reprompt) returns valid data.
    client = _SequentialFakeClient(
        [
            ValueError("response did not satisfy schema: $.records[0].parameter missing"),
            [valid_spec],
        ]
    )

    records = extract_section(section, client=client, data_dir=data_dir, max_repair_attempts=2)

    assert len(records) == 1
    assert records[0].parameter == "Supply voltage"
    assert len(client.calls) == 2
    assert client.calls[1]["prompt_template"] == "extraction/spec_value/reprompt"
    # Repaired record has a confidence penalty; confirm it is below a perfect score.
    assert records[0].confidence < 1.0
