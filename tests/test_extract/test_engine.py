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
from knowledge_forge.intake.importer import RegistrationRequest, load_manifest, register_document
from knowledge_forge.intake.manifest import DocumentStatus
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


class _FakeClient:
    def __init__(self, responses: dict[str, list[dict[str, object]]]) -> None:
        self.responses = responses
        self.calls: list[dict[str, object]] = []
        self.config = _FakeConfig()

    def complete(
        self,
        prompt: str,
        system: str,
        model: str | None = None,
        schema: dict[str, object] | None = None,
        **kwargs: object,
    ):
        record_type = str(kwargs["prompt_template"]).split("/")[-1]
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
                "parsed_json": {"records": self.responses[record_type]},
            },
        )()


def test_extract_section_builds_prompt_parses_records_and_writes_files(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    section = Section(
        doc_id="honeywell-dc1000-service-manual-rev3",
        section_id="honeywell-dc1000-service-manual-rev3--startup--001",
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
    assert json.loads(procedure_path.read_text(encoding="utf-8"))["title"] == "Start the controller"
    assert json.loads(warning_path.read_text(encoding="utf-8"))["severity"] == "warning"


def test_extract_document_loads_sections_and_marks_manifest_extracted(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_parsed_fixture(_write_pdf(tmp_path / "manual.pdf"), data_dir)
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


def test_section_type_mapping_covers_all_canonical_section_types() -> None:
    assert set(SECTION_RECORD_TYPE_MAP) == {
        "safety",
        "installation",
        "configuration",
        "startup",
        "shutdown",
        "maintenance",
        "troubleshooting",
        "specifications",
        "parts",
        "revision_notes",
        "other",
    }
    assert SECTION_RECORD_TYPE_MAP["maintenance"] == ["procedure", "warning"]
    assert SECTION_RECORD_TYPE_MAP["troubleshooting"] == ["troubleshooting_entry", "alarm_definition"]


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

    def fake_extract_document(
        doc_id: str,
        *,
        section_id: str | None = None,
        config: object | None = None,
        data_dir: Path | None = None,
    ):
        calls.append({"doc_id": doc_id, "section_id": section_id, "config": config, "data_dir": data_dir})
        return [object(), object()]

    monkeypatch.setattr("knowledge_forge.cli.extract_document", fake_extract_document)
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)}

    single = runner.invoke(cli, ["extract", "doc-001", "--config", str(config_path)], env=env)
    assert single.exit_code == 0
    assert "Extracted 2 record(s) for doc-001" in single.output

    one_section = runner.invoke(
        cli,
        ["extract", "doc-001", "--section", "doc-001--startup--001", "--config", str(config_path)],
        env=env,
    )
    assert one_section.exit_code == 0
    assert "Section: doc-001--startup--001" in one_section.output
    assert calls == [
        {"doc_id": "doc-001", "section_id": None, "config": config_sentinel, "data_dir": data_dir},
        {
            "doc_id": "doc-001",
            "section_id": "doc-001--startup--001",
            "config": config_sentinel,
            "data_dir": data_dir,
        },
    ]
