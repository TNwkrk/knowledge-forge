"""Tests for canonical parse sectioning."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from knowledge_forge.cli import cli
from knowledge_forge.intake.importer import RegistrationRequest, register_document
from knowledge_forge.intake.manifest import DocumentStatus
from knowledge_forge.parse import section_document


def _register_parsed_fixture(data_dir: Path, pdf_path: Path, *, revision: str = "Rev 3") -> str:
    request = RegistrationRequest(
        pdf_path=pdf_path,
        manufacturer="Honeywell",
        family="DC1000",
        model_applicability=["DC1000"],
        document_type="Service Manual",
        revision=revision,
        publication_date=None,
        language="en",
        priority=1,
    )
    result = register_document(request, data_dir=data_dir)
    manifest_path = data_dir / "manifests" / f"{result.manifest.doc_id}.yaml"
    manifest = result.manifest.transition_status(DocumentStatus.PARSED, reason="test parsed fixture")
    manifest_path.write_text(manifest.to_yaml(), encoding="utf-8")
    return result.manifest.doc_id


def _write_pdf(path: Path, content: bytes = b"%PDF-1.4\n% test fixture\n") -> Path:
    path.write_bytes(content)
    return path


def _write_parsed_artifacts(
    data_dir: Path,
    *,
    doc_id: str,
    structure: dict[str, object],
    headings: dict[str, object],
) -> None:
    parsed_dir = data_dir / "parsed" / doc_id
    parsed_dir.mkdir(parents=True, exist_ok=True)
    (parsed_dir / "structure.json").write_text(json.dumps(structure, indent=2), encoding="utf-8")
    (parsed_dir / "headings.json").write_text(json.dumps(headings, indent=2), encoding="utf-8")


def test_section_document_splits_nested_headings_and_persists_sections(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_parsed_fixture(data_dir, _write_pdf(tmp_path / "fixture.pdf"))
    structure = {
        "doc_id": doc_id,
        "parser": "docling",
        "parser_version": "test",
        "page_count": 3,
        "texts": [
            {"item_ref": "#/texts/0", "label": "title", "text": "DC1000 Service Manual", "page_numbers": [1]},
            {"item_ref": "#/texts/1", "label": "section_header", "text": "Safety", "page_numbers": [1]},
            {"item_ref": "#/texts/2", "label": "text", "text": "Wear insulated gloves.", "page_numbers": [1]},
            {"item_ref": "#/texts/3", "label": "section_header", "text": "Installation", "page_numbers": [2]},
            {"item_ref": "#/texts/4", "label": "text", "text": "Mount the unit on a flat panel.", "page_numbers": [2]},
            {"item_ref": "#/texts/5", "label": "section_title", "text": "Startup Checklist", "page_numbers": [2]},
            {"item_ref": "#/texts/6", "label": "text", "text": "Power on the main breaker.", "page_numbers": [2]},
            {"item_ref": "#/texts/7", "label": "section_header", "text": "Specifications", "page_numbers": [3]},
        ],
        "tables": [
            {
                "item_ref": "#/tables/0",
                "label": "table",
                "page_numbers": [3],
                "row_count": 2,
                "column_count": 2,
                "data": [["Voltage", "120V"], ["Phase", "1"]],
            }
        ],
        "pages": [
            {"page_number": 1, "width": 612, "height": 792, "source_ref": "p1"},
            {"page_number": 2, "width": 612, "height": 792, "source_ref": "p2"},
            {"page_number": 3, "width": 612, "height": 792, "source_ref": "p3"},
        ],
    }
    headings = {
        "doc_id": doc_id,
        "headings": [
            {
                "title": "DC1000 Service Manual",
                "label": "title",
                "level": 1,
                "page_number": 1,
                "item_ref": "#/texts/0",
                "children": [
                    {
                        "title": "Safety",
                        "label": "section_header",
                        "level": 2,
                        "page_number": 1,
                        "item_ref": "#/texts/1",
                        "children": [],
                    },
                    {
                        "title": "Installation",
                        "label": "section_header",
                        "level": 2,
                        "page_number": 2,
                        "item_ref": "#/texts/3",
                        "children": [
                            {
                                "title": "Startup Checklist",
                                "label": "section_title",
                                "level": 3,
                                "page_number": 2,
                                "item_ref": "#/texts/5",
                                "children": [],
                            }
                        ],
                    },
                    {
                        "title": "Specifications",
                        "label": "section_header",
                        "level": 2,
                        "page_number": 3,
                        "item_ref": "#/texts/7",
                        "children": [],
                    },
                ],
            }
        ],
    }
    _write_parsed_artifacts(data_dir, doc_id=doc_id, structure=structure, headings=headings)

    sections = section_document(doc_id, data_dir=data_dir)

    assert [section.title for section in sections] == [
        "Safety",
        "Installation",
        "Startup Checklist",
        "Specifications",
    ]
    assert [section.section_type for section in sections] == [
        "safety",
        "installation",
        "startup",
        "specifications",
    ]
    assert sections[1].page_range == (2, 2)
    assert sections[2].parent_section_id == sections[1].section_id
    assert sections[3].content.endswith("| Phase | 1 |")

    persisted = sorted((data_dir / "sections" / doc_id).glob("*.json"))
    assert len(persisted) == len(sections)
    payload_path = data_dir / "sections" / doc_id / f"{sections[-1].section_id}.json"
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    assert payload["doc_id"] == doc_id
    assert payload["section_id"] == sections[-1].section_id

    second_run = section_document(doc_id, data_dir=data_dir)
    assert [section.section_id for section in second_run] == [section.section_id for section in sections]


def test_section_document_handles_nonstandard_heading_structures(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_parsed_fixture(data_dir, _write_pdf(tmp_path / "nonstandard.pdf"), revision="Rev 4")
    structure = {
        "doc_id": doc_id,
        "parser": "marker",
        "parser_version": "test",
        "page_count": 2,
        "texts": [
            {"item_ref": "#/texts/0", "label": "title", "text": "DC1000 Quick Notes", "page_numbers": [1]},
            {"item_ref": "#/texts/1", "label": "text", "text": "PREPARATION", "page_numbers": [1]},
            {"item_ref": "#/texts/2", "label": "text", "text": "Unpack the enclosure.", "page_numbers": [1]},
            {"item_ref": "#/texts/3", "label": "text", "text": "MAINTENANCE", "page_numbers": [2]},
            {
                "item_ref": "#/texts/4",
                "label": "text",
                "text": "Replace the filter every 90 days.",
                "page_numbers": [2],
            },
        ],
        "tables": [],
        "pages": [
            {"page_number": 1, "width": 612, "height": 792, "source_ref": "p1"},
            {"page_number": 2, "width": 612, "height": 792, "source_ref": "p2"},
        ],
    }
    headings = {
        "doc_id": doc_id,
        "headings": [
            {
                "title": "DC1000 Quick Notes",
                "label": "title",
                "level": 1,
                "page_number": 1,
                "item_ref": "#/texts/0",
                "children": [
                    {
                        "title": "PREPARATION",
                        "label": "section_header",
                        "level": 2,
                        "page_number": 1,
                        "item_ref": "#/texts/1",
                        "children": [],
                    },
                    {
                        "title": "MAINTENANCE",
                        "label": "section_header",
                        "level": 2,
                        "page_number": 2,
                        "item_ref": "#/texts/3",
                        "children": [],
                    },
                ],
            }
        ],
    }
    _write_parsed_artifacts(data_dir, doc_id=doc_id, structure=structure, headings=headings)

    sections = section_document(doc_id, data_dir=data_dir)

    assert [section.title for section in sections] == ["PREPARATION", "MAINTENANCE"]
    assert sections[0].section_type == "installation"
    assert sections[1].section_type == "maintenance"
    assert sections[1].page_range == (2, 2)


def test_section_document_handles_documents_without_headings(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_parsed_fixture(data_dir, _write_pdf(tmp_path / "headingless.pdf"), revision="Rev 5")
    structure = {
        "doc_id": doc_id,
        "parser": "docling",
        "parser_version": "test",
        "page_count": 2,
        "texts": [
            {"item_ref": "#/texts/0", "label": "title", "text": "Plain Service Notes", "page_numbers": [1]},
            {"item_ref": "#/texts/1", "label": "text", "text": "Inspect wiring before startup.", "page_numbers": [1]},
            {"item_ref": "#/texts/2", "label": "text", "text": "Log all error codes for review.", "page_numbers": [2]},
        ],
        "tables": [],
        "pages": [
            {"page_number": 1, "width": 612, "height": 792, "source_ref": "p1"},
            {"page_number": 2, "width": 612, "height": 792, "source_ref": "p2"},
        ],
    }
    headings = {"doc_id": doc_id, "headings": []}
    _write_parsed_artifacts(data_dir, doc_id=doc_id, structure=structure, headings=headings)

    sections = section_document(doc_id, data_dir=data_dir)

    assert len(sections) == 1
    assert sections[0].title == "Plain Service Notes"
    assert sections[0].section_type == "other"
    assert sections[0].page_range == (1, 2)


def test_section_cli_supports_doc_id_and_all(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    first_doc_id = _register_parsed_fixture(data_dir, _write_pdf(tmp_path / "first.pdf"))
    second_doc_id = _register_parsed_fixture(
        data_dir,
        _write_pdf(tmp_path / "second.pdf", content=b"%PDF-1.4\n% second fixture\n"),
        revision="Rev 4",
    )
    for doc_id in (first_doc_id, second_doc_id):
        parsed_dir = data_dir / "parsed" / doc_id
        parsed_dir.mkdir(parents=True, exist_ok=True)
        (parsed_dir / "structure.json").write_text("{}", encoding="utf-8")
    runner = CliRunner()
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)}
    calls: list[str] = []

    def fake_section(doc_id: str, *, data_dir: Path | None = None) -> list[object]:
        calls.append(doc_id)
        assert data_dir == Path(env["KNOWLEDGE_FORGE_DATA_DIR"])
        return [object(), object()]

    monkeypatch.setattr("knowledge_forge.cli.section_document", fake_section)

    single = runner.invoke(cli, ["section", first_doc_id], env=env)
    assert single.exit_code == 0
    assert f"Sectioned {first_doc_id}" in single.output

    every = runner.invoke(cli, ["section", "--all"], env=env)
    assert every.exit_code == 0
    assert f"Sectioned {first_doc_id} -> 2 sections" in every.output
    assert f"Sectioned {second_doc_id} -> 2 sections" in every.output
    assert calls == [first_doc_id, first_doc_id, second_doc_id]
