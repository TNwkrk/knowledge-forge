"""Tests for canonical parse sectioning."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from knowledge_forge.cli import cli
from knowledge_forge.intake.importer import RegistrationRequest, register_document
from knowledge_forge.intake.manifest import DocumentStatus
from knowledge_forge.parse import section_all_documents, section_document


def _register_parsed_fixture(
    data_dir: Path,
    pdf_path: Path,
    *,
    revision: str = "Rev 3",
    document_type: str = "Service Manual",
    document_class: str = "authoritative-technical",
) -> str:
    request = RegistrationRequest(
        pdf_path=pdf_path,
        manufacturer="Honeywell",
        family="DC1000",
        model_applicability=["DC1000"],
        document_type=document_type,
        revision=revision,
        publication_date=None,
        language="en",
        priority=1,
        document_class=document_class,
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


def test_section_document_types_operational_sections_and_preserves_ordered_steps(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_parsed_fixture(
        data_dir,
        _write_pdf(tmp_path / "sop.pdf"),
        revision="Rev SOP",
        document_type="SOP",
        document_class="operational",
    )
    structure = {
        "doc_id": doc_id,
        "parser": "docling",
        "parser_version": "test",
        "page_count": 3,
        "texts": [
            {"item_ref": "#/texts/0", "label": "title", "text": "DC1000 Lockout SOP", "page_numbers": [1]},
            {
                "item_ref": "#/texts/1",
                "label": "section_header",
                "text": "Standard Operating Procedure",
                "page_numbers": [1],
            },
            {
                "item_ref": "#/texts/2",
                "label": "list_item",
                "text": "1. De-energize the controller.",
                "page_numbers": [1],
            },
            {"item_ref": "#/texts/3", "label": "list_item", "text": "2. Verify zero voltage.", "page_numbers": [1]},
            {"item_ref": "#/texts/4", "label": "section_header", "text": "Inspection Checklist", "page_numbers": [2]},
            {"item_ref": "#/texts/5", "label": "list_item", "text": "- [ ] Confirm panel is dry.", "page_numbers": [2]},
            {
                "item_ref": "#/texts/6",
                "label": "list_item",
                "text": "- [ ] Inspect all terminal lugs.",
                "page_numbers": [2],
            },
            {"item_ref": "#/texts/7", "label": "section_header", "text": "Commissioning", "page_numbers": [3]},
            {"item_ref": "#/texts/8", "label": "text", "text": "Step 1: Restore line power.", "page_numbers": [3]},
            {
                "item_ref": "#/texts/9",
                "label": "text",
                "text": "Step 2: Record ready-state amperage.",
                "page_numbers": [3],
            },
        ],
        "tables": [],
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
                "title": "DC1000 Lockout SOP",
                "label": "title",
                "level": 1,
                "page_number": 1,
                "item_ref": "#/texts/0",
                "children": [
                    {
                        "title": "Standard Operating Procedure",
                        "label": "section_header",
                        "level": 2,
                        "page_number": 1,
                        "item_ref": "#/texts/1",
                        "children": [],
                    },
                    {
                        "title": "Inspection Checklist",
                        "label": "section_header",
                        "level": 2,
                        "page_number": 2,
                        "item_ref": "#/texts/4",
                        "children": [],
                    },
                    {
                        "title": "Commissioning",
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

    assert [section.section_type for section in sections] == ["sop", "checklist", "commissioning"]
    assert [step.text for step in sections[0].ordered_steps] == [
        "De-energize the controller.",
        "Verify zero voltage.",
    ]
    assert [step.text for step in sections[1].ordered_steps] == [
        "Confirm panel is dry.",
        "Inspect all terminal lugs.",
    ]
    assert [step.step_number for step in sections[2].ordered_steps] == [1, 2]


def test_section_document_types_bulletin_and_captures_wiring_callouts(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_parsed_fixture(
        data_dir,
        _write_pdf(tmp_path / "bulletin.pdf"),
        revision="Bulletin 1",
        document_type="Service Bulletin",
    )
    structure = {
        "doc_id": doc_id,
        "parser": "docling",
        "parser_version": "test",
        "page_count": 2,
        "texts": [
            {"item_ref": "#/texts/0", "label": "title", "text": "DC1000 Service Bulletin", "page_numbers": [1]},
            {"item_ref": "#/texts/1", "label": "section_header", "text": "Addendum A", "page_numbers": [1]},
            {
                "item_ref": "#/texts/2",
                "label": "text",
                "text": "Updated terminal torque guidance.",
                "page_numbers": [1],
            },
            {"item_ref": "#/texts/3", "label": "section_header", "text": "Wiring Diagram", "page_numbers": [2]},
            {"item_ref": "#/texts/4", "label": "text", "text": "Figure 2 Wiring Diagram", "page_numbers": [2]},
            {"item_ref": "#/texts/5", "label": "text", "text": "A: Line input", "page_numbers": [2]},
            {"item_ref": "#/texts/6", "label": "text", "text": "B: Alarm relay", "page_numbers": [2]},
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
                "title": "DC1000 Service Bulletin",
                "label": "title",
                "level": 1,
                "page_number": 1,
                "item_ref": "#/texts/0",
                "children": [
                    {
                        "title": "Addendum A",
                        "label": "section_header",
                        "level": 2,
                        "page_number": 1,
                        "item_ref": "#/texts/1",
                        "children": [],
                    },
                    {
                        "title": "Wiring Diagram",
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

    assert [section.section_type for section in sections] == ["addendum", "wiring"]
    assert sections[1].figure_regions[0].label == "Figure 2 Wiring Diagram"
    assert sections[1].figure_regions[0].callouts == ["A: Line input", "B: Alarm relay"]


def test_section_document_ignores_page_headers_as_section_boundaries(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_parsed_fixture(data_dir, _write_pdf(tmp_path / "page-headers.pdf"), revision="Rev 6")
    structure = {
        "doc_id": doc_id,
        "parser": "docling",
        "parser_version": "test",
        "page_count": 2,
        "texts": [
            {"item_ref": "#/texts/0", "label": "title", "text": "Service Notes", "page_numbers": [1]},
            {"item_ref": "#/texts/1", "label": "page_header", "text": "Service Notes", "page_numbers": [1]},
            {"item_ref": "#/texts/2", "label": "text", "text": "General intro text.", "page_numbers": [1]},
            {"item_ref": "#/texts/3", "label": "section_header", "text": "Maintenance", "page_numbers": [2]},
            {"item_ref": "#/texts/4", "label": "text", "text": "Replace the filter yearly.", "page_numbers": [2]},
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
                "title": "Service Notes",
                "label": "title",
                "level": 1,
                "page_number": 1,
                "item_ref": "#/texts/0",
                "children": [
                    {
                        "title": "Maintenance",
                        "label": "section_header",
                        "level": 2,
                        "page_number": 2,
                        "item_ref": "#/texts/3",
                        "children": [],
                    }
                ],
            }
        ],
    }
    _write_parsed_artifacts(data_dir, doc_id=doc_id, structure=structure, headings=headings)

    sections = section_document(doc_id, data_dir=data_dir)

    assert [section.title for section in sections] == ["Service Notes", "Maintenance"]
    assert sections[0].content == "General intro text."
    assert sections[1].content.endswith("Replace the filter yearly.")


def test_section_document_assigns_preamble_tables_before_first_heading(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_parsed_fixture(data_dir, _write_pdf(tmp_path / "preamble-table.pdf"), revision="Rev 7")
    structure = {
        "doc_id": doc_id,
        "parser": "docling",
        "parser_version": "test",
        "page_count": 2,
        "texts": [
            {"item_ref": "#/texts/0", "label": "title", "text": "DC1000 Service Notes", "page_numbers": [1]},
            {"item_ref": "#/texts/1", "label": "text", "text": "Read this overview first.", "page_numbers": [1]},
            {"item_ref": "#/texts/2", "label": "section_header", "text": "Installation", "page_numbers": [2]},
            {"item_ref": "#/texts/3", "label": "text", "text": "Mount as instructed.", "page_numbers": [2]},
        ],
        "tables": [
            {
                "item_ref": "#/tables/0",
                "label": "table",
                "page_numbers": [1],
                "row_count": 2,
                "column_count": 2,
                "data": [["Item", "Value"], ["Torque", "10 Nm"]],
            }
        ],
        "pages": [
            {"page_number": 1, "width": 612, "height": 792, "source_ref": "p1"},
            {"page_number": 2, "width": 612, "height": 792, "source_ref": "p2"},
        ],
    }
    headings = {
        "doc_id": doc_id,
        "headings": [
            {
                "title": "DC1000 Service Notes",
                "label": "title",
                "level": 1,
                "page_number": 1,
                "item_ref": "#/texts/0",
                "children": [
                    {
                        "title": "Installation",
                        "label": "section_header",
                        "level": 2,
                        "page_number": 2,
                        "item_ref": "#/texts/2",
                        "children": [],
                    }
                ],
            }
        ],
    }
    _write_parsed_artifacts(data_dir, doc_id=doc_id, structure=structure, headings=headings)

    sections = section_document(doc_id, data_dir=data_dir)

    assert [section.title for section in sections] == ["DC1000 Service Notes", "Installation"]
    assert "| Torque | 10 Nm |" in sections[0].content
    assert "| Torque | 10 Nm |" not in sections[1].content


def test_section_all_documents_skips_incomplete_parsed_artifacts(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    complete_doc = _register_parsed_fixture(data_dir, _write_pdf(tmp_path / "complete.pdf"), revision="Rev 8")
    incomplete_doc = _register_parsed_fixture(data_dir, _write_pdf(tmp_path / "incomplete.pdf"), revision="Rev 9")

    structure = {
        "doc_id": complete_doc,
        "parser": "docling",
        "parser_version": "test",
        "page_count": 1,
        "texts": [
            {"item_ref": "#/texts/0", "label": "title", "text": "Complete", "page_numbers": [1]},
            {"item_ref": "#/texts/1", "label": "section_header", "text": "Safety", "page_numbers": [1]},
            {"item_ref": "#/texts/2", "label": "text", "text": "Use PPE.", "page_numbers": [1]},
        ],
        "tables": [],
        "pages": [{"page_number": 1, "width": 612, "height": 792, "source_ref": "p1"}],
    }
    headings = {
        "doc_id": complete_doc,
        "headings": [
            {
                "title": "Complete",
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
                    }
                ],
            }
        ],
    }
    _write_parsed_artifacts(data_dir, doc_id=complete_doc, structure=structure, headings=headings)

    incomplete_parsed_dir = data_dir / "parsed" / incomplete_doc
    incomplete_parsed_dir.mkdir(parents=True, exist_ok=True)
    (incomplete_parsed_dir / "structure.json").write_text(json.dumps(structure, indent=2), encoding="utf-8")

    result = section_all_documents(data_dir=data_dir)

    assert len(result) == 1
    assert [section.doc_id for section in result[0]] == [complete_doc]


def test_section_document_falls_back_to_default_context_when_manifest_missing(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_parsed_fixture(data_dir, _write_pdf(tmp_path / "no-manifest.pdf"), revision="Rev NM")
    structure = {
        "doc_id": doc_id,
        "parser": "docling",
        "parser_version": "test",
        "page_count": 1,
        "texts": [
            {"item_ref": "#/texts/0", "label": "title", "text": "Orphan Notes", "page_numbers": [1]},
            {"item_ref": "#/texts/1", "label": "section_header", "text": "Safety", "page_numbers": [1]},
            {"item_ref": "#/texts/2", "label": "text", "text": "Wear PPE.", "page_numbers": [1]},
        ],
        "tables": [],
        "pages": [{"page_number": 1, "width": 612, "height": 792, "source_ref": "p1"}],
    }
    headings = {
        "doc_id": doc_id,
        "headings": [
            {
                "title": "Orphan Notes",
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
                    }
                ],
            }
        ],
    }
    _write_parsed_artifacts(data_dir, doc_id=doc_id, structure=structure, headings=headings)
    (data_dir / "manifests" / f"{doc_id}.yaml").unlink()

    sections = section_document(doc_id, data_dir=data_dir)

    assert len(sections) == 1
    assert sections[0].section_type == "safety"


def test_section_document_headingless_sop_extracts_steps(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_parsed_fixture(
        data_dir,
        _write_pdf(tmp_path / "headingless-sop.pdf"),
        revision="Rev SOP2",
        document_type="SOP",
        document_class="operational",
    )
    structure = {
        "doc_id": doc_id,
        "parser": "docling",
        "parser_version": "test",
        "page_count": 1,
        "texts": [
            {"item_ref": "#/texts/0", "label": "title", "text": "Lockout Procedure", "page_numbers": [1]},
            {"item_ref": "#/texts/1", "label": "list_item", "text": "1. Isolate power.", "page_numbers": [1]},
            {"item_ref": "#/texts/2", "label": "list_item", "text": "2. Apply lockout tag.", "page_numbers": [1]},
        ],
        "tables": [],
        "pages": [{"page_number": 1, "width": 612, "height": 792, "source_ref": "p1"}],
    }
    headings = {"doc_id": doc_id, "headings": []}
    _write_parsed_artifacts(data_dir, doc_id=doc_id, structure=structure, headings=headings)

    sections = section_document(doc_id, data_dir=data_dir)

    assert len(sections) == 1
    # "Lockout Procedure" title matches "procedure" keyword → classified as workflow (not other)
    assert sections[0].section_type == "workflow"
    assert [step.text for step in sections[0].ordered_steps] == ["Isolate power.", "Apply lockout tag."]


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
        (parsed_dir / "headings.json").write_text("{}", encoding="utf-8")
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
