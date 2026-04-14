"""Tests for the Docling parser integration."""

from __future__ import annotations

import builtins
import importlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace

from click.testing import CliRunner

from knowledge_forge.cli import cli
from knowledge_forge.intake.importer import RegistrationRequest, load_manifest, register_document
from knowledge_forge.intake.manifest import DocumentStatus
from knowledge_forge.parse.docling_parser import parse_document
from knowledge_forge.parse.quality import score_parse


def _write_pdf(path: Path, text: bytes = b"BT\n/F1 12 Tf\n10 20 Td\n(Knowledge Forge parse) Tj\nET") -> Path:
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        (
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 200]"
            b" /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>"
        ),
        b"<< /Length " + str(len(text)).encode("ascii") + b" >>\nstream\n" + text + b"\nendstream",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
    ]
    pdf = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets: list[int] = []
    for obj_num, obj in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{obj_num} 0 obj\n".encode("ascii"))
        pdf.extend(obj)
        pdf.extend(b"\nendobj\n")
    xref_offset = len(pdf)
    pdf.extend(f"xref\n0 {len(objects) + 1}\n".encode("ascii"))
    pdf.extend(b"0000000000 65535 f \n")
    for offset in offsets:
        pdf.extend(f"{offset:010d} 00000 n \n".encode("ascii"))
    trailer = f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_offset}\n%%EOF\n"
    pdf.extend(trailer.encode("ascii"))
    path.write_bytes(pdf)
    return path


def _register_normalized_fixture(pdf_path: Path, data_dir: Path, *, revision: str = "Rev 3") -> str:
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
    manifest = result.manifest.transition_status(DocumentStatus.NORMALIZED, reason="test normalized fixture")
    manifest_path.write_text(manifest.to_yaml(), encoding="utf-8")

    normalized_dir = data_dir / "normalized"
    normalized_dir.mkdir(parents=True, exist_ok=True)
    normalized_path = normalized_dir / f"{result.manifest.doc_id}.pdf"
    normalized_path.write_bytes(pdf_path.read_bytes())
    return result.manifest.doc_id


class _FakeDoclingDocument:
    def __init__(self, structure: dict[str, object], markdown: str) -> None:
        self._structure = structure
        self._markdown = markdown

    def export_to_dict(self) -> dict[str, object]:
        return self._structure

    def export_to_markdown(self) -> str:
        return self._markdown


class _FakeConverter:
    def __init__(self, result: object) -> None:
        self._result = result
        self.calls: list[Path] = []

    def convert(self, source: Path) -> object:
        self.calls.append(source)
        return self._result


def test_parse_document_writes_artifacts_and_updates_manifest(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    source = _write_pdf(tmp_path / "manual.pdf")
    doc_id = _register_normalized_fixture(source, data_dir)
    structure = {
        "texts": [
            {"self_ref": "#/texts/0", "label": "title", "text": "DC1000 Service Manual", "prov": [{"page_no": 1}]},
            {"self_ref": "#/texts/1", "label": "section_header", "text": "Safety", "prov": [{"page_no": 1}]},
            {"self_ref": "#/texts/2", "label": "text", "text": "Wear gloves.", "prov": [{"page_no": 1}]},
            {"self_ref": "#/texts/3", "label": "section_header", "text": "Specifications", "prov": [{"page_no": 2}]},
        ],
        "tables": [
            {
                "self_ref": "#/tables/0",
                "label": "table",
                "prov": [{"page_no": 2}],
                "data": [["Voltage", "120V"]],
            }
        ],
        "pages": {
            "1": {"page_no": 1, "size": {"width": 612, "height": 792}},
            "2": {"page_no": 2, "size": {"width": 612, "height": 792}},
        },
    }
    fake_document = _FakeDoclingDocument(
        structure=structure,
        markdown="# DC1000 Service Manual\n\n## Safety\n\nWear gloves.\n\n## Specifications",
    )
    fake_result = SimpleNamespace(
        status="success",
        document=fake_document,
        input=SimpleNamespace(document_hash="abc123", page_count=2),
        confidence={"mean_score": 0.92},
        timings={"total": 1.25},
        errors=[],
    )
    fake_converter = _FakeConverter(fake_result)
    monkeypatch.setattr("knowledge_forge.parse.docling_parser._get_document_converter", lambda: fake_converter)
    monkeypatch.setattr("knowledge_forge.parse.docling_parser._get_docling_version", lambda: "test-docling")

    result = parse_document(doc_id, data_dir=data_dir)

    assert fake_converter.calls == [data_dir / "normalized" / f"{doc_id}.pdf"]
    assert result.content_path.exists()
    assert result.structure_path.exists()
    assert result.headings_path.exists()
    assert result.tables_path.exists()
    assert result.page_map_path.exists()
    assert result.meta_path.exists()
    assert result.quality_path.exists()
    assert "# DC1000 Service Manual" in result.content_path.read_text(encoding="utf-8")

    parsed_structure = json.loads(result.structure_path.read_text(encoding="utf-8"))
    assert parsed_structure["doc_id"] == doc_id
    assert parsed_structure["parser"] == "docling"
    assert parsed_structure["texts"][0]["text"] == "DC1000 Service Manual"

    headings = json.loads(result.headings_path.read_text(encoding="utf-8"))
    assert headings["headings"][0]["title"] == "DC1000 Service Manual"
    assert headings["headings"][0]["children"][0]["title"] == "Safety"
    assert headings["headings"][0]["children"][1]["page_number"] == 2

    tables = json.loads(result.tables_path.read_text(encoding="utf-8"))
    assert tables["tables"][0]["data"][0] == ["Voltage", "120V"]

    page_map = json.loads(result.page_map_path.read_text(encoding="utf-8"))
    assert page_map["items"][0]["page_numbers"] == [1]
    assert any(item["item_type"] == "table" for item in page_map["items"])

    meta = json.loads(result.meta_path.read_text(encoding="utf-8"))
    assert meta["parser"] == "docling"
    assert meta["parser_version"] == "test-docling"
    assert meta["page_count"] == 2
    assert meta["document_hash"] == "abc123"
    quality = json.loads(result.quality_path.read_text(encoding="utf-8"))
    assert quality["overall_score"] >= 70.0
    assert quality["passes_threshold"] is True

    manifest = load_manifest(data_dir, doc_id)
    assert manifest.document.status == DocumentStatus.PARSED


def test_parse_cli_supports_doc_id_and_all(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    first_doc_id = _register_normalized_fixture(_write_pdf(tmp_path / "first.pdf"), data_dir)
    second_doc_id = _register_normalized_fixture(
        _write_pdf(
            tmp_path / "second.pdf",
            text=b"BT\n/F1 12 Tf\n10 20 Td\n(Knowledge Forge parse second fixture) Tj\nET",
        ),
        data_dir,
        revision="Rev 4",
    )
    runner = CliRunner()
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)}
    calls: list[str] = []

    def fake_parse(doc_id: str, *, data_dir: Path | None = None) -> object:
        calls.append(doc_id)
        assert data_dir == Path(env["KNOWLEDGE_FORGE_DATA_DIR"])
        return SimpleNamespace(
            content_path=data_dir / "parsed" / doc_id / "content.md",
            quality_report=SimpleNamespace(overall_score=91.5),
        )

    monkeypatch.setattr("knowledge_forge.cli.parse_document", fake_parse)

    single = runner.invoke(cli, ["parse", first_doc_id], env=env)
    assert single.exit_code == 0
    assert f"Parsed {first_doc_id}" in single.output

    every = runner.invoke(cli, ["parse", "--all"], env=env)
    assert every.exit_code == 0
    assert f"Parsed {first_doc_id}" in every.output
    assert f"Parsed {second_doc_id}" in every.output
    assert calls == [first_doc_id, first_doc_id, second_doc_id]


def test_parse_quality_cli_reports_metric_breakdown(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = _register_normalized_fixture(_write_pdf(tmp_path / "fixture.pdf"), data_dir)
    runner = CliRunner()
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)}

    report = SimpleNamespace(
        overall_score=88.5,
        passes_threshold=True,
        metrics=SimpleNamespace(
            heading_coverage=90.0,
            table_extraction_rate=100.0,
            text_completeness=85.0,
            structure_depth=75.0,
            page_coverage=92.0,
        ),
    )
    monkeypatch.setattr("knowledge_forge.cli.score_parse", lambda *args, **kwargs: report)

    result = runner.invoke(cli, ["parse", "--quality", doc_id], env=env)

    assert result.exit_code == 0
    assert f"Document: {doc_id}" in result.output
    assert "Overall score: 88.50" in result.output
    assert "heading_coverage\t90.00" in result.output


def test_score_parse_uses_configured_threshold_and_writes_report(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    doc_id = "honeywell-dc1000-service-manual-rev-3"
    output_dir = data_dir / "parsed" / doc_id
    output_dir.mkdir(parents=True, exist_ok=True)

    (output_dir / "content.md").write_text("# Title\n\nOnly partial content.", encoding="utf-8")
    (output_dir / "structure.json").write_text(
        json.dumps(
            {
                "doc_id": doc_id,
                "parser": "docling",
                "parser_version": "test-docling",
                "page_count": 3,
                "texts": [
                    {"item_ref": "#/texts/0", "label": "title", "text": "Title", "page_numbers": [1]},
                    {"item_ref": "#/texts/1", "label": "section_header", "text": "Safety", "page_numbers": [2]},
                    {
                        "item_ref": "#/texts/2",
                        "label": "text",
                        "text": "This is a much longer body paragraph that is only partially rendered.",
                        "page_numbers": [2, 3],
                    },
                ],
                "tables": [
                    {
                        "item_ref": "#/tables/0",
                        "label": "table",
                        "page_numbers": [3],
                        "row_count": 0,
                        "column_count": 0,
                        "data": [],
                    }
                ],
                "pages": [
                    {"page_number": 1, "width": 612.0, "height": 792.0, "source_ref": "#/pages/1"},
                    {"page_number": 2, "width": 612.0, "height": 792.0, "source_ref": "#/pages/2"},
                    {"page_number": 3, "width": 612.0, "height": 792.0, "source_ref": "#/pages/3"},
                ],
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "headings.json").write_text(
        json.dumps(
            {
                "doc_id": doc_id,
                "headings": [
                    {
                        "title": "Title",
                        "label": "title",
                        "level": 1,
                        "page_number": 1,
                        "item_ref": "#/texts/0",
                        "children": [],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "tables.json").write_text(
        json.dumps(
            {
                "doc_id": doc_id,
                "tables": [
                    {
                        "item_ref": "#/tables/0",
                        "label": "table",
                        "page_numbers": [3],
                        "row_count": 0,
                        "column_count": 0,
                        "data": [],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "page_map.json").write_text(
        json.dumps(
            {
                "doc_id": doc_id,
                "items": [
                    {
                        "item_ref": "#/texts/0",
                        "item_type": "text",
                        "label": "title",
                        "page_numbers": [1],
                        "text": "Title",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    (output_dir / "meta.json").write_text(
        json.dumps(
            {
                "doc_id": doc_id,
                "parser": "docling",
                "parser_version": "test-docling",
                "processed_at": "2026-04-14T00:00:00Z",
                "processing_time_seconds": 1.2,
                "page_count": 3,
                "status": "success",
                "input_path": str(data_dir / "normalized" / f"{doc_id}.pdf"),
                "input_checksum": "0" * 64,
                "document_hash": "abc123",
                "timings": {"total": 1.2},
                "confidence": {"mean_score": 0.45},
                "errors": [],
            }
        ),
        encoding="utf-8",
    )
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(
        "stages:\n  parse:\n    minimum_quality_score: 80\n",
        encoding="utf-8",
    )

    report = score_parse(doc_id, data_dir=data_dir, config_path=config_path)

    assert report.thresholds.minimum_quality_score == 80
    assert report.overall_score < 80
    assert report.passes_threshold is False
    persisted = json.loads((output_dir / "quality.json").read_text(encoding="utf-8"))
    assert persisted["overall_score"] == report.overall_score
    assert persisted["passes_threshold"] is False


def test_parse_package_imports_without_prefect(monkeypatch) -> None:
    original_import = builtins.__import__

    def fake_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "prefect" or name.startswith("prefect."):
            raise ModuleNotFoundError("No module named 'prefect'")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    sys.modules.pop("knowledge_forge.parse.docling_parser", None)
    sys.modules.pop("knowledge_forge.parse", None)

    parse_package = importlib.import_module("knowledge_forge.parse")

    assert hasattr(parse_package, "parse_document")
    assert not hasattr(parse_package, "parse_flow")
