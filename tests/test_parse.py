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
    fake_document = _FakeDoclingDocument(structure=structure, markdown="# DC1000 Service Manual\n\n## Safety")
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
    assert "# DC1000 Service Manual" in result.content_path.read_text(encoding="utf-8")

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
        return SimpleNamespace(content_path=data_dir / "parsed" / doc_id / "content.md")

    monkeypatch.setattr("knowledge_forge.cli.parse_document", fake_parse)

    single = runner.invoke(cli, ["parse", first_doc_id], env=env)
    assert single.exit_code == 0
    assert f"Parsed {first_doc_id}" in single.output

    every = runner.invoke(cli, ["parse", "--all"], env=env)
    assert every.exit_code == 0
    assert f"Parsed {first_doc_id}" in every.output
    assert f"Parsed {second_doc_id}" in every.output
    assert calls == [first_doc_id, first_doc_id, second_doc_id]


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
