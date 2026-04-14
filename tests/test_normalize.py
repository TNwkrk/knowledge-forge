"""Tests for OCR normalization pipeline."""

from __future__ import annotations

import builtins
import importlib
import json
import sys
from pathlib import Path

import ocrmypdf
from click.testing import CliRunner
from ocrmypdf.pdfinfo import PdfInfo

from knowledge_forge.cli import cli
from knowledge_forge.intake.importer import RegistrationRequest, load_manifest, register_document
from knowledge_forge.intake.manifest import DocumentStatus
from knowledge_forge.normalize import normalize_document
from knowledge_forge.normalize.ocr import (
    NormalizationSettings,
    PageNormalizationMetadata,
    _load_normalization_settings,
)


def _write_pdf(path: Path, content_streams: list[bytes | None] | None = None) -> Path:
    streams = content_streams or [None]
    font_object_num = 3 + len(streams) * 2
    objects = [b"<< /Type /Catalog /Pages 2 0 R >>"]

    kids = " ".join(f"{3 + index * 2} 0 R" for index in range(len(streams)))
    objects.append(f"<< /Type /Pages /Kids [{kids}] /Count {len(streams)} >>".encode("ascii"))

    for index, content_stream in enumerate(streams):
        page_object_num = 3 + index * 2
        contents_object_num = page_object_num + 1
        page_object = b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 200 200]"
        if content_stream is not None:
            page_object += b" /Resources << /Font << /F1 " + f"{font_object_num} 0 R".encode("ascii") + b" >> >>"
            page_object += b" /Contents " + f"{contents_object_num} 0 R".encode("ascii")
        page_object += b" >>"
        objects.append(page_object)

        if content_stream is not None:
            objects.append(
                b"<< /Length "
                + str(len(content_stream)).encode("ascii")
                + b" >>\nstream\n"
                + content_stream
                + b"\nendstream"
            )
        else:
            objects.append(b"<< /Length 0 >>\nstream\n\nendstream")

    if any(content_stream is not None for content_stream in streams):
        objects.append(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

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

    pdf.extend(
        (f"trailer\n<< /Size {len(objects) + 1} /Root 1 0 R >>\nstartxref\n{xref_offset}\n%%EOF\n").encode("ascii")
    )

    path.write_bytes(pdf)
    return path


def _build_digital_pdf(path: Path) -> Path:
    content_stream = b"BT\n/F1 12 Tf\n10 20 Td\n(Knowledge Forge normalization) Tj\nET"
    return _write_pdf(path, [content_stream])


def _build_mixed_pdf(path: Path) -> Path:
    content_stream = b"BT\n/F1 12 Tf\n10 20 Td\n(Knowledge Forge normalization) Tj\nET"
    return _write_pdf(path, [content_stream, None])


def _build_scanned_pdf(path: Path) -> Path:
    return _write_pdf(path, [None])


def _register_fixture(pdf_path: Path, data_dir: Path, *, revision: str = "Rev 3") -> str:
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
    return result.manifest.doc_id


def test_normalize_runs_ocr_for_scanned_pdf(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    source = _build_scanned_pdf(tmp_path / "scanned.pdf")
    doc_id = _register_fixture(source, data_dir)
    calls = {"count": 0}

    def fake_ocr(input_path: Path, output_path: Path, **_: object) -> None:
        calls["count"] += 1
        _build_digital_pdf(Path(output_path))

    monkeypatch.setattr(ocrmypdf, "ocr", fake_ocr)

    result = normalize_document(doc_id, data_dir=data_dir)

    assert calls["count"] == 1
    assert result.ocr_applied is True
    assert result.pages_ocrd == result.page_count == 1
    assert result.page_metadata[0].ocr_applied is True
    meta_path = data_dir / "normalized" / f"{doc_id}.meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta["ocr_applied"] is True
    assert meta["pages_ocrd"] == 1
    assert meta["page_metadata"][0]["ocr_applied"] is True
    assert PdfInfo(result.output_path).pages[0].has_text is True
    manifest = load_manifest(data_dir, doc_id)
    assert manifest.document.status == DocumentStatus.NORMALIZED


def test_normalize_skips_ocr_when_text_present(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    source = _build_digital_pdf(tmp_path / "digital.pdf")
    doc_id = _register_fixture(source, data_dir)

    def _should_not_run(*_: object, **__: object) -> None:
        raise AssertionError("OCR should be skipped for digital PDFs")

    monkeypatch.setattr(ocrmypdf, "ocr", _should_not_run)

    result = normalize_document(doc_id, data_dir=data_dir)

    assert result.ocr_applied is False
    assert result.pages_ocrd == 0
    assert result.page_metadata[0].ocr_applied is False
    meta_path = data_dir / "normalized" / f"{doc_id}.meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta["ocr_applied"] is False
    assert meta["pages_ocrd"] == 0
    assert meta["page_metadata"][0]["ocr_applied"] is False
    assert PdfInfo(result.output_path).pages[0].has_text is True
    manifest = load_manifest(data_dir, doc_id)
    assert manifest.document.status == DocumentStatus.NORMALIZED


def test_normalize_is_idempotent_when_input_unchanged(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    source = _build_scanned_pdf(tmp_path / "scanned.pdf")
    doc_id = _register_fixture(source, data_dir)
    call_count = {"count": 0}

    def fake_ocr(input_path: Path, output_path: Path, **_: object) -> None:
        call_count["count"] += 1
        _build_digital_pdf(Path(output_path))

    monkeypatch.setattr(ocrmypdf, "ocr", fake_ocr)

    first = normalize_document(doc_id, data_dir=data_dir)
    second = normalize_document(doc_id, data_dir=data_dir)

    assert call_count["count"] == 1
    assert second.ocr_applied is True
    assert first.output_path == second.output_path


def test_normalize_selects_only_low_density_pages_for_ocr(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    source = _build_mixed_pdf(tmp_path / "mixed.pdf")
    doc_id = _register_fixture(source, data_dir, revision="Rev 5")
    calls: list[str] = []

    def fake_ocr(input_path: Path, output_path: Path, *, pages: str | None = None, **_: object) -> None:
        calls.append(pages or "")
        _write_pdf(
            Path(output_path),
            [
                b"BT\n/F1 12 Tf\n10 20 Td\n(Page one already had text) Tj\nET",
                b"BT\n/F1 12 Tf\n10 20 Td\n(Page two OCR output) Tj\nET",
            ],
        )

    def fake_density(pdf_path: Path, page_index: int, _: object) -> float:
        densities = {
            ("mixed.pdf", 0): 2.4,
            ("mixed.pdf", 1): 0.0,
            (f"{doc_id}.pdf", 0): 2.4,
            (f"{doc_id}.pdf", 1): 3.1,
        }
        return densities[(pdf_path.name, page_index)]

    monkeypatch.setattr(ocrmypdf, "ocr", fake_ocr)
    monkeypatch.setattr("knowledge_forge.normalize.ocr._extract_text_density", fake_density)

    result = normalize_document(doc_id, data_dir=data_dir)

    assert calls == ["2"]
    assert result.ocr_applied is True
    assert result.pages_ocrd == 1
    assert [page.ocr_applied for page in result.page_metadata] == [False, True]
    assert result.page_metadata[0].text_density_before == 2.4
    assert result.page_metadata[1].text_density_after == 3.1


def test_normalize_respects_document_type_bypass_rules(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    source = _build_scanned_pdf(tmp_path / "scanned.pdf")
    doc_id = _register_fixture(source, data_dir)

    def _should_not_run(*_: object, **__: object) -> None:
        raise AssertionError("OCR should be bypassed for configured document types")

    monkeypatch.setattr(ocrmypdf, "ocr", _should_not_run)
    monkeypatch.setattr(
        "knowledge_forge.normalize.ocr._load_normalization_settings",
        lambda config_path=None: NormalizationSettings(bypass_document_types=("Service Manual",)),
    )

    result = normalize_document(doc_id, data_dir=data_dir)

    assert result.ocr_applied is False
    assert result.pages_ocrd == 0
    assert result.page_metadata[0].bypass_reason == "document_type_bypass"


def test_normalize_cli_supports_doc_id_and_all(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    first_doc_id = _register_fixture(_build_scanned_pdf(tmp_path / "first.pdf"), data_dir)
    second_doc_id = _register_fixture(
        _build_digital_pdf(tmp_path / "second.pdf"),
        data_dir,
        revision="Rev 4",
    )
    runner = CliRunner()
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)}
    calls: list[str] = []

    def fake_normalize(doc_id: str, *, data_dir: Path | None = None) -> object:
        calls.append(doc_id)
        assert data_dir == Path(env["KNOWLEDGE_FORGE_DATA_DIR"])
        return type("Result", (), {"output_path": data_dir / "normalized" / f"{doc_id}.pdf"})()

    monkeypatch.setattr("knowledge_forge.cli.normalize_document", fake_normalize)

    single = runner.invoke(cli, ["normalize", first_doc_id], env=env)
    assert single.exit_code == 0
    assert f"Normalized {first_doc_id}" in single.output

    all_docs = runner.invoke(cli, ["normalize", "--all"], env=env)
    assert all_docs.exit_code == 0
    assert f"Normalized {first_doc_id} ->" in all_docs.output
    assert f"Normalized {second_doc_id} ->" in all_docs.output
    assert calls == [first_doc_id, first_doc_id, second_doc_id]


def test_normalize_inspect_cli_prints_page_metadata(monkeypatch, tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    runner = CliRunner()
    env = {"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)}
    doc_id = "honeywell-dc1000-service-manual-rev-3"

    def fake_inspect(target_doc_id: str, *, data_dir: Path | None = None) -> object:
        assert target_doc_id == doc_id
        assert data_dir == Path(env["KNOWLEDGE_FORGE_DATA_DIR"])
        return type(
            "Result",
            (),
            {
                "output_path": data_dir / "normalized" / f"{target_doc_id}.pdf",
                "page_metadata": [
                    PageNormalizationMetadata(
                        page_number=1,
                        has_text_before=True,
                        has_vector=False,
                        text_density_before=2.4,
                        text_density_after=2.4,
                        ocr_applied=False,
                        confidence=0.48,
                        bypass_reason=None,
                    )
                ],
            },
        )()

    monkeypatch.setattr("knowledge_forge.cli.inspect_normalization", fake_inspect)

    result = runner.invoke(cli, ["normalize", "inspect", doc_id], env=env)

    assert result.exit_code == 0
    assert f"Document: {doc_id}" in result.output
    assert "PAGE\tOCR\tTEXT_BEFORE\tVECTOR" in result.output
    assert "1\tno\tyes\tno\t2.4000\t2.4000\t0.480\t-" in result.output


def test_load_normalization_settings_reads_selective_ocr_config(tmp_path: Path) -> None:
    config_path = tmp_path / "pipeline.yaml"
    config_path.write_text(
        "\n".join(
            [
                "stages:",
                "  normalize:",
                "    language: fra",
                "    deskew: false",
                "    clean: false",
                "    optimize: 3",
                "    text_density_threshold: 1.5",
                "    confidence_density_target: 4.0",
                "    skip_vector_pages: false",
                "    bypass_document_types:",
                "      - Service Manual",
                "      - Quick Start",
            ]
        ),
        encoding="utf-8",
    )

    settings = _load_normalization_settings(config_path)

    assert settings.language == "fra"
    assert settings.deskew is False
    assert settings.clean is False
    assert settings.optimize == 3
    assert settings.text_density_threshold == 1.5
    assert settings.confidence_density_target == 4.0
    assert settings.skip_vector_pages is False
    assert settings.bypass_document_types == ("Service Manual", "Quick Start")


def test_normalize_package_imports_without_prefect() -> None:
    real_import = builtins.__import__

    def fake_import(
        name: str,
        globals: dict[str, object] | None = None,
        locals: dict[str, object] | None = None,
        fromlist: tuple[str, ...] = (),
        level: int = 0,
    ) -> object:
        if name == "prefect" or name.startswith("prefect."):
            raise ModuleNotFoundError("No module named 'prefect'")
        return real_import(name, globals, locals, fromlist, level)

    sys.modules.pop("knowledge_forge.normalize", None)
    sys.modules.pop("knowledge_forge.normalize.ocr", None)
    builtins.__import__ = fake_import
    try:
        module = importlib.import_module("knowledge_forge.normalize")
    finally:
        builtins.__import__ = real_import
        sys.modules.pop("knowledge_forge.normalize", None)
        sys.modules.pop("knowledge_forge.normalize.ocr", None)
        importlib.import_module("knowledge_forge.normalize")

    assert hasattr(module, "normalize_document")
    assert not hasattr(module, "normalization_flow")
