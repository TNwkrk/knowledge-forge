"""Tests for OCR normalization pipeline."""

from __future__ import annotations

import json
from pathlib import Path

import ocrmypdf
from fpdf import FPDF
from ocrmypdf.pdfinfo import PdfInfo
from PIL import Image, ImageDraw

from knowledge_forge.intake.importer import RegistrationRequest, load_manifest, register_document
from knowledge_forge.intake.manifest import DocumentStatus
from knowledge_forge.normalize import normalize_document


def _build_digital_pdf(path: Path) -> Path:
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Helvetica", size=12)
    pdf.text(10, 20, "Knowledge Forge normalization")
    pdf.output(path)
    return path


def _build_scanned_pdf(path: Path) -> Path:
    image = Image.new("RGB", (200, 200), color="white")
    draw = ImageDraw.Draw(image)
    draw.rectangle((20, 20, 180, 180), outline="black", width=2)
    draw.text((40, 90), "Scan me", fill="black")
    image.save(path, "PDF")
    return path


def _register_fixture(pdf_path: Path, data_dir: Path) -> str:
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
    meta_path = data_dir / "normalized" / f"{doc_id}.meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta["ocr_applied"] is True
    assert meta["pages_ocrd"] == 1
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
    meta_path = data_dir / "normalized" / f"{doc_id}.meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta["ocr_applied"] is False
    assert meta["pages_ocrd"] == 0
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
