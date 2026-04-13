"""OCR normalization pipeline using OCRmyPDF."""

from __future__ import annotations

import json
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import ocrmypdf
from ocrmypdf.pdfinfo import PdfInfo
from pydantic import BaseModel, ConfigDict, Field
from yaml import safe_load

from knowledge_forge.intake.importer import get_data_dir, load_manifest
from knowledge_forge.intake.manifest import DocumentStatus, ManifestEntry, compute_sha256


class NormalizationResult(BaseModel):
    """Metadata captured from an OCR normalization run."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    output_path: Path
    input_checksum: str
    page_count: int
    ocr_applied: bool
    pages_ocrd: int
    confidence_scores: list[float] = Field(default_factory=list)
    processing_time: float


@dataclass(frozen=True)
class NormalizationSettings:
    """User-configurable settings for OCRmyPDF execution."""

    language: str = "eng"
    deskew: bool = True
    clean: bool = True
    optimize: int = 1


def normalize_document(doc_id: str, *, data_dir: Path | None = None) -> NormalizationResult:
    """Run OCR normalization for a registered document."""
    resolved_data_dir = get_data_dir(data_dir)
    manifest = load_manifest(resolved_data_dir, doc_id)
    raw_path = _resolve_raw_path(resolved_data_dir, doc_id)
    output_dir = resolved_data_dir / "normalized"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"{doc_id}.pdf"
    meta_path = output_dir / f"{doc_id}.meta.json"
    input_checksum = compute_sha256(raw_path)

    existing = _load_normalization_meta(meta_path)
    if (
        existing is not None
        and existing.input_checksum == input_checksum
        and output_path.exists()
    ):
        _persist_manifest_status(manifest, resolved_data_dir)
        return existing

    settings = _load_normalization_settings()
    start = time.perf_counter()
    pdf_info = PdfInfo(raw_path)
    page_count = len(pdf_info.pages)
    text_pages = sum(1 for page in pdf_info.pages if page and page.has_text)
    needs_ocr = text_pages != page_count
    pages_requiring_ocr = page_count - text_pages if needs_ocr else 0
    confidence_scores: list[float] = []

    if needs_ocr:
        ocrmypdf.ocr(
            raw_path,
            output_path,
            language=settings.language,
            deskew=settings.deskew,
            clean=settings.clean,
            optimize=settings.optimize,
        )
    else:
        shutil.copy2(raw_path, output_path)

    processing_time = time.perf_counter() - start
    result = NormalizationResult(
        output_path=output_path,
        input_checksum=input_checksum,
        page_count=page_count,
        ocr_applied=needs_ocr,
        pages_ocrd=pages_requiring_ocr,
        confidence_scores=confidence_scores,
        processing_time=processing_time,
    )

    meta_path.write_text(result.model_dump_json(indent=2), encoding="utf-8")
    _persist_manifest_status(manifest, resolved_data_dir)
    return result


def _resolve_raw_path(data_dir: Path, doc_id: str) -> Path:
    """Find the stored raw PDF path for a manifest doc_id."""
    candidates = sorted((data_dir / "raw").glob(f"{doc_id}.*"))
    if not candidates:
        raise FileNotFoundError(f"raw file not found for doc_id '{doc_id}'")
    return candidates[0]


def _load_normalization_settings(config_path: Path | None = None) -> NormalizationSettings:
    """Load normalization settings from the pipeline config if available."""
    path = config_path or Path("config/pipeline.yaml")
    if not path.exists():
        return NormalizationSettings()

    payload = safe_load(path.read_text(encoding="utf-8")) or {}
    normalize_config: dict[str, Any] = payload.get("stages", {}).get("normalize", {}) or {}
    defaults = NormalizationSettings()
    allowed: dict[str, Any] = {}
    for key in ("language", "deskew", "clean", "optimize"):
        if key in normalize_config:
            allowed[key] = normalize_config[key]
    return NormalizationSettings(**allowed) if allowed else defaults


def _load_normalization_meta(meta_path: Path) -> NormalizationResult | None:
    """Restore a prior normalization result if it exists on disk."""
    if not meta_path.exists():
        return None
    return NormalizationResult.model_validate_json(meta_path.read_text(encoding="utf-8"))


def _persist_manifest_status(manifest: ManifestEntry, data_dir: Path) -> None:
    """Transition a manifest to normalized status and persist it."""
    normalized = manifest.transition_status(
        DocumentStatus.NORMALIZED,
        reason="normalized via OCRmyPDF",
    )
    if normalized == manifest and manifest.document.status == DocumentStatus.NORMALIZED:
        return

    manifest_path = data_dir / "manifests" / f"{manifest.doc_id}.yaml"
    manifest_path.write_text(normalized.to_yaml(), encoding="utf-8")


# Prefect flow wrapper
try:  # pragma: no cover - optional import boundary
    from prefect import flow

    @flow(name="normalize-document")
    def normalization_flow(doc_id: str, data_dir: str | Path | None = None) -> NormalizationResult:
        """Prefect flow wrapper for OCR normalization."""
        return normalize_document(doc_id, data_dir=data_dir if data_dir else None)

except Exception:  # pragma: no cover
    # Prefect is optional at runtime; skip flow registration if unavailable.
    pass
