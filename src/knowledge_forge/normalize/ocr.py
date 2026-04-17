"""OCR normalization pipeline using OCRmyPDF."""

from __future__ import annotations

import shutil
import time
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import ocrmypdf
from ocrmypdf.exceptions import MissingDependencyError
from ocrmypdf.pdfinfo import PdfInfo
from pdfminer.high_level import extract_text
from pydantic import BaseModel, ConfigDict
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
    processing_time: float
    page_metadata: list["PageNormalizationMetadata"]


class PageNormalizationMetadata(BaseModel):
    """Per-page OCR analysis captured during normalization."""

    page_number: int
    has_text_before: bool
    has_vector: bool
    text_density_before: float
    text_density_after: float
    ocr_applied: bool
    confidence: float
    bypass_reason: str | None = None


@dataclass(frozen=True)
class NormalizationSettings:
    """User-configurable settings for OCRmyPDF execution."""

    language: str = "eng"
    deskew: bool = True
    clean: bool = True
    optimize: int = 1
    text_density_threshold: float = 1.0
    confidence_density_target: float = 5.0
    bypass_document_types: tuple[str, ...] = ()
    skip_vector_pages: bool = True
    fast_text_detection_page_threshold: int = 25


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
    if existing is not None and existing.input_checksum == input_checksum and output_path.exists():
        _persist_manifest_status(manifest, resolved_data_dir)
        return existing

    settings = _load_normalization_settings()
    start = time.perf_counter()
    pdf_info = PdfInfo(raw_path)
    page_count = len(pdf_info.pages)
    page_analyses = _analyze_pages(raw_path, pdf_info, manifest.document.document_type, settings)
    pages_requiring_ocr = sum(1 for page in page_analyses if page["ocr_applied"])
    needs_ocr = pages_requiring_ocr > 0

    if needs_ocr:
        ocr_completed = _run_ocr(
            raw_path,
            output_path,
            settings=settings,
            pages=_serialize_pages(page["page_number"] for page in page_analyses if page["ocr_applied"]),
        )
        if not ocr_completed:
            shutil.copy2(raw_path, output_path)
            page_analyses = [
                {
                    **page,
                    "ocr_applied": False,
                    "bypass_reason": page["bypass_reason"] or "ocr_dependency_missing",
                }
                for page in page_analyses
            ]
    else:
        shutil.copy2(raw_path, output_path)
        ocr_completed = False

    page_metadata = _build_page_metadata(output_path, page_analyses, settings)
    pages_ocrd = sum(1 for page in page_metadata if page.ocr_applied)

    processing_time = time.perf_counter() - start
    result = NormalizationResult(
        output_path=output_path,
        input_checksum=input_checksum,
        page_count=page_count,
        ocr_applied=ocr_completed,
        pages_ocrd=pages_ocrd,
        processing_time=processing_time,
        page_metadata=page_metadata,
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
    for key in (
        "language",
        "deskew",
        "clean",
        "optimize",
        "text_density_threshold",
        "confidence_density_target",
        "skip_vector_pages",
        "fast_text_detection_page_threshold",
    ):
        if key in normalize_config:
            allowed[key] = normalize_config[key]
    if "bypass_document_types" in normalize_config:
        allowed["bypass_document_types"] = tuple(str(value) for value in normalize_config["bypass_document_types"])
    return NormalizationSettings(**allowed) if allowed else defaults


def _load_normalization_meta(meta_path: Path) -> NormalizationResult | None:
    """Restore a prior normalization result if it exists on disk."""
    if not meta_path.exists():
        return None
    return NormalizationResult.model_validate_json(meta_path.read_text(encoding="utf-8"))


def _persist_manifest_status(manifest: ManifestEntry, data_dir: Path) -> None:
    """Transition a manifest to normalized status and persist it."""
    if manifest.document.status.value in {
        DocumentStatus.PARSED.value,
        DocumentStatus.EXTRACTED.value,
        DocumentStatus.COMPILED.value,
        DocumentStatus.PUBLISHED.value,
    }:
        return
    normalized = manifest.transition_status(
        DocumentStatus.NORMALIZED,
        reason="normalized via OCRmyPDF",
    )
    if normalized == manifest and manifest.document.status == DocumentStatus.NORMALIZED:
        return

    manifest_path = data_dir / "manifests" / f"{manifest.doc_id}.yaml"
    manifest_path.write_text(normalized.to_yaml(), encoding="utf-8")


def inspect_normalization(doc_id: str, *, data_dir: Path | None = None) -> NormalizationResult:
    """Load persisted normalization metadata for CLI inspection."""
    resolved_data_dir = get_data_dir(data_dir)
    meta_path = resolved_data_dir / "normalized" / f"{doc_id}.meta.json"
    existing = _load_normalization_meta(meta_path)
    if existing is None:
        raise FileNotFoundError(f"normalization metadata not found for doc_id '{doc_id}'")
    return existing


def _analyze_pages(
    raw_path: Path,
    pdf_info: PdfInfo,
    document_type: str,
    settings: NormalizationSettings,
) -> list[dict[str, Any]]:
    """Decide which pages should receive OCR and record baseline metrics."""
    bypass_document_type = document_type.casefold() in {
        value.strip().casefold() for value in settings.bypass_document_types if value.strip()
    }
    analyses: list[dict[str, Any]] = []
    for index, page in enumerate(pdf_info.pages, start=1):
        has_text_before = bool(page and page.has_text)
        has_vector = bool(page and page.has_vector)
        if has_text_before and len(pdf_info.pages) >= settings.fast_text_detection_page_threshold:
            density_before = max(settings.text_density_threshold, settings.confidence_density_target)
        else:
            density_before = _extract_text_density(raw_path, index - 1, page)
        bypass_reason: str | None = None
        ocr_applied = False
        if bypass_document_type:
            bypass_reason = "document_type_bypass"
        elif settings.skip_vector_pages and has_vector and not has_text_before:
            bypass_reason = "vector_page_bypass"
        else:
            ocr_applied = (not has_text_before) or density_before < settings.text_density_threshold
        analyses.append(
            {
                "page_number": index,
                "has_text_before": has_text_before,
                "has_vector": has_vector,
                "text_density_before": density_before,
                "ocr_applied": ocr_applied,
                "bypass_reason": bypass_reason,
            }
        )
    return analyses


def _build_page_metadata(
    output_path: Path,
    page_analyses: list[dict[str, Any]],
    settings: NormalizationSettings,
) -> list[PageNormalizationMetadata]:
    """Measure post-normalization text density and finalize page metadata."""
    normalized_info = PdfInfo(output_path)
    metadata: list[PageNormalizationMetadata] = []
    for analysis, page in zip(page_analyses, normalized_info.pages, strict=True):
        density_after = _extract_text_density(output_path, analysis["page_number"] - 1, page)
        metadata.append(
            PageNormalizationMetadata(
                page_number=analysis["page_number"],
                has_text_before=analysis["has_text_before"],
                has_vector=analysis["has_vector"],
                text_density_before=round(analysis["text_density_before"], 4),
                text_density_after=round(density_after, 4),
                ocr_applied=analysis["ocr_applied"],
                confidence=_confidence_score(density_after, settings.confidence_density_target),
                bypass_reason=analysis["bypass_reason"],
            )
        )
    return metadata


def _extract_text_density(pdf_path: Path, page_index: int, page_info: Any) -> float:
    """Approximate page text density as visible characters per square inch."""
    text = extract_text(str(pdf_path), page_numbers=[page_index]) or ""
    visible_characters = sum(1 for char in text if not char.isspace())
    area = max(float(page_info.width_inches) * float(page_info.height_inches), 1.0)
    return visible_characters / area


def _confidence_score(text_density_after: float, confidence_density_target: float) -> float:
    """Convert post-normalization text density into a 0-1 confidence score."""
    if confidence_density_target <= 0:
        return 1.0
    return round(min(text_density_after / confidence_density_target, 1.0), 3)


def _serialize_pages(page_numbers: Iterable[int]) -> str:
    """Convert 1-based page numbers into the OCRmyPDF pages selector."""
    return ",".join(str(page_number) for page_number in page_numbers)


def _run_ocr(raw_path: Path, output_path: Path, *, settings: NormalizationSettings, pages: str) -> bool:
    """Run OCRmyPDF and retry without cleaning when unpaper is unavailable."""
    try:
        ocrmypdf.ocr(
            raw_path,
            output_path,
            language=settings.language,
            deskew=settings.deskew,
            clean=settings.clean,
            optimize=settings.optimize,
            pages=pages,
        )
        return True
    except MissingDependencyError as exc:
        if settings.clean and "unpaper" in str(exc).lower():
            try:
                ocrmypdf.ocr(
                    raw_path,
                    output_path,
                    language=settings.language,
                    deskew=settings.deskew,
                    clean=False,
                    optimize=settings.optimize,
                    pages=pages,
                )
                return True
            except MissingDependencyError:
                return False
        return False


# Prefect flow wrapper
try:  # pragma: no cover - optional import boundary
    from prefect import flow

    @flow(name="normalize-document")
    def normalization_flow(doc_id: str, data_dir: str | Path | None = None) -> NormalizationResult:
        """Prefect flow wrapper for OCR normalization."""
        return normalize_document(doc_id, data_dir=data_dir if data_dir else None)

except ImportError:  # pragma: no cover
    # Prefect is optional at runtime; skip flow registration if unavailable.
    pass
