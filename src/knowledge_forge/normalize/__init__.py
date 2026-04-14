"""Normalization package."""

from knowledge_forge.normalize import ocr as _ocr

NormalizationResult = _ocr.NormalizationResult
PageNormalizationMetadata = _ocr.PageNormalizationMetadata
inspect_normalization = _ocr.inspect_normalization
normalize_document = _ocr.normalize_document

__all__ = [
    "NormalizationResult",
    "PageNormalizationMetadata",
    "inspect_normalization",
    "normalize_document",
]

if hasattr(_ocr, "normalization_flow"):
    normalization_flow = _ocr.normalization_flow
    __all__.append("normalization_flow")
