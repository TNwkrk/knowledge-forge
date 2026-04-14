"""Parsing package."""

from . import docling_parser as _docling_parser
from . import quality as _quality

ParseResult = _docling_parser.ParseResult
parse_all_documents = _docling_parser.parse_all_documents
parse_document = _docling_parser.parse_document
ParseQualityReport = _quality.ParseQualityReport
score_parse = _quality.score_parse

__all__ = [
    "ParseResult",
    "ParseQualityReport",
    "parse_all_documents",
    "parse_document",
    "score_parse",
]

if hasattr(_docling_parser, "parse_flow"):
    parse_flow = _docling_parser.parse_flow
    __all__.append("parse_flow")
