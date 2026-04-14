"""Parsing package."""

from . import docling_parser as _docling_parser

ParseResult = _docling_parser.ParseResult
parse_all_documents = _docling_parser.parse_all_documents
parse_document = _docling_parser.parse_document

__all__ = [
    "ParseResult",
    "parse_all_documents",
    "parse_document",
]

if hasattr(_docling_parser, "parse_flow"):
    parse_flow = _docling_parser.parse_flow
    __all__.append("parse_flow")
