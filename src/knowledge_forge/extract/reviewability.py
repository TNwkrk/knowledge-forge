"""Section and record-quality heuristics used to keep extraction reviewable."""

from __future__ import annotations

import re
from dataclasses import dataclass

from knowledge_forge.extract.schemas import SpecValue
from knowledge_forge.parse.sectioning import Section

_GENERIC_CARRYOVER_TITLES = {
    "attention",
    "caution",
    "continued",
    "danger",
    "example",
    "examples",
    "hazard",
    "important",
    "important user information",
    "note",
    "notes",
    "notes:",
    "remark",
    "remarks",
    "tip",
    "warning",
}
_VAGUE_SECTION_TITLES = {
    "details",
    "example",
    "examples",
    "general",
    "overview",
    "remark",
    "remarks",
    "software",
}
_LOW_SIGNAL_SPEC_TOKENS = {
    "a",
    "b",
    "c",
    "d",
    "e",
    "f",
    "g",
    "h",
    "l",
    "w",
    "x",
    "y",
    "z",
}
_LOW_SIGNAL_SPEC_PHRASES = {
    "character encoding",
    "compliance",
    "csv",
    "download",
    "encoding",
    "export",
    "file extension",
    "file format",
    "file restriction",
    "import",
    "regulatory",
    "rohs",
    "unicode",
    "utf-8",
    "utf8",
    "weee",
    "xml",
}
_SINGLE_LETTER_TITLE_PATTERN = re.compile(r"^[A-Z]$", re.IGNORECASE)
_NUMERIC_FRAGMENT_PATTERN = re.compile(r"^(?:\d+|(?:\d+\.)+|\d+\.)$")
_TOC_PAGE_REFERENCE_PATTERN = re.compile(r"(?:\b[A-Z]-\d+\b|\b\d+-\d+\b|\b\d+\b)")
_DOT_LEADER_PATTERN = re.compile(r"\.{2,}")
_HEADING_LINE_PATTERN = re.compile(r"^##\s+.+$")
_WHITESPACE_PATTERN = re.compile(r"\s+")


@dataclass(frozen=True)
class SectionReviewabilityAssessment:
    """Whether a section title looks trustworthy enough for inference by default."""

    reviewable: bool
    reason_codes: list[str]
    messages: list[str]


@dataclass(frozen=True)
class RecordPromotionAssessment:
    """Whether an extracted record is strong enough to promote downstream."""

    promotable: bool
    reason_codes: list[str]
    messages: list[str]


def assess_section_reviewability(section: Section) -> SectionReviewabilityAssessment:
    """Return whether a section should reach expensive inference by default."""
    title = _normalize_title(section.title)
    if not title:
        return SectionReviewabilityAssessment(
            reviewable=False,
            reason_codes=["empty_title"],
            messages=["Section title is empty after normalization."],
        )
    content = _normalize_content(section.content)

    reason_codes: list[str] = []
    messages: list[str] = []

    if len(_alnum_characters(title)) < 2:
        reason_codes.append("near_empty_title")
        messages.append(f"Section title '{section.title}' is too short to be trustworthy.")

    if _SINGLE_LETTER_TITLE_PATTERN.fullmatch(title):
        reason_codes.append("single_letter_title")
        messages.append(f"Section title '{section.title}' is a single-letter fragment.")

    if _NUMERIC_FRAGMENT_PATTERN.fullmatch(title):
        reason_codes.append("numeric_fragment_title")
        messages.append(f"Section title '{section.title}' is a numeric fragment.")

    lowered = title.casefold()
    if lowered in _GENERIC_CARRYOVER_TITLES:
        reason_codes.append("generic_carryover_title")
        messages.append(f"Section title '{section.title}' is a generic carryover heading.")

    if lowered in _VAGUE_SECTION_TITLES and len(title.split()) <= 2:
        reason_codes.append("vague_title")
        messages.append(f"Section title '{section.title}' is too vague to stand on its own.")

    if _looks_like_table_of_contents_blob(title):
        reason_codes.append("toc_fragment_title")
        messages.append(f"Section title '{section.title}' looks like a table-of-contents fragment.")

    if _looks_like_table_of_contents_blob(content):
        reason_codes.append("toc_fragment_content")
        messages.append(f"Section '{section.title}' looks like a table-of-contents or index artifact.")

    if _looks_like_index_artifact(content):
        reason_codes.append("index_artifact_content")
        messages.append(f"Section '{section.title}' looks like an index-style artifact instead of source content.")

    return SectionReviewabilityAssessment(
        reviewable=not reason_codes,
        reason_codes=reason_codes,
        messages=messages,
    )


def assess_record_promotion(section: Section, record_type: str, record: object) -> RecordPromotionAssessment:
    """Return whether one extracted record is strong enough for downstream promotion."""
    if record_type == "spec_value" and isinstance(record, SpecValue):
        return _assess_spec_value_promotion(section, record)
    return RecordPromotionAssessment(promotable=True, reason_codes=[], messages=[])


def _normalize_title(title: str) -> str:
    return _WHITESPACE_PATTERN.sub(" ", title).strip()


def _normalize_content(content: str) -> str:
    lines = []
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped or _HEADING_LINE_PATTERN.match(stripped):
            continue
        lines.append(stripped)
    return "\n".join(lines)


def _alnum_characters(value: str) -> str:
    return "".join(character for character in value if character.isalnum())


def _looks_like_table_of_contents_blob(value: str) -> bool:
    if len(value) < 80:
        return False
    token_count = len(value.split())
    if token_count < 12:
        return False
    page_ref_count = len(_TOC_PAGE_REFERENCE_PATTERN.findall(value))
    dot_leader_count = len(_DOT_LEADER_PATTERN.findall(value))
    return page_ref_count >= 6 or (page_ref_count >= 3 and dot_leader_count >= 2)


def _looks_like_index_artifact(content: str) -> bool:
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    if len(lines) < 4:
        return False
    short_lines = sum(1 for line in lines if len(line.split()) <= 6)
    page_ref_lines = sum(
        1 for line in lines if _DOT_LEADER_PATTERN.search(line) or _TOC_PAGE_REFERENCE_PATTERN.search(line)
    )
    return short_lines >= 4 and page_ref_lines >= max(2, len(lines) // 2)


def _assess_spec_value_promotion(section: Section, record: SpecValue) -> RecordPromotionAssessment:
    reason_codes: list[str] = []
    messages: list[str] = []

    parameter = _normalize_title(record.parameter)
    normalized_parameter = _normalize_phrase(parameter)
    context_text = _normalize_phrase(
        " ".join(
            part
            for part in (
                section.title,
                record.parameter,
                record.value,
                record.unit or "",
                record.conditions or "",
            )
            if part
        )
    )

    if normalized_parameter in _LOW_SIGNAL_SPEC_TOKENS:
        reason_codes.append("single_token_dimension_fragment")
        messages.append(f"Specification parameter '{record.parameter}' looks like an orphaned dimension fragment.")

    if any(phrase in context_text for phrase in _LOW_SIGNAL_SPEC_PHRASES):
        reason_codes.append("low_signal_spec_fragment")
        messages.append(f"Specification '{record.parameter}' looks like file, encoding, or compliance noise.")

    if normalized_parameter in _GENERIC_CARRYOVER_TITLES or normalized_parameter in _VAGUE_SECTION_TITLES:
        reason_codes.append("generic_spec_parameter")
        messages.append(f"Specification parameter '{record.parameter}' is too generic to promote downstream.")

    return RecordPromotionAssessment(
        promotable=not reason_codes,
        reason_codes=reason_codes,
        messages=messages,
    )


def _normalize_phrase(value: str) -> str:
    return _WHITESPACE_PATTERN.sub(" ", re.sub(r"[^a-z0-9]+", " ", value.casefold())).strip()
