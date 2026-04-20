"""Section-level reviewability heuristics used before expensive inference."""

from __future__ import annotations

import re
from dataclasses import dataclass

from knowledge_forge.parse.sectioning import Section

_GENERIC_CARRYOVER_TITLES = {
    "attention",
    "caution",
    "continued",
    "danger",
    "hazard",
    "important",
    "important user information",
    "notes",
    "notes:",
    "tip",
    "warning",
}
_SINGLE_LETTER_TITLE_PATTERN = re.compile(r"^[A-Z]$", re.IGNORECASE)
_NUMERIC_FRAGMENT_PATTERN = re.compile(r"^(?:\d+|(?:\d+\.)+|\d+\.)$")
_TOC_PAGE_REFERENCE_PATTERN = re.compile(r"(?:\b[A-Z]-\d+\b|\b\d+-\d+\b|\b\d+\b)")
_WHITESPACE_PATTERN = re.compile(r"\s+")


@dataclass(frozen=True)
class SectionReviewabilityAssessment:
    """Whether a section title looks trustworthy enough for inference by default."""

    reviewable: bool
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

    reason_codes: list[str] = []
    messages: list[str] = []

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

    if _looks_like_table_of_contents_blob(title):
        reason_codes.append("toc_fragment_title")
        messages.append(f"Section title '{section.title}' looks like a table-of-contents fragment.")

    return SectionReviewabilityAssessment(
        reviewable=not reason_codes,
        reason_codes=reason_codes,
        messages=messages,
    )


def _normalize_title(title: str) -> str:
    return _WHITESPACE_PATTERN.sub(" ", title).strip()


def _looks_like_table_of_contents_blob(title: str) -> bool:
    if len(title) < 80:
        return False
    token_count = len(title.split())
    if token_count < 12:
        return False
    page_ref_count = len(_TOC_PAGE_REFERENCE_PATTERN.findall(title))
    return page_ref_count >= 6
