"""Contradiction-note compilation for bucket-scoped extracted records."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from knowledge_forge.compile.source_pages import (
    GENERATED_BY,
    PUBLISH_RUN_PLACEHOLDER,
    CompiledPage,
    CompileMetadata,
)
from knowledge_forge.extract.contradiction import (
    ANALYSIS_VERSION,
    ComparableClaim,
    _load_comparable_claims,
    analyze_contradictions,
)
from knowledge_forge.intake.importer import get_data_dir, list_manifests
from knowledge_forge.intake.manifest import slugify

COMPILATION_VERSION = "contradiction-notes-v1"


@dataclass(frozen=True)
class ContradictionNoteEntry:
    """One rendered contradiction plus optional supersession guidance."""

    conflicting_claim: str
    rationale: str
    left: ComparableClaim
    right: ComparableClaim
    supersession_precedence_basis: str | None
    recommended_resolution: str

    @property
    def key(self) -> tuple[str, str, str]:
        return (
            self.left.subject_label.casefold(),
            self.left.record_id,
            self.right.record_id,
        )


def render_contradiction_notes(bucket_id: str, *, data_dir: Path | None = None) -> list[CompiledPage]:
    """Compile one standalone contradiction summary page for a bucket."""
    resolved_data_dir = get_data_dir(data_dir)
    entries = _build_note_entries(bucket_id, data_dir=resolved_data_dir)
    generated_at = _utc_timestamp()

    claims = {
        claim.record_id: claim
        for claim in _load_comparable_claims(bucket_id, data_dir=resolved_data_dir)
    }
    source_documents = _build_source_documents(bucket_id, data_dir=resolved_data_dir)
    extraction_versions = sorted(
        {
            claim.record.extraction_version
            for entry in entries
            for claim in (entry.left, entry.right)
        }
        or {ANALYSIS_VERSION}
    )
    parser_versions = sorted(
        {
            claim.record.parser_version
            for entry in entries
            for claim in (entry.left, entry.right)
        }
    )

    output_path = resolved_data_dir / "compiled" / "contradiction-notes" / f"{slugify(bucket_id)}.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frontmatter = {
        "title": f"Contradiction Notes: {bucket_id}",
        "generated_by": GENERATED_BY,
        "publish_run": PUBLISH_RUN_PLACEHOLDER,
        "source_documents": source_documents,
        "generated_at": generated_at,
        "extraction_version": ", ".join(extraction_versions),
        "compilation_version": COMPILATION_VERSION,
        "bucket_id": bucket_id,
        "status": "compiled",
    }
    page = CompiledPage(
        output_path=output_path,
        doc_id=bucket_id,
        frontmatter=frontmatter,
        content=_render_standalone_content(bucket_id, entries),
        compile_metadata=CompileMetadata(
            generated_at=generated_at,
            extraction_versions=extraction_versions,
            parser_versions=parser_versions,
            record_counts={
                "contradiction_candidate": len(entries),
                "source_documents": len(source_documents),
                "analyzed_claims": len(claims),
            },
            review_flag_count=0,
        ),
    )
    output_path.write_text(page.render(), encoding="utf-8")
    return [page]


def compile_all_contradiction_notes(*, data_dir: Path | None = None) -> list[CompiledPage]:
    """Compile contradiction-note pages for every extracted bucket."""
    resolved_data_dir = get_data_dir(data_dir)
    pages: list[CompiledPage] = []
    for bucket_id in _discover_bucket_ids(resolved_data_dir):
        pages.extend(render_contradiction_notes(bucket_id, data_dir=resolved_data_dir))
    return pages


def render_inline_contradiction_notes(
    bucket_id: str,
    *,
    record_ids: set[str],
    data_dir: Path | None = None,
) -> list[str]:
    """Render inline contradiction callouts for one compiled topic page."""
    resolved_data_dir = get_data_dir(data_dir)
    lines: list[str] = []
    for entry in _build_note_entries(bucket_id, data_dir=resolved_data_dir):
        candidate_ids = (entry.left.record_id, entry.right.record_id)
        if not any(_record_matches(record_id, record_ids) for record_id in candidate_ids):
            continue
        if lines:
            lines.append("")
        lines.extend(_render_inline_block(entry))
    return lines


def _build_note_entries(bucket_id: str, *, data_dir: Path) -> list[ContradictionNoteEntry]:
    report = analyze_contradictions(bucket_id, data_dir=data_dir)
    claims_by_id = {
        claim.record_id: claim
        for claim in _load_comparable_claims(bucket_id, data_dir=data_dir)
    }
    supersession_by_pair = {
        frozenset((candidate.superseding_record_id, candidate.superseded_record_id)): candidate
        for candidate in report.supersessions
    }

    entries: list[ContradictionNoteEntry] = []
    for candidate in report.contradictions:
        matched_claims = [claims_by_id[record_id] for record_id in candidate.record_ids if record_id in claims_by_id]
        if len(matched_claims) < 2:
            continue
        left, right = sorted(matched_claims[:2], key=lambda claim: claim.record_id)
        supersession = supersession_by_pair.get(frozenset(candidate.record_ids))
        entries.append(
            ContradictionNoteEntry(
                conflicting_claim=candidate.conflicting_claim,
                rationale=candidate.rationale,
                left=left,
                right=right,
                supersession_precedence_basis=(
                    supersession.precedence_basis if supersession is not None else None
                ),
                recommended_resolution=_recommended_resolution(left, right, supersession_precedence_basis=(
                    supersession.precedence_basis if supersession is not None else None
                )),
            )
        )
    return sorted(entries, key=lambda entry: entry.key)


def _render_standalone_content(bucket_id: str, entries: list[ContradictionNoteEntry]) -> str:
    lines = [f"# Contradiction Notes for {bucket_id}", ""]
    if not entries:
        lines.extend(
            [
                "No contradiction candidates were found for this bucket.",
                "",
                "This page is still generated so publish validation has a stable artifact path.",
            ]
        )
        return "\n".join(lines).rstrip()

    lines.extend(
        [
            (
                "These notes surface competing claims, source document types, "
                "precedence assessment, and the recommended review action."
            ),
            "",
        ]
    )
    for index, entry in enumerate(entries, start=1):
        lines.extend(
            [
                f"## Candidate {index}: {entry.left.subject_label}",
                "",
                f"- Conflict summary: {entry.conflicting_claim}",
                f"- Rationale: {entry.rationale}",
                f"- Claim A: {_claim_line(entry.left)}",
                f"- Claim B: {_claim_line(entry.right)}",
                (
                    "- Precedence assessment: "
                    f"{_precedence_text(entry.left, entry.right, entry.supersession_precedence_basis)}"
                ),
                f"- Recommended resolution: {entry.recommended_resolution}",
                "",
            ]
        )
    return "\n".join(lines).rstrip()


def _render_inline_block(entry: ContradictionNoteEntry) -> list[str]:
    return [
        "> [!WARNING] Contradiction",
        f"> {_claim_line(entry.left)}",
        f"> {_claim_line(entry.right)}",
        f"> Precedence: {_precedence_text(entry.left, entry.right, entry.supersession_precedence_basis)}",
        f"> Recommended resolution: {entry.recommended_resolution}",
    ]


def _claim_line(claim: ComparableClaim) -> str:
    citation = _page_label(claim)
    return (
        f"`{claim.claim_text}` from `{claim.doc_id}` "
        f"({claim.document_type}, {citation}, {claim.precedence_label}, level {claim.precedence_level})"
    )


def _precedence_text(
    left: ComparableClaim,
    right: ComparableClaim,
    supersession_precedence_basis: str | None,
) -> str:
    if supersession_precedence_basis is not None:
        return supersession_precedence_basis
    if left.precedence_level == right.precedence_level:
        return (
            f"same precedence tier: {left.precedence_label} (level {left.precedence_level}) "
            f"for both `{left.document_type}` and `{right.document_type}`"
        )
    preferred, deferred = sorted((left, right), key=lambda claim: claim.precedence_level)
    return (
        f"{preferred.precedence_label} (level {preferred.precedence_level}) outranks "
        f"{deferred.precedence_label} (level {deferred.precedence_level})"
    )


def _recommended_resolution(
    left: ComparableClaim,
    right: ComparableClaim,
    *,
    supersession_precedence_basis: str | None,
) -> str:
    if supersession_precedence_basis is None or left.precedence_level == right.precedence_level:
        return "Hold for human review because the precedence signal is ambiguous."
    preferred, deferred = sorted((left, right), key=lambda claim: claim.precedence_level)
    return (
        f"Prefer `{preferred.doc_id}` ({preferred.document_type}) over "
        f"`{deferred.doc_id}` ({deferred.document_type}) unless reviewer context overrides the precedence rule."
    )


def _page_label(claim: ComparableClaim) -> str:
    start_page = claim.record.source_page_range.start_page
    end_page = claim.record.source_page_range.end_page
    if start_page == end_page:
        return f"p.{start_page}"
    return f"pp.{start_page}-{end_page}"


def _record_matches(candidate_record_id: str, record_ids: set[str]) -> bool:
    if candidate_record_id in record_ids:
        return True
    return any(candidate_record_id.startswith(f"{record_id}::") for record_id in record_ids)


def _utc_timestamp() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _build_source_documents(bucket_id: str, *, data_dir: Path) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    for manifest in list_manifests(data_dir):
        if bucket_id not in {assignment.bucket_id for assignment in manifest.bucket_assignments}:
            continue
        document = manifest.document
        documents.append(
            {
                "doc_id": manifest.doc_id,
                "revision": document.revision,
                "manufacturer": document.manufacturer,
                "family": document.family,
                "document_type": document.document_type,
            }
        )
    return sorted(documents, key=lambda item: str(item["doc_id"]))


def _discover_bucket_ids(data_dir: Path) -> list[str]:
    bucket_ids: set[str] = set()
    for manifest in list_manifests(data_dir):
        if not (data_dir / "extracted" / manifest.doc_id).exists():
            continue
        bucket_ids.update(assignment.bucket_id for assignment in manifest.bucket_assignments)
    return sorted(bucket_ids)
