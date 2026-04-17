"""Contradiction-note compilation for bucket-scoped extracted records."""

from __future__ import annotations

import json
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
    ContradictionAnalysisReport,
    analyze_contradictions,
)
from knowledge_forge.extract.schemas.contradiction_candidate import REVIEW_STATUS_VALUES
from knowledge_forge.intake.importer import get_data_dir, list_manifests
from knowledge_forge.intake.manifest import slugify

COMPILATION_VERSION = "contradiction-notes-v1"


@dataclass(frozen=True)
class ContradictionNoteEntry:
    """One rendered contradiction plus optional supersession guidance."""

    candidate_record_ids: tuple[str, str]
    conflicting_claim: str
    rationale: str
    left: ComparableClaim
    right: ComparableClaim
    review_status: str
    supersession_confidence: str | None
    supersession_precedence_basis: str | None
    recommended_resolution: str

    @property
    def key(self) -> tuple[str, str, str]:
        return (
            self.left.subject_label.casefold(),
            self.left.record_id,
            self.right.record_id,
        )


@dataclass(frozen=True)
class ContradictionReviewDecision:
    """Persisted reviewer state for one contradiction candidate."""

    candidate_key: str
    record_ids: tuple[str, str]
    review_status: str
    reviewer: str | None
    reviewed_at: str | None
    notes: str | None


@dataclass(frozen=True)
class ContradictionReviewArtifacts:
    """Generated review markdown plus the sidecar decision template."""

    report_path: Path
    decision_path: Path
    decisions: list[ContradictionReviewDecision]


def render_contradiction_notes(bucket_id: str, *, data_dir: Path | None = None) -> list[CompiledPage]:
    """Compile one standalone contradiction summary page for a bucket."""
    resolved_data_dir = get_data_dir(data_dir)
    report = analyze_contradictions(bucket_id, data_dir=resolved_data_dir)
    entries = _build_note_entries(report)
    generated_at = _utc_timestamp()

    source_documents = _build_source_documents(bucket_id, data_dir=resolved_data_dir)
    extraction_versions_set: set[str] = {ANALYSIS_VERSION}
    parser_versions_set: set[str] = set()
    for entry in entries:
        for claim in (entry.left, entry.right):
            extraction_versions_set.add(claim.record.extraction_version)
            parser_versions_set.add(claim.record.parser_version)
    extraction_versions = sorted(extraction_versions_set)
    parser_versions = sorted(parser_versions_set)

    output_path = resolved_data_dir / "compiled" / "contradiction-notes" / f"{slugify(bucket_id)}.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frontmatter = {
        "title": f"Contradiction Notes: {bucket_id}",
        "generated_by": GENERATED_BY,
        "publish_run": PUBLISH_RUN_PLACEHOLDER,
        "source_documents": source_documents,
        "generated_at": generated_at,
        "extraction_version": ", ".join(extraction_versions),
        "analysis_version": ANALYSIS_VERSION,
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
                "analyzed_claims": len(report.claims),
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


def build_note_entries(bucket_id: str, *, data_dir: Path | None = None) -> list[ContradictionNoteEntry]:
    """Build contradiction note entries for one bucket (public API for batch callers)."""
    resolved_data_dir = get_data_dir(data_dir)
    report = analyze_contradictions(bucket_id, data_dir=resolved_data_dir)
    return _build_note_entries(report)


def render_contradiction_review_report(
    bucket_id: str,
    *,
    data_dir: Path | None = None,
) -> ContradictionReviewArtifacts:
    """Generate the contradiction review report and decision sidecar for one bucket."""
    resolved_data_dir = get_data_dir(data_dir)
    report = analyze_contradictions(bucket_id, data_dir=resolved_data_dir)
    entries = _build_note_entries(report)
    output_dir = resolved_data_dir / "compiled" / "contradiction-notes"
    output_dir.mkdir(parents=True, exist_ok=True)

    bucket_slug = slugify(bucket_id)
    report_path = output_dir / f"{bucket_slug}-review.md"
    decision_path = output_dir / f"{bucket_slug}-review-status.json"

    existing_decisions = _load_review_decisions(decision_path)
    decisions = _merge_review_decisions(entries, existing_decisions)

    report_path.write_text(_render_review_content(bucket_id, entries, decisions), encoding="utf-8")
    decision_payload = {
        "bucket_id": bucket_id,
        "updated_at": _utc_timestamp(),
        "candidates": [
            {
                "candidate_key": decision.candidate_key,
                "record_ids": list(decision.record_ids),
                "review_status": decision.review_status,
                "reviewer": decision.reviewer,
                "reviewed_at": decision.reviewed_at,
                "notes": decision.notes,
            }
            for decision in decisions
        ],
    }
    decision_path.write_text(json.dumps(decision_payload, indent=2), encoding="utf-8")
    return ContradictionReviewArtifacts(
        report_path=report_path,
        decision_path=decision_path,
        decisions=decisions,
    )


def render_inline_contradiction_notes(
    bucket_id: str,
    *,
    record_ids: set[str],
    data_dir: Path | None = None,
    entries: list[ContradictionNoteEntry] | None = None,
) -> list[str]:
    """Render inline contradiction callouts for one compiled topic page.

    Pass ``entries`` to reuse pre-computed entries for the bucket and avoid
    running contradiction analysis more than once per bucket compile run.
    """
    if entries is None:
        resolved_data_dir = get_data_dir(data_dir)
        report = analyze_contradictions(bucket_id, data_dir=resolved_data_dir)
        entries = _build_note_entries(report)
    lines: list[str] = []
    for entry in entries:
        candidate_ids = (entry.left.record_id, entry.right.record_id)
        if not any(_record_matches(record_id, record_ids) for record_id in candidate_ids):
            continue
        if lines:
            lines.append("")
        lines.extend(_render_inline_block(entry))
    return lines


def _build_note_entries(report: ContradictionAnalysisReport) -> list[ContradictionNoteEntry]:
    claims_by_id = {claim.record_id: claim for claim in report.claims}
    entries: list[ContradictionNoteEntry] = []
    for candidate in report.contradictions:
        matched_claims = [claims_by_id[record_id] for record_id in candidate.record_ids if record_id in claims_by_id]
        if len(matched_claims) < 2:
            continue
        left, right = sorted(matched_claims[:2], key=lambda claim: claim.record_id)
        supersession = candidate.supersession
        entries.append(
            ContradictionNoteEntry(
                candidate_record_ids=tuple(sorted(candidate.record_ids)),
                conflicting_claim=candidate.conflicting_claim,
                rationale=candidate.rationale,
                left=left,
                right=right,
                review_status=candidate.review_status,
                supersession_confidence=(supersession.confidence if supersession is not None else None),
                supersession_precedence_basis=(
                    supersession.precedence_rule_applied if supersession is not None else None
                ),
                recommended_resolution=_recommended_resolution(
                    left,
                    right,
                    supersession_precedence_basis=(
                        supersession.precedence_rule_applied if supersession is not None else None
                    ),
                ),
            )
        )
    return sorted(entries, key=lambda entry: entry.key)


def _load_review_decisions(decision_path: Path) -> dict[str, ContradictionReviewDecision]:
    if not decision_path.exists():
        return {}

    payload = json.loads(decision_path.read_text(encoding="utf-8"))
    decisions: dict[str, ContradictionReviewDecision] = {}
    for item in payload.get("candidates", []):
        record_ids = tuple(sorted(str(record_id).strip() for record_id in item.get("record_ids", [])))
        if len(record_ids) != 2 or any(not record_id for record_id in record_ids):
            continue
        decision = ContradictionReviewDecision(
            candidate_key=str(item.get("candidate_key") or _candidate_key(record_ids)),
            record_ids=record_ids,
            review_status=_sanitize_review_status(item.get("review_status")),
            reviewer=_optional_text(item.get("reviewer")),
            reviewed_at=_optional_text(item.get("reviewed_at")),
            notes=_optional_text(item.get("notes")),
        )
        decisions[decision.candidate_key] = decision
    return decisions


def _merge_review_decisions(
    entries: list[ContradictionNoteEntry],
    existing_decisions: dict[str, ContradictionReviewDecision],
) -> list[ContradictionReviewDecision]:
    decisions: list[ContradictionReviewDecision] = []
    for entry in entries:
        candidate_key = _candidate_key(entry.candidate_record_ids)
        existing = existing_decisions.get(candidate_key)
        decisions.append(
            ContradictionReviewDecision(
                candidate_key=candidate_key,
                record_ids=entry.candidate_record_ids,
                review_status=existing.review_status if existing is not None else entry.review_status,
                reviewer=existing.reviewer if existing is not None else None,
                reviewed_at=existing.reviewed_at if existing is not None else None,
                notes=existing.notes if existing is not None else None,
            )
        )
    return decisions


def _render_review_content(
    bucket_id: str,
    entries: list[ContradictionNoteEntry],
    decisions: list[ContradictionReviewDecision],
) -> str:
    lines = [f"# Contradiction Review for {bucket_id}", ""]
    if not entries:
        lines.extend(
            [
                "No contradiction candidates were found for this bucket.",
                "",
                "The review-status sidecar still exists so future reviewer decisions have a stable path.",
            ]
        )
        return "\n".join(lines).rstrip()

    decision_by_key = {decision.candidate_key: decision for decision in decisions}
    lines.extend(
        [
            (
                "This report is review-ready: it shows the competing claims, source document "
                "types, page references, supersession guidance, and the persisted review "
                "status placeholder for each candidate."
            ),
            "",
            "Accepted review statuses: `unreviewed`, `approved`, `rejected`, `deferred`.",
            "",
        ]
    )
    for index, entry in enumerate(entries, start=1):
        decision = decision_by_key[_candidate_key(entry.candidate_record_ids)]
        lines.extend(
            [
                f"## Candidate {index}: {entry.left.subject_label}",
                "",
                f"- Candidate key: `{decision.candidate_key}`",
                f"- Review status: `{decision.review_status}`",
            ]
        )
        if _is_unresolved(entry):
            lines.extend(
                [
                    "",
                    "> [!IMPORTANT] Review Required",
                    f"> {_unresolved_reason(entry)}",
                ]
            )
        lines.extend(
            [
                "",
                "### Conflict Summary",
                "",
                f"- {entry.conflicting_claim}",
                f"- {entry.rationale}",
                "",
                "### Compared Claims",
                "",
                _review_claim_line("Claim A", entry.left),
                _review_claim_line("Claim B", entry.right),
                "",
                "### Supersession Assessment",
                "",
                f"- Confidence: `{entry.supersession_confidence or 'none'}`",
                f"- Rule applied: {_precedence_text(entry.left, entry.right, entry.supersession_precedence_basis)}",
                f"- Recommended resolution: {entry.recommended_resolution}",
                "",
                "### Reviewer Notes",
                "",
                f"- Reviewer: {decision.reviewer or 'unassigned'}",
                f"- Reviewed at: {decision.reviewed_at or 'not recorded'}",
                f"- Notes: {decision.notes or 'none'}",
                "",
            ]
        )
    return "\n".join(lines).rstrip()


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


def _review_claim_line(label: str, claim: ComparableClaim) -> str:
    citation = _page_label(claim)
    return (
        f"- {label}: `{claim.claim_text}` from `{claim.doc_id}` "
        f"(`{claim.document_type}`, {citation}, {claim.precedence_label}, level {claim.precedence_level})"
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


def _candidate_key(record_ids: tuple[str, str]) -> str:
    return "||".join(record_ids)


def _sanitize_review_status(value: object) -> str:
    normalized = str(value or "").strip().casefold()
    if normalized in REVIEW_STATUS_VALUES:
        return normalized
    return "unreviewed"


def _optional_text(value: object) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _is_unresolved(entry: ContradictionNoteEntry) -> bool:
    return entry.supersession_confidence == "low" or entry.left.precedence_level == entry.right.precedence_level


def _unresolved_reason(entry: ContradictionNoteEntry) -> str:
    if entry.supersession_confidence == "low":
        return "Supersession confidence is low, so reviewer approval is required before downstream use."
    return (
        "Both sides share the same precedence tier, so this contradiction should stay "
        "in human review even if a revision ordering hint exists."
    )


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
