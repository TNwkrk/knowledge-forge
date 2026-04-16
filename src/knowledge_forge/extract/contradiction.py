"""Bucket-scoped contradiction and supersession analysis."""

from __future__ import annotations

import re
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path
from typing import Literal

from knowledge_forge.extract.schemas import (
    AlarmDefinition,
    Applicability,
    ContradictionCandidate,
    ExtractionSchemaModel,
    Procedure,
    SpecValue,
    SupersessionCandidate,
    Warning,
    get_schema_model,
)
from knowledge_forge.inference import InferenceClient
from knowledge_forge.intake.importer import get_data_dir, list_manifests
from knowledge_forge.intake.manifest import ManifestEntry, slugify

ANALYSIS_VERSION = "contradiction-analysis@v1"
_STOPWORDS = {
    "a",
    "an",
    "and",
    "at",
    "be",
    "for",
    "from",
    "in",
    "into",
    "is",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
}
_NEGATIONS = {"no", "not", "never", "without", "avoid", "do-not", "don't"}

ClaimType = Literal["spec_value", "procedure_step", "warning", "alarm_definition"]


@dataclass(frozen=True)
class ApplicabilityScope:
    """Normalized applicability data used for comparison gating."""

    manufacturer: str
    family: str
    models: frozenset[str]
    revision: str | None = None


@dataclass(frozen=True)
class ComparableClaim:
    """One contradiction-comparable claim derived from extracted records."""

    record_id: str
    claim_type: ClaimType
    manifest: ManifestEntry
    record: ExtractionSchemaModel
    subject_key: str
    subject_label: str
    claim_text: str
    semantic_signature: str
    applicability: ApplicabilityScope
    precedence_level: int
    precedence_label: str

    @property
    def document_type(self) -> str:
        return self.manifest.document.document_type

    @property
    def doc_id(self) -> str:
        return self.manifest.doc_id


@dataclass(frozen=True)
class ContradictionAnalysisReport:
    """Combined contradiction and supersession output for one bucket."""

    bucket_id: str
    contradictions: list[ContradictionCandidate]
    supersessions: list[SupersessionCandidate]


def find_contradiction_candidates(
    bucket_id: str,
    *,
    client: InferenceClient | None = None,
    data_dir: Path | None = None,
) -> list[ContradictionCandidate]:
    """Return contradiction candidates for one bucket."""
    return analyze_contradictions(bucket_id, client=client, data_dir=data_dir).contradictions


def analyze_contradictions(
    bucket_id: str,
    *,
    client: InferenceClient | None = None,
    data_dir: Path | None = None,
) -> ContradictionAnalysisReport:
    """Analyze one bucket for contradiction and supersession candidates."""
    resolved_data_dir = get_data_dir(data_dir)
    claims = _load_comparable_claims(bucket_id, data_dir=resolved_data_dir)

    contradictions: list[ContradictionCandidate] = []
    supersessions: list[SupersessionCandidate] = []
    seen_pairs: set[tuple[str, str]] = set()

    for left, right in combinations(claims, 2):
        if not _claims_are_comparable(left, right):
            continue

        pair_key = tuple(sorted((left.record_id, right.record_id)))
        if pair_key in seen_pairs:
            continue

        if not _claims_contradict(left, right, client=client):
            continue

        seen_pairs.add(pair_key)
        contradictions.append(_build_contradiction_candidate(bucket_id, left, right))
        supersession = _build_supersession_candidate(bucket_id, left, right)
        if supersession is not None:
            supersessions.append(supersession)

    contradictions.sort(key=lambda candidate: tuple(candidate.record_ids))
    supersessions.sort(key=lambda candidate: (candidate.superseding_record_id, candidate.superseded_record_id))
    return ContradictionAnalysisReport(
        bucket_id=bucket_id,
        contradictions=contradictions,
        supersessions=supersessions,
    )


def _load_comparable_claims(bucket_id: str, *, data_dir: Path) -> list[ComparableClaim]:
    claims: list[ComparableClaim] = []
    for manifest in list_manifests(data_dir):
        if bucket_id not in {assignment.bucket_id for assignment in manifest.bucket_assignments}:
            continue

        extracted_dir = data_dir / "extracted" / manifest.doc_id
        if not extracted_dir.exists():
            continue

        precedence_level, precedence_label = _document_precedence(manifest)
        for record_dir in sorted(path for path in extracted_dir.iterdir() if path.is_dir() and path.name != "reviews"):
            model = get_schema_model(record_dir.name)
            for record_path in sorted(record_dir.glob("*.json")):
                record = model.model_validate_json(record_path.read_text(encoding="utf-8"))
                if bucket_id not in {context.bucket_id for context in record.bucket_context}:
                    continue
                claims.extend(
                    _expand_comparable_claims(
                        record_path.stem,
                        record,
                        manifest=manifest,
                        precedence_level=precedence_level,
                        precedence_label=precedence_label,
                    )
                )
    return sorted(claims, key=lambda claim: (claim.claim_type, claim.subject_key, claim.doc_id, claim.record_id))


def _expand_comparable_claims(
    record_id: str,
    record: ExtractionSchemaModel,
    *,
    manifest: ManifestEntry,
    precedence_level: int,
    precedence_label: str,
) -> list[ComparableClaim]:
    applicability = _applicability_scope(record, manifest)
    claims: list[ComparableClaim] = []

    if isinstance(record, SpecValue):
        claims.append(
            ComparableClaim(
                record_id=record_id,
                claim_type="spec_value",
                manifest=manifest,
                record=record,
                subject_key=slugify(record.parameter),
                subject_label=record.parameter,
                claim_text=_render_spec_claim(record),
                semantic_signature="|".join(
                    part
                    for part in (
                        record.value.casefold(),
                        (record.unit or "").casefold(),
                        (record.conditions or "").casefold(),
                    )
                    if part
                ),
                applicability=applicability,
                precedence_level=precedence_level,
                precedence_label=precedence_label,
            )
        )
        return claims

    if isinstance(record, AlarmDefinition):
        claims.append(
            ComparableClaim(
                record_id=record_id,
                claim_type="alarm_definition",
                manifest=manifest,
                record=record,
                subject_key=record.code.casefold(),
                subject_label=record.code,
                claim_text=(
                    f"{record.description}; cause {record.cause}; "
                    f"remedy {record.remedy}; severity {record.severity}"
                ),
                semantic_signature="|".join(
                    (
                        record.description.casefold(),
                        record.cause.casefold(),
                        record.remedy.casefold(),
                        record.severity.casefold(),
                    )
                ),
                applicability=applicability,
                precedence_level=precedence_level,
                precedence_label=precedence_label,
            )
        )
        return claims

    if isinstance(record, Warning):
        warning_subject = record.context or record.source_heading
        claims.append(
            ComparableClaim(
                record_id=record_id,
                claim_type="warning",
                manifest=manifest,
                record=record,
                subject_key=slugify(warning_subject),
                subject_label=warning_subject,
                claim_text=f"{record.severity}: {record.text}",
                semantic_signature=f"{record.severity.casefold()}|{record.text.casefold()}",
                applicability=_applicability_scope(record.applicability or record, manifest),
                precedence_level=precedence_level,
                precedence_label=precedence_label,
            )
        )
        return claims

    if not isinstance(record, Procedure):
        return claims

    procedure_scope = _applicability_scope(record.applicability or record, manifest)
    for index, step in enumerate(record.steps, start=1):
        step_id = f"{record_id}::step-{index:03d}"
        claims.append(
            ComparableClaim(
                record_id=step_id,
                claim_type="procedure_step",
                manifest=manifest,
                record=step,
                subject_key=f"{slugify(record.title)}::step-{step.step_number:03d}",
                subject_label=f"{record.title} step {step.step_number}",
                claim_text=step.instruction,
                semantic_signature=step.instruction.casefold(),
                applicability=procedure_scope,
                precedence_level=precedence_level,
                precedence_label=precedence_label,
            )
        )
    for index, warning in enumerate(record.warnings, start=1):
        warning_id = f"{record_id}::warning-{index:03d}"
        warning_subject = warning.context or record.title
        claims.append(
            ComparableClaim(
                record_id=warning_id,
                claim_type="warning",
                manifest=manifest,
                record=warning,
                subject_key=slugify(warning_subject),
                subject_label=warning_subject,
                claim_text=f"{warning.severity}: {warning.text}",
                semantic_signature=f"{warning.severity.casefold()}|{warning.text.casefold()}",
                applicability=_applicability_scope(warning.applicability or record.applicability or warning, manifest),
                precedence_level=precedence_level,
                precedence_label=precedence_label,
            )
        )
    return claims


def _claims_are_comparable(left: ComparableClaim, right: ComparableClaim) -> bool:
    if left.claim_type != right.claim_type:
        return False
    if left.subject_key != right.subject_key:
        return False
    if not _applicability_overlaps(left.applicability, right.applicability):
        return False
    return True


def _claims_contradict(left: ComparableClaim, right: ComparableClaim, *, client: InferenceClient | None) -> bool:
    if left.semantic_signature == right.semantic_signature:
        return False

    if left.claim_type in {"spec_value", "alarm_definition"}:
        return True

    llm_result = _llm_compare(left, right, client=client)
    if llm_result is not None:
        return llm_result

    return _heuristic_text_contradiction(left.claim_text, right.claim_text)


def _build_contradiction_candidate(
    bucket_id: str,
    left: ComparableClaim,
    right: ComparableClaim,
) -> ContradictionCandidate:
    authoritative, secondary = _preferred_claim_order(left, right)
    return ContradictionCandidate(
        source_doc_id=authoritative.record.source_doc_id,
        source_page_range=authoritative.record.source_page_range,
        source_heading=authoritative.record.source_heading,
        parser_version=authoritative.record.parser_version,
        extraction_version=ANALYSIS_VERSION,
        confidence=round(min(left.record.confidence, right.record.confidence), 3),
        bucket_context=[context for context in authoritative.record.bucket_context if context.bucket_id == bucket_id]
        or authoritative.record.bucket_context,
        record_ids=[left.record_id, right.record_id],
        conflicting_claim=f"{left.subject_label} differs between {left.document_type} and {right.document_type}.",
        rationale=(
            f"{left.doc_id} claims '{left.claim_text}' while {right.doc_id} claims '{right.claim_text}'. "
            f"Both apply to overlapping records in bucket {bucket_id}."
        ),
        review_status="pending",
    )


def _build_supersession_candidate(
    bucket_id: str,
    left: ComparableClaim,
    right: ComparableClaim,
) -> SupersessionCandidate | None:
    if left.precedence_level == right.precedence_level:
        return None

    superseding, superseded = _preferred_claim_order(left, right)
    return SupersessionCandidate(
        source_doc_id=superseding.record.source_doc_id,
        source_page_range=superseding.record.source_page_range,
        source_heading=superseding.record.source_heading,
        parser_version=superseding.record.parser_version,
        extraction_version=ANALYSIS_VERSION,
        confidence=round(min(left.record.confidence, right.record.confidence), 3),
        bucket_context=[context for context in superseding.record.bucket_context if context.bucket_id == bucket_id]
        or superseding.record.bucket_context,
        superseding_record_id=superseding.record_id,
        superseded_record_id=superseded.record_id,
        rationale=(
            f"{superseding.document_type} outranks {superseded.document_type} for "
            f"{superseding.subject_label} in bucket {bucket_id}."
        ),
        precedence_basis=(
            f"{superseding.precedence_label} (level {superseding.precedence_level}) outranks "
            f"{superseded.precedence_label} (level {superseded.precedence_level})"
        ),
    )


def _preferred_claim_order(left: ComparableClaim, right: ComparableClaim) -> tuple[ComparableClaim, ComparableClaim]:
    if left.precedence_level != right.precedence_level:
        return (left, right) if left.precedence_level < right.precedence_level else (right, left)
    return (left, right) if left.record_id <= right.record_id else (right, left)


def _document_precedence(manifest: ManifestEntry) -> tuple[int, str]:
    document = manifest.document
    document_type = slugify(document.document_type)
    document_class = document.document_class.casefold()

    if document_class == "contextual":
        return 8, "non-authoritative material"
    if document_type in {"service-bulletin", "bulletin", "addendum"}:
        return 1, "service bulletin or addendum"
    if document_type in {"installation-manual", "operation-manual", "service-manual"}:
        if _is_original_revision(document.revision):
            return 3, "original manual"
        return 2, "revised manual"
    if document_type in {"datasheet", "specification-sheet", "selection-guide", "certification"}:
        return 4, "OEM datasheet or specification sheet"
    if document_type in {
        "startup-procedure",
        "shutdown-procedure",
        "winterization-procedure",
        "pm-procedure",
        "sop",
        "checklist",
        "safety-procedure",
        "loto-sheet",
        "permit-reference",
        "field-form",
        "inspection-template",
        "commissioning-sheet",
        "best-practice",
    } or document_class == "operational":
        return 5, "internal SOP or best practice"
    if document_type in {"quick-start", "supplemental-guide"}:
        return 6, "quick start or supplemental guide"
    if document_type in {"training-material", "technician-reference"}:
        return 7, "training material or technician reference"
    return 8, "non-authoritative material"


def _is_original_revision(revision: str) -> bool:
    normalized = revision.strip().casefold()
    if normalized in {"original", "initial", "first-edition", "first edition"}:
        return True
    return normalized in {"rev 0", "rev0", "revision 0", "r0", "a", "rev a", "revision a"}


def _applicability_scope(
    record_or_applicability: ExtractionSchemaModel | Applicability,
    manifest: ManifestEntry,
) -> ApplicabilityScope:
    if isinstance(record_or_applicability, Applicability):
        return ApplicabilityScope(
            manufacturer=record_or_applicability.manufacturer.casefold(),
            family=record_or_applicability.family.casefold(),
            models=frozenset(model.casefold() for model in record_or_applicability.models),
            revision=(record_or_applicability.revision or None),
        )

    document = manifest.document
    return ApplicabilityScope(
        manufacturer=document.manufacturer.casefold(),
        family=document.family.casefold(),
        models=frozenset(model.casefold() for model in document.model_applicability),
        revision=document.revision,
    )


def _applicability_overlaps(left: ApplicabilityScope, right: ApplicabilityScope) -> bool:
    if left.manufacturer != right.manufacturer or left.family != right.family:
        return False
    if left.models and right.models and not (left.models & right.models):
        return False
    return True


def _render_spec_claim(record: SpecValue) -> str:
    unit = f" {record.unit}" if record.unit else ""
    conditions = f" ({record.conditions})" if record.conditions else ""
    return f"{record.parameter}: {record.value}{unit}{conditions}"


def _llm_compare(left: ComparableClaim, right: ComparableClaim, *, client: InferenceClient | None) -> bool | None:
    if client is None:
        return None

    if left.claim_type not in {"procedure_step", "warning"}:
        return None

    prompt = (
        "Decide whether these two bucket-scoped technical claims are contradictory.\n"
        "Return only YES or NO.\n\n"
        f"Claim 1 ({left.document_type}): {left.claim_text}\n"
        f"Claim 2 ({right.document_type}): {right.claim_text}\n"
        f"Subject: {left.subject_label}\n"
    )
    try:
        result = client.complete(
            prompt=prompt,
            system="You compare technical instructions and warnings for contradiction. Return only YES or NO.",
            prompt_template="analysis/contradiction_compare",
            source_doc_id=left.doc_id,
            source_section_id=left.subject_key,
        )
    except Exception:
        return None

    normalized = result.response_text.strip().casefold()
    if normalized.startswith("yes"):
        return True
    if normalized.startswith("no"):
        return False
    return None


def _heuristic_text_contradiction(left_text: str, right_text: str) -> bool:
    left_tokens = _meaningful_tokens(left_text)
    right_tokens = _meaningful_tokens(right_text)
    if not left_tokens or not right_tokens:
        return left_text.casefold() != right_text.casefold()

    overlap = len(left_tokens & right_tokens)
    union = len(left_tokens | right_tokens)
    similarity = overlap / union if union else 0.0
    negation_flip = bool(left_tokens & _NEGATIONS) != bool(right_tokens & _NEGATIONS)
    return negation_flip or similarity >= 0.35


def _meaningful_tokens(value: str) -> set[str]:
    tokens = {
        token
        for token in re.findall(r"[a-z0-9]+", value.casefold())
        if token not in _STOPWORDS and len(token) > 1
    }
    return tokens
