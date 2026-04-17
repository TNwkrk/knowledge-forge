"""Extraction evaluation helpers for benchmark fixture sets."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean

from pydantic import BaseModel, Field

from knowledge_forge.extract.provenance import validate_record_provenance
from knowledge_forge.extract.schemas import ExtractionSchemaModel, get_schema_model
from knowledge_forge.intake.manifest import slugify

PROVENANCE_FIELDS = {
    "source_doc_id",
    "source_page_range",
    "source_heading",
    "parser_version",
    "extraction_version",
    "confidence",
    "bucket_context",
}


class ExpectedExtractionFixture(BaseModel):
    """Ground-truth extraction records for one committed benchmark fixture."""

    fixture_id: str
    title: str
    source_pdf: str
    expected_records: dict[str, list[dict[str, object]]] = Field(default_factory=dict)


class ConfidenceDistribution(BaseModel):
    """Distribution summary for extracted-record confidence scores."""

    total_records: int = Field(ge=0)
    min_confidence: float = Field(ge=0.0, le=1.0)
    max_confidence: float = Field(ge=0.0, le=1.0)
    mean_confidence: float = Field(ge=0.0, le=1.0)
    low_count: int = Field(ge=0)
    medium_count: int = Field(ge=0)
    high_count: int = Field(ge=0)


class ExtractionEvalMetrics(BaseModel):
    """Stable metric surface for extraction evaluation reports."""

    record_count_accuracy: float = Field(ge=0, le=100)
    field_accuracy: dict[str, float] = Field(default_factory=dict)
    provenance_completeness: float = Field(ge=0, le=100)
    schema_compliance_rate: float = Field(ge=0, le=100)
    confidence_distribution: ConfidenceDistribution


class ExtractionFixtureScore(BaseModel):
    """Per-fixture extraction evaluation result."""

    fixture_id: str
    title: str
    source_pdf: str
    extraction_versions: list[str] = Field(default_factory=list)
    metrics: ExtractionEvalMetrics
    overall_score: float = Field(ge=0, le=100)
    expected_record_counts: dict[str, int] = Field(default_factory=dict)
    actual_record_counts: dict[str, int] = Field(default_factory=dict)


class ExtractionEvalReport(BaseModel):
    """Aggregate extraction evaluation report for one committed fixture set."""

    fixture_set: str
    extraction_versions: list[str] = Field(default_factory=list)
    generated_at: str
    report_schema_version: str = "1.0"
    metrics: ExtractionEvalMetrics
    overall_score: float = Field(ge=0, le=100)
    fixture_reports: list[ExtractionFixtureScore] = Field(default_factory=list)


class _ActualRecordArtifact(BaseModel):
    record_type: str
    path: str
    payload: dict[str, object]
    schema_valid: bool
    provenance_valid: bool
    record: ExtractionSchemaModel | None = None


def evaluate_extraction(fixture_set: str) -> ExtractionEvalReport:
    """Evaluate committed extraction artifacts for one fixture set."""
    fixture_root = _fixture_set_root(fixture_set)
    fixture_reports: list[ExtractionFixtureScore] = []
    extraction_versions: set[str] = set()
    aggregate_confidences: list[float] = []

    for fixture_dir in sorted(path for path in fixture_root.iterdir() if path.is_dir()):
        ground_truth = ExpectedExtractionFixture.model_validate_json(
            (fixture_dir / "ground_truth.json").read_text(encoding="utf-8")
        )
        if ground_truth.fixture_id != fixture_dir.name:
            raise ValueError(
                f"fixture_id mismatch in '{fixture_dir}': "
                f"ground_truth.fixture_id={ground_truth.fixture_id!r} "
                f"but directory name is {fixture_dir.name!r}"
            )

        actual_records = _load_fixture_records(fixture_dir)
        for artifact in actual_records:
            if artifact.record is not None:
                extraction_versions.add(artifact.record.extraction_version)
                aggregate_confidences.append(artifact.record.confidence)

        field_accuracy = _score_field_accuracy(ground_truth.expected_records, actual_records)
        metrics = ExtractionEvalMetrics(
            record_count_accuracy=_score_record_count_accuracy(ground_truth.expected_records, actual_records),
            field_accuracy=field_accuracy,
            provenance_completeness=_score_provenance_completeness(actual_records),
            schema_compliance_rate=_score_schema_compliance(actual_records),
            confidence_distribution=_confidence_distribution(actual_records),
        )
        fixture_reports.append(
            ExtractionFixtureScore(
                fixture_id=ground_truth.fixture_id,
                title=ground_truth.title,
                source_pdf=ground_truth.source_pdf,
                extraction_versions=sorted(
                    {artifact.record.extraction_version for artifact in actual_records if artifact.record is not None}
                ),
                metrics=metrics,
                overall_score=_overall_score(metrics),
                expected_record_counts={
                    record_type: len(records) for record_type, records in sorted(ground_truth.expected_records.items())
                },
                actual_record_counts=_record_counts(actual_records),
            )
        )

    if not fixture_reports:
        raise FileNotFoundError(f"no fixtures found in extraction eval set '{fixture_set}'")

    field_accuracy = _aggregate_field_accuracy(fixture_reports)
    aggregate_metrics = ExtractionEvalMetrics(
        record_count_accuracy=round(mean(report.metrics.record_count_accuracy for report in fixture_reports), 2),
        field_accuracy=field_accuracy,
        provenance_completeness=round(mean(report.metrics.provenance_completeness for report in fixture_reports), 2),
        schema_compliance_rate=round(mean(report.metrics.schema_compliance_rate for report in fixture_reports), 2),
        confidence_distribution=_confidence_distribution_from_scores(aggregate_confidences),
    )
    return ExtractionEvalReport(
        fixture_set=fixture_set,
        extraction_versions=sorted(extraction_versions),
        generated_at=datetime.now(timezone.utc).isoformat(),
        metrics=aggregate_metrics,
        overall_score=round(mean(report.overall_score for report in fixture_reports), 2),
        fixture_reports=fixture_reports,
    )


def _fixture_set_root(fixture_set: str) -> Path:
    root = _repo_root() / "tests" / "fixtures" / "extraction_eval" / slugify(fixture_set)
    if not root.exists():
        raise FileNotFoundError(f"extraction eval fixture set not found: {root}")
    return root


def _repo_root() -> Path:
    candidate = Path(__file__).resolve()
    for parent in candidate.parents:
        if (parent / "pyproject.toml").exists():
            return parent
    return candidate.parents[3]


def _load_fixture_records(fixture_dir: Path) -> list[_ActualRecordArtifact]:
    extracted_dir = fixture_dir / "extracted"
    if not extracted_dir.exists():
        raise FileNotFoundError(f"extracted fixture artifacts not found: {extracted_dir}")

    repo_root = _repo_root()
    artifacts: list[_ActualRecordArtifact] = []
    for record_path in sorted(extracted_dir.glob("*/*.json")):
        record_type = record_path.parent.name
        payload = json.loads(record_path.read_text(encoding="utf-8"))
        try:
            path_str = str(record_path.relative_to(repo_root))
        except ValueError:
            path_str = str(record_path)
        try:
            model = get_schema_model(record_type)
            record = model.model_validate(payload)
        except Exception:
            artifacts.append(
                _ActualRecordArtifact(
                    record_type=record_type,
                    path=path_str,
                    payload=payload,
                    schema_valid=False,
                    provenance_valid=False,
                )
            )
            continue
        try:
            validate_record_provenance(record)
            provenance_valid = True
        except ValueError:
            provenance_valid = False
        artifacts.append(
            _ActualRecordArtifact(
                record_type=record_type,
                path=path_str,
                payload=payload,
                schema_valid=True,
                provenance_valid=provenance_valid,
                record=record,
            )
        )
    return artifacts


def _score_record_count_accuracy(
    expected_records: dict[str, list[dict[str, object]]],
    actual_records: list[_ActualRecordArtifact],
) -> float:
    expected_total = sum(len(records) for records in expected_records.values())
    actual_total = len(actual_records)
    if expected_total == 0:
        return 100.0 if actual_total == 0 else 0.0
    return round(max(0.0, 1.0 - abs(actual_total - expected_total) / expected_total) * 100, 2)


def _score_field_accuracy(
    expected_records: dict[str, list[dict[str, object]]],
    actual_records: list[_ActualRecordArtifact],
) -> dict[str, float]:
    actual_by_type: dict[str, list[dict[str, object]]] = {}
    for artifact in actual_records:
        if artifact.record is None:
            continue
        actual_by_type.setdefault(artifact.record_type, []).append(_normalize_record_payload(artifact.payload))

    scores: dict[str, float] = {}
    for record_type, expected_items in sorted(expected_records.items()):
        normalized_expected = [_normalize_record_payload(item) for item in expected_items]
        normalized_actual = actual_by_type.get(record_type, [])
        if not normalized_expected:
            scores[record_type] = 100.0 if not normalized_actual else 0.0
            continue

        total_fields = 0
        matched_fields = 0
        sorted_expected = sorted(normalized_expected, key=lambda item: json.dumps(item, sort_keys=True))
        sorted_actual = sorted(normalized_actual, key=lambda item: json.dumps(item, sort_keys=True))
        for index, expected_item in enumerate(sorted_expected):
            total_fields += _leaf_count(expected_item)
            if index >= len(sorted_actual):
                continue
            matched_fields += _matching_fields(expected_item, sorted_actual[index])
        scores[record_type] = round((matched_fields / total_fields) * 100, 2) if total_fields else 100.0
    return scores


def _score_provenance_completeness(actual_records: list[_ActualRecordArtifact]) -> float:
    if not actual_records:
        return 100.0
    valid = sum(1 for artifact in actual_records if artifact.provenance_valid)
    return round((valid / len(actual_records)) * 100, 2)


def _score_schema_compliance(actual_records: list[_ActualRecordArtifact]) -> float:
    if not actual_records:
        return 100.0
    valid = sum(1 for artifact in actual_records if artifact.schema_valid)
    return round((valid / len(actual_records)) * 100, 2)


def _aggregate_field_accuracy(fixture_reports: list[ExtractionFixtureScore]) -> dict[str, float]:
    record_types = sorted({record_type for report in fixture_reports for record_type in report.metrics.field_accuracy})
    return {
        record_type: round(
            mean(
                report.metrics.field_accuracy[record_type]
                for report in fixture_reports
                if record_type in report.metrics.field_accuracy
            ),
            2,
        )
        for record_type in record_types
    }


def _record_counts(actual_records: list[_ActualRecordArtifact]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for artifact in actual_records:
        counts[artifact.record_type] = counts.get(artifact.record_type, 0) + 1
    return dict(sorted(counts.items()))


def _confidence_distribution(actual_records: list[_ActualRecordArtifact]) -> ConfidenceDistribution:
    return _confidence_distribution_from_scores(
        [artifact.record.confidence for artifact in actual_records if artifact.record is not None]
    )


def _confidence_distribution_from_scores(scores: list[float]) -> ConfidenceDistribution:
    if not scores:
        return ConfidenceDistribution(
            total_records=0,
            min_confidence=0.0,
            max_confidence=0.0,
            mean_confidence=0.0,
            low_count=0,
            medium_count=0,
            high_count=0,
        )

    low_count = sum(1 for score in scores if score < 0.5)
    high_count = sum(1 for score in scores if score >= 0.8)
    return ConfidenceDistribution(
        total_records=len(scores),
        min_confidence=min(scores),
        max_confidence=max(scores),
        mean_confidence=round(mean(scores), 3),
        low_count=low_count,
        medium_count=len(scores) - low_count - high_count,
        high_count=high_count,
    )


def _overall_score(metrics: ExtractionEvalMetrics) -> float:
    field_accuracy_average = mean(metrics.field_accuracy.values()) if metrics.field_accuracy else 100.0
    return round(
        mean(
            [
                metrics.record_count_accuracy,
                field_accuracy_average,
                metrics.provenance_completeness,
                metrics.schema_compliance_rate,
            ]
        ),
        2,
    )


def _normalize_record_payload(value: object) -> object:
    if isinstance(value, dict):
        return {
            key: _normalize_record_payload(item) for key, item in sorted(value.items()) if key not in PROVENANCE_FIELDS
        }
    if isinstance(value, list):
        return [_normalize_record_payload(item) for item in value]
    if isinstance(value, str):
        return _normalize_text(value)
    return value


def _normalize_text(value: str) -> str:
    return " ".join(re.sub(r"\s+", " ", value.strip()).split())


def _leaf_count(value: object) -> int:
    if isinstance(value, dict):
        return sum(_leaf_count(item) for item in value.values())
    if isinstance(value, list):
        return sum(_leaf_count(item) for item in value)
    return 1


def _matching_fields(expected: object, actual: object) -> int:
    if isinstance(expected, dict):
        if not isinstance(actual, dict):
            return 0
        return sum(_matching_fields(item, actual.get(key)) for key, item in expected.items())
    if isinstance(expected, list):
        if not isinstance(actual, list):
            return 0
        matched = sum(_matching_fields(item, actual[index]) for index, item in enumerate(expected[: len(actual)]))
        return matched
    return int(expected == actual)


def write_extraction_report(report: ExtractionEvalReport, *, output_dir: Path) -> Path:
    """Persist an extraction evaluation report under the local data directory."""
    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / f"{slugify(report.fixture_set)}.json"
    report_path.write_text(report.model_dump_json(indent=2), encoding="utf-8")
    return report_path
