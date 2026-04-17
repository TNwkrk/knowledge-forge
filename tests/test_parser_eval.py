"""Tests for the parser evaluation harness."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from knowledge_forge.cli import cli
from knowledge_forge.evaluation import evaluate_parser


def test_evaluate_parser_scores_committed_fixture_set() -> None:
    report = evaluate_parser("baseline", "docling")

    assert report.fixture_set == "baseline"
    assert report.parser == "docling"
    assert len(report.fixture_reports) == 3
    assert report.metrics.heading_accuracy == 100.0
    assert report.metrics.table_extraction_accuracy == 100.0
    assert report.metrics.text_completeness == 100.0
    assert report.metrics.structure_fidelity == 100.0
    assert report.overall_score == 100.0
    assert "workflow" in report.fixture_reports[2].actual_section_types


def test_eval_parser_cli_writes_report(tmp_path: Path) -> None:
    runner = CliRunner()
    data_dir = tmp_path / "data"

    result = runner.invoke(
        cli,
        ["eval", "parser", "baseline", "--parser", "docling"],
        env={"KNOWLEDGE_FORGE_DATA_DIR": str(data_dir)},
    )

    assert result.exit_code == 0
    assert "Fixture set: baseline" in result.output
    assert "Overall score: 100.00" in result.output

    report_path = data_dir / "evaluation" / "parser" / "baseline--docling.json"
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["fixture_set"] == "baseline"
    assert payload["parser_versions"] == ["test-docling-1.0"]
