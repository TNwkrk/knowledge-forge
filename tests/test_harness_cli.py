"""Tests for repo-owned Codex/Symphony harness commands."""

from __future__ import annotations

from click.testing import CliRunner

from knowledge_forge.cli import cli


def test_cli_help_includes_harness_commands() -> None:
    result = CliRunner().invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "doctor" in result.output
    assert "docs-check" in result.output
    assert "validate" in result.output


def test_doctor_runs_without_secrets(monkeypatch) -> None:
    for name in [
        "OPENAI_API_KEY",
        "KNOWLEDGE_FORGE_DATA_DIR",
        "FLOWCOMMANDER_REPO_PATH",
        "GITHUB_TOKEN",
        "SYMPHONY_WORKSPACE_ROOT",
    ]:
        monkeypatch.delenv(name, raising=False)

    result = CliRunner().invoke(cli, ["doctor"])

    assert result.exit_code == 0
    assert "OPENAI_API_KEY: missing" in result.output
    assert "Package import: ok" in result.output


def test_docs_check_succeeds() -> None:
    result = CliRunner().invoke(cli, ["docs-check"])

    assert result.exit_code == 0
    assert "Docs check passed" in result.output
