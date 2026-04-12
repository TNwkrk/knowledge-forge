"""Smoke tests for the Knowledge Forge package bootstrap."""

from click.testing import CliRunner

from knowledge_forge import __version__
from knowledge_forge.cli import cli


def test_package_version_is_defined() -> None:
    assert __version__


def test_cli_help_runs() -> None:
    result = CliRunner().invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "Knowledge Forge" in result.output
