"""CLI entry points for Knowledge Forge."""

import click


@click.group(help="Knowledge Forge command line interface.")
def cli() -> None:
    """Top-level CLI group for future subcommands."""


if __name__ == "__main__":
    cli()
