"""Shared pytest configuration for Knowledge Forge tests."""

from __future__ import annotations

import sys
from pathlib import Path


def _prepend_repo_src() -> None:
    """Force pytest to import the current worktree package before any global editable install."""
    src_path = Path(__file__).resolve().parents[1] / "src"
    sys.path.insert(0, str(src_path))


_prepend_repo_src()
