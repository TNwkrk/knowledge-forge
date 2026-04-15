"""Shared pytest configuration for Knowledge Forge tests."""

from __future__ import annotations

import sys
from pathlib import Path


def _prepend_repo_src() -> None:
    """Force pytest to import the current worktree package before any global editable install."""
    src_path = Path(__file__).resolve().parents[1] / "src"
    src_path_str = str(src_path)
    if sys.path and sys.path[0] == src_path_str:
        return
    if src_path_str in sys.path:
        sys.path.remove(src_path_str)
    sys.path.insert(0, src_path_str)


_prepend_repo_src()
