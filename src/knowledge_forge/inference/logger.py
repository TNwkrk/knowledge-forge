"""Structured logging helpers for inference requests."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterator

from pydantic import BaseModel, ConfigDict, Field, ValidationError

LOGGER = logging.getLogger(__name__)


class InferenceLogEntry(BaseModel):
    """Normalized shape for persisted inference request logs."""

    model_config = ConfigDict(extra="forbid")

    request_id: str | None = None
    batch_id: str | None = None
    pipeline_run_id: str | None = None
    mode: str = Field(min_length=1)
    model: str = Field(min_length=1)
    prompt_template: str | None = None
    source_doc_id: str | None = None
    source_section_id: str | None = None
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    estimated_cost_usd: float = Field(ge=0.0)
    latency_ms: int = Field(ge=0)
    status: str = Field(min_length=1)
    schema_valid: bool | None = None
    error: str | None = None
    timestamp: datetime


class InferenceLogger:
    """Persist one JSON log file per inference request, partitioned by UTC date."""

    def __init__(self, log_dir: Path) -> None:
        self.log_dir = log_dir

    def log(self, entry: InferenceLogEntry) -> Path:
        """Write a structured JSON log entry to disk."""
        entry_dir = self.log_dir / entry.timestamp.astimezone(UTC).date().isoformat()
        entry_dir.mkdir(parents=True, exist_ok=True)

        request_fragment = _sanitize_filename_component(entry.request_id or "no-request-id")
        timestamp_fragment = entry.timestamp.astimezone(UTC).strftime("%H%M%S%f")
        path = entry_dir / f"{timestamp_fragment}-{request_fragment}.json"
        path.write_text(entry.model_dump_json(indent=2), encoding="utf-8")
        return path


def iter_log_entries(log_dir: Path) -> Iterator[InferenceLogEntry]:
    """Yield all structured inference logs from the configured directory."""
    if not log_dir.exists():
        return

    for path in sorted(log_dir.rglob("*.json")):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            yield InferenceLogEntry.model_validate(payload)
        except (json.JSONDecodeError, ValidationError) as exc:
            LOGGER.warning("skipping invalid inference log entry '%s': %s", path, exc)
            continue


def utc_now() -> datetime:
    """Return the current UTC timestamp as an aware datetime."""
    return datetime.now(UTC)


def _sanitize_filename_component(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in value)
