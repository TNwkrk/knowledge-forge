"""Retry helpers for transient inference operations."""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from typing import TypeVar

from pydantic import BaseModel, ConfigDict, Field

T = TypeVar("T")

TRANSIENT_STATUS_CODES = frozenset({429, 500, 503})


class RetryPolicy(BaseModel):
    """Exponential backoff settings shared by direct and batch inference calls."""

    model_config = ConfigDict(extra="forbid")

    max_retries: int = Field(default=3, ge=0)
    initial_delay_seconds: float = Field(default=1.0, ge=0.0)
    backoff_multiplier: float = Field(default=2.0, ge=1.0)
    max_delay_seconds: float = Field(default=30.0, ge=0.0)
    jitter_seconds: float = Field(default=0.0, ge=0.0)


def retry_transient(
    operation: Callable[[], T],
    *,
    policy: RetryPolicy | None = None,
    is_retryable: Callable[[Exception], bool] | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
    random_fn: Callable[[], float] = random.random,
) -> T:
    """Run an operation with exponential backoff for retryable failures."""
    resolved_policy = policy or RetryPolicy()
    retryable = is_retryable or is_transient_error
    delay = resolved_policy.initial_delay_seconds

    for attempt in range(resolved_policy.max_retries + 1):
        try:
            return operation()
        except Exception as exc:
            if attempt >= resolved_policy.max_retries or not retryable(exc):
                raise
            sleep_fn(_next_delay(delay, resolved_policy, random_fn))
            delay = min(delay * resolved_policy.backoff_multiplier, resolved_policy.max_delay_seconds)

    raise RuntimeError("retry loop exited without returning or raising")


def is_transient_error(error: Exception) -> bool:
    """Return whether an exception should be retried automatically."""
    status_code = getattr(error, "status_code", None)
    if status_code is None:
        response = getattr(error, "response", None)
        status_code = getattr(response, "status_code", None)
    if status_code in TRANSIENT_STATUS_CODES:
        return True

    message = str(error).lower()
    return any(token in message for token in {"timeout", "temporarily unavailable", "rate limit"})


def _next_delay(
    delay: float,
    policy: RetryPolicy,
    random_fn: Callable[[], float],
) -> float:
    jitter = policy.jitter_seconds * random_fn() if policy.jitter_seconds > 0 else 0.0
    return min(delay + jitter, policy.max_delay_seconds)
