"""Cost estimation and aggregation for inference logs."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class ModelPricing(BaseModel):
    """Per-model pricing expressed in USD per 1M tokens."""

    model_config = ConfigDict(extra="forbid")

    input_per_million_tokens: float = Field(ge=0.0)
    output_per_million_tokens: float = Field(ge=0.0)


class CostTotals(BaseModel):
    """Aggregate token and cost totals for a logical grouping."""

    model_config = ConfigDict(extra="forbid")

    request_count: int = Field(default=0, ge=0)
    input_tokens: int = Field(default=0, ge=0)
    output_tokens: int = Field(default=0, ge=0)
    estimated_cost_usd: float = Field(default=0.0, ge=0.0)


class CostReport(BaseModel):
    """Roll-up view over inference log entries."""

    model_config = ConfigDict(extra="forbid")

    total: CostTotals = Field(default_factory=CostTotals)
    by_model: dict[str, CostTotals] = Field(default_factory=dict)
    by_date: dict[str, CostTotals] = Field(default_factory=dict)
    by_pipeline_run: dict[str, CostTotals] = Field(default_factory=dict)


def estimate_cost(model: str, input_tokens: int, output_tokens: int, pricing: dict[str, ModelPricing]) -> float:
    """Estimate USD cost from token counts using configured per-model pricing."""
    if model not in pricing:
        raise ValueError(f"no pricing configured for model '{model}'")

    model_pricing = pricing[model]
    input_cost = (input_tokens / 1_000_000) * model_pricing.input_per_million_tokens
    output_cost = (output_tokens / 1_000_000) * model_pricing.output_per_million_tokens
    return round(input_cost + output_cost, 8)


def aggregate_costs(log_dir: Path) -> CostReport:
    """Aggregate structured inference logs by model, date, and pipeline run."""
    from knowledge_forge.inference.logger import iter_log_entries

    report = CostReport()
    by_model: defaultdict[str, CostTotals] = defaultdict(CostTotals)
    by_date: defaultdict[str, CostTotals] = defaultdict(CostTotals)
    by_pipeline_run: defaultdict[str, CostTotals] = defaultdict(CostTotals)

    for entry in iter_log_entries(log_dir):
        _update_totals(report.total, entry.input_tokens, entry.output_tokens, entry.estimated_cost_usd)
        _update_totals(by_model[entry.model], entry.input_tokens, entry.output_tokens, entry.estimated_cost_usd)
        _update_totals(
            by_date[entry.timestamp.date().isoformat()],
            entry.input_tokens,
            entry.output_tokens,
            entry.estimated_cost_usd,
        )
        if entry.pipeline_run_id is not None:
            _update_totals(
                by_pipeline_run[entry.pipeline_run_id],
                entry.input_tokens,
                entry.output_tokens,
                entry.estimated_cost_usd,
            )

    report.by_model = dict(sorted(by_model.items()))
    report.by_date = dict(sorted(by_date.items()))
    report.by_pipeline_run = dict(sorted(by_pipeline_run.items()))
    report.total.estimated_cost_usd = round(report.total.estimated_cost_usd, 8)
    return report


def _update_totals(totals: CostTotals, input_tokens: int, output_tokens: int, estimated_cost_usd: float) -> None:
    totals.request_count += 1
    totals.input_tokens += input_tokens
    totals.output_tokens += output_tokens
    totals.estimated_cost_usd = round(totals.estimated_cost_usd + estimated_cost_usd, 8)
