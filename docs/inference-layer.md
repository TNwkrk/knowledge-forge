# Knowledge Forge — OpenAI Inference Layer

## Overview

The OpenAI inference layer is a first-class subsystem of Knowledge Forge. It is not a utility function called in passing—it has its own client abstraction, configuration, logging, cost tracking, retry logic, and batch processing infrastructure.

## Design principles

1. **Two modes.** Every inference operation supports both direct (single-request) and batch modes.
2. **Logged by default.** Every request and response is logged with token counts, cost, latency, and provenance.
3. **Schema-bound.** Extraction and compilation prompts define expected output schemas. Responses are validated against these schemas.
4. **Retriable.** Failed requests are retried with backoff. Failed batch items are reconciled individually.
5. **Cost-aware.** Token usage and estimated cost are tracked per request, per batch, and per pipeline run.

## Architecture

```
┌─────────────────────────────────────────┐
│            Inference Client              │
│  ┌───────────┐  ┌────────────────────┐  │
│  │  Direct    │  │  Batch             │  │
│  │  Request   │  │  ┌──────────────┐  │  │
│  │  Mode      │  │  │ JSONL Builder│  │  │
│  │            │  │  │ Submitter    │  │  │
│  │            │  │  │ Poller       │  │  │
│  │            │  │  │ Ingester     │  │  │
│  │            │  │  └──────────────┘  │  │
│  └───────────┘  └────────────────────┘  │
│  ┌───────────┐  ┌────────────────────┐  │
│  │  Config &  │  │  Logging &         │  │
│  │  Secrets   │  │  Cost Tracking     │  │
│  └───────────┘  └────────────────────┘  │
│  ┌───────────┐  ┌────────────────────┐  │
│  │  Retry &   │  │  Schema            │  │
│  │  Rate Limit│  │  Validation        │  │
│  └───────────┘  └────────────────────┘  │
└─────────────────────────────────────────┘
```

## Components

### Client wrapper

The core client wraps the OpenAI Python SDK and provides:

- Centralized configuration (model, temperature, max tokens, etc.)
- API key management via environment variables or config file
- Request/response type definitions
- Unified interface for both direct and batch modes

### Configuration

```yaml
# config/inference.yaml
openai:
  api_key_env: OPENAI_API_KEY
  default_model: gpt-4o
  extraction_model: gpt-4o
  compilation_model: gpt-4o
  temperature: 0.0
  max_tokens: 4096
  rate_limit:
    max_requests_per_minute: 500
    max_tokens_per_minute: 150000
  batch:
    max_batch_size: 50000
    poll_interval_seconds: 60
    max_poll_duration_seconds: 86400
```

### Direct request mode

Used for:
- Prompt development and debugging
- Single-document testing
- Interactive extraction runs
- Low-volume operations

Flow:
1. Build prompt from template + section content
2. Send request via OpenAI client
3. Parse and validate response against schema
4. Log tokens, cost, latency
5. Return structured result or error

### Batch mode

Used for:
- Backlog processing runs
- Section-by-section extraction across a corpus
- Corpus-scale wiki compilation
- Any operation touching more than ~10 sections

Flow:
1. **Build** — Collect prompts into a JSONL file following the OpenAI Batch API format
2. **Submit** — Upload JSONL and create a batch job
3. **Poll** — Check batch status on an interval
4. **Ingest** — Download results, parse each response, validate against schema
5. **Reconcile** — Identify failed items, log errors, queue retries

### Request logging

Every inference call (direct or batch) produces a log entry:

```json
{
  "request_id": "req-abc123",
  "batch_id": null,
  "mode": "direct",
  "model": "gpt-4o",
  "prompt_template": "extraction/procedure",
  "source_doc_id": "doc-001",
  "source_section_id": "sec-014",
  "input_tokens": 1523,
  "output_tokens": 487,
  "estimated_cost_usd": 0.012,
  "latency_ms": 2340,
  "status": "success",
  "schema_valid": true,
  "timestamp": "2024-01-15T10:30:00Z"
}
```

Logs are stored in `data/inference_logs/` organized by date and run.

### Cost tracking

The system tracks:
- Per-request token usage and estimated cost
- Per-batch aggregate cost
- Per-pipeline-run total cost
- Running totals across all runs

Cost estimates use configurable per-model pricing. Alerts can be configured for cost thresholds.

### Retry and rate limiting

- Transient errors (429, 500, 503) are retried with exponential backoff
- Rate limiting respects both RPM and TPM limits
- Failed batch items are retried individually in a follow-up batch or via direct mode
- Permanent failures (schema violations after repair, content policy blocks) are logged and flagged

### Schema validation

Extraction responses are validated against JSON schemas:
- Valid responses are accepted and stored
- Invalid responses enter the repair path:
  1. Re-prompt with the invalid output and a correction instruction
  2. If still invalid, relax schema constraints and retry
  3. If still invalid, flag for manual review
- Compilation responses are validated for required frontmatter fields and structural completeness

## Inference operations

### Extraction operations

| Operation | Input | Output |
|---|---|---|
| Extract procedures | Parsed section (maintenance, startup, etc.) | `procedure` + `procedure_step` records |
| Extract warnings | Parsed section (safety, caution callouts) | `warning` records |
| Extract specs | Parsed section (specifications, ratings) | `spec_value` records |
| Extract alarms | Parsed section (alarm tables, fault codes) | `alarm_definition` records |
| Extract troubleshooting | Parsed section (troubleshooting) | `troubleshooting_entry` records |
| Extract parts | Parsed section (parts lists) | `part_reference` records |
| Extract applicability | Parsed section (model coverage) | `applicability` records |
| Extract revision notes | Parsed section (revision history) | `revision_note` records |

### Compilation operations

| Operation | Input | Output |
|---|---|---|
| Compile source page | All extraction records for one document | Source page Markdown |
| Compile topic page | All extraction records for one topic across a bucket | Topic page Markdown |
| Compile family overview | All extraction records for one family | Family overview Markdown |
| Compile contradiction notes | Contradiction candidates in a bucket | Contradiction note Markdown |

### Analysis operations

| Operation | Input | Output |
|---|---|---|
| Detect contradictions | Two overlapping records from different sources | `contradiction_candidate` record |
| Assess supersession | Two records with revision/date ordering | `supersession_candidate` record |

## Prompt management

Prompts are stored as versioned templates in `src/knowledge_forge/inference/prompts/`:

```
prompts/
  extraction/
    procedure.yaml
    warning.yaml
    spec_value.yaml
    alarm_definition.yaml
    troubleshooting.yaml
    part_reference.yaml
    applicability.yaml
    revision_note.yaml
  compilation/
    source_page.yaml
    topic_page.yaml
    family_overview.yaml
    contradiction_notes.yaml
  analysis/
    contradiction_detect.yaml
    supersession_assess.yaml
```

Each prompt template includes:
- System message
- User message template with placeholders
- Expected output schema reference
- Model override (if different from default)
- Temperature override (if different from default)

## Token budget management

For large sections that exceed context limits:
1. Split the section into overlapping chunks
2. Extract from each chunk independently
3. Deduplicate and merge results
4. Record that the extraction was chunked in provenance metadata
