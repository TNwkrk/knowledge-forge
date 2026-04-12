# Knowledge Forge — Agent Workflow and FlowCommander Integration

## Purpose

This document explains how agents should work in Knowledge Forge while staying
aligned with FlowCommander as the downstream integration target.

## Two-repo operating model

- **Knowledge Forge** is the upstream generation repo.
- **FlowCommander** is the downstream product repo.
- Knowledge Forge produces reviewable artifacts.
- FlowCommander receives approved wiki output through pull requests.

Agents should treat these repos as related but separate:
- work from `/Users/taylor/development/knowledge-forge`
- inspect `/Users/taylor/development/FlowCommander` when it exists locally
- avoid mixing unrelated implementation concerns between repos

## Local assumptions

The practical local working model is:
- Knowledge Forge lives at `/Users/taylor/development/knowledge-forge`
- FlowCommander may be available at `/Users/taylor/development/FlowCommander`
- local environment settings come from `.env` based on `.env.example`
- repo-local Codex defaults live in `.codex/`
- durable repo behavior lives in `AGENTS.md` and the docs in `docs/`

If FlowCommander is not present locally, agents can still prepare Knowledge
Forge artifacts, but they should call out that downstream structure was not
verified against the local target clone.

## Repo phase boundaries

Knowledge Forge should be worked in the phase it is actually in, not the phase
contributors wish already existed.

### Bootstrap

In scope:
- repo docs and operating guidance
- package scaffolding and local developer workflow
- minimal CLI and test harness setup
- fixture and golden-file skeletons for later ratcheting

Out of scope:
- speculative end-to-end pipeline code
- finished publish automation
- pretending unimplemented roadmap phases already exist

### Pipeline and parsing

In scope:
- intake manifests and bucket assignment
- OCR and normalization workflow
- parse artifact shape, parser selection, and parse quality checks
- parser-oriented fixtures and regression tests

Out of scope:
- wiki compilation features unrelated to parser inputs
- FlowCommander product changes
- publish automation beyond the documented contract

### Inference and extraction

In scope:
- OpenAI request handling, batching, retry, logging, and cost accounting
- extraction schemas, provenance, repair loops, and extraction validation
- fixture-driven evals for parsed sections and extraction outputs

Out of scope:
- direct downstream repo mutation
- final publish orchestration unless the issue explicitly targets publish work

### Publish and integration

In scope:
- publish staging shape inside Knowledge Forge
- contract validation against `docs/publish-contract.md`
- PR-oriented downstream handoff into FlowCommander

Out of scope:
- casual writes into `/Users/taylor/development/FlowCommander`
- merging Knowledge Forge and FlowCommander responsibilities
- silent automation that bypasses reviewability or provenance

## Working with FlowCommander safely

Use the local FlowCommander repo for:
- inspecting current `repo-wiki/knowledge/` layout
- comparing target slugs, page shapes, and doc organization
- validating that staged output fits downstream expectations
- preparing PR-ready artifact sets

Do not use the local FlowCommander repo for:
- casual hand-edits from intermediate generation output
- silent writes outside a PR-oriented publish workflow
- bypassing provenance or review requirements

## FlowCommander-facing artifact stages

Knowledge Forge should make it obvious which artifacts are:

1. **Generated working artifacts**
   - parser outputs
   - extracted records
   - intermediate compiled pages

2. **Publish-ready staged artifacts**
   - output that is ready to be proposed to FlowCommander
   - content with stable slugs and provenance
   - manifests or metadata needed to review what changed

Conceptually, the handoff looks like:

```text
Knowledge Forge local artifacts
  -> Knowledge Forge publish staging
  -> FlowCommander PR
  -> reviewed merge into repo-wiki/knowledge/
```

## Provenance and reviewability requirements

FlowCommander-facing wiki output should be reviewable by humans and future
agents. At minimum, staged output should preserve:
- source document identity
- revision or edition context when known
- the Knowledge Forge run or publish context
- enough metadata to explain why a page changed

Future automation should preserve these guarantees rather than replacing them.

## Documentation touchpoints

When changing durable workflow behavior, keep these docs aligned:
- `README.md`
- `AGENTS.md`
- `docs/codex-issue-runbook.md`
- `docs/architecture.md`
- `docs/evals.md`
- `docs/repo-structure.md`
- `docs/publish-contract.md`
- `docs/agent-workflow.md`

## Current boundary

This repository currently establishes setup and conventions only.

It does **not** yet promise:
- a full publish automation pipeline
- direct FlowCommander mutation
- completed Python bootstrap or CI wiring

Agents should keep scope honest and build on the documented baseline instead of
pretending later roadmap phases already exist.
