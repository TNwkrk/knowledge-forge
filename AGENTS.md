# Knowledge Forge Agent Guide

## Project
Knowledge Forge is a separate repository in the broader FlowCommander project.

Its job is to turn manuals and other technical source material into reviewable,
human-readable knowledge artifacts that can later be proposed to FlowCommander
through a pull request workflow.

Core responsibilities:
- intake and manifesting
- pre-bucketing and parsing preparation
- structured extraction and wiki compilation
- provenance-preserving staging for publish
- PR-based publication into FlowCommander `repo-wiki/knowledge/`

Primary priorities:
- trustworthy generated artifacts
- provenance and reviewability
- safe repo-to-repo workflow boundaries
- practical local agent execution
- minimal operational friction for future Codex work

## Working Rules
- Keep Knowledge Forge separate from FlowCommander. Do not collapse the two repos
  into one working tree or one architecture.
- Respect the current repo state. This repository is still in bootstrap mode; do
  not invent finished pipeline code when only planning or scaffolding exists.
- Favor small, mergeable changes and explicit docs over hidden conventions.
- Do not widen scope into publish automation unless the repo already contains the
  necessary implementation path.
- Generated FlowCommander-facing output must preserve provenance, reviewability,
  and rerun safety.
- Do not directly edit FlowCommander `repo-wiki` as an ad hoc side effect of
  normal Knowledge Forge work. Publication happens through a controlled PR flow.

## FlowCommander Integration Boundary
- FlowCommander is the downstream product and integration repo.
- Knowledge Forge is the upstream artifact-generation repo.
- The intended publish target is FlowCommander `repo-wiki/knowledge/`.
- Local inspection of `/Users/taylor/development/FlowCommander` is encouraged
  when present so agents can validate target structure and downstream
  expectations.
- Local FlowCommander access is for inspection, comparison, and publish
  preparation. It is not a license to silently write into that repo.
- Any eventual publish step should stage reviewable output in Knowledge Forge
  first, then open a PR against `TNwkrk/FlowCommander`.

## Local Working Conventions
- Keep repo-local Codex defaults in `.codex/`.
- Put durable repo behavior in checked-in docs such as this file and
  `docs/agent-workflow.md`.
- Use `.env.example` and committed docs for shared setup guidance. Do not commit
  secrets, tokens, or machine-private settings.
- Treat `data/` as a local working area for generated artifacts. Commit only
  intentional scaffolding files such as `README.md` and `.gitkeep`.

## Canonical References
- `README.md` explains the repo's purpose, scope, and FlowCommander boundary.
- `AGENTS.md` is the top-level operating rules document for agents.
- `docs/codex-issue-runbook.md` is the standard issue-worker runbook.
- `docs/roadmap.md` is the source of truth for phased issue sequencing.
- `docs/publish-contract.md` defines the downstream publish contract.
- `docs/repo-structure.md` defines the intended artifact layout.
- `docs/evals.md` defines the current lightweight fixtures and golden-file
  skeleton.

## Phase Boundaries
- **Bootstrap**: repo setup, docs, package scaffolding, local tooling, and basic
  validation are in scope. Full parser, extraction, or publish implementations
  are not.
- **Pipeline / parsing**: intake, bucketing, OCR, parse artifact shape, and
  parser validation are in scope. Topic-page generation, publish automation, and
  downstream integration are not.
- **Inference / extraction**: OpenAI client behavior, schema validation,
  provenance, extraction repair, and extraction evals are in scope. FlowCommander
  product behavior and final publish workflows are not.
- **Publish / integration**: staging, publish-contract validation, manifesting,
  and PR-oriented handoff into FlowCommander are in scope. Casual direct writes
  into FlowCommander and unrelated product changes are not.

## Safe Edit Zones
- Usually safe to modify: `docs/`, `.codex/`, `tests/`, and small scaffolding in
  `src/knowledge_forge/` that matches the current roadmap phase.
- Edit with care: `pyproject.toml`, `.env.example`, `data/README.md`, and any
  schema or artifact-shape docs that define downstream expectations.
- Do not change unless the issue requires it: FlowCommander local clone content,
  publish-boundary assumptions in `docs/publish-contract.md`, and repo-wide
  structure or naming conventions already referenced by other docs.

## Issue Acceptance Signals
- Every issue should have a concrete acceptance signal before work starts.
- Preferred signals are real commands, schema validation, expected artifact
  shapes, or explicit docs-consistency checks.
- If the repo does not yet have executable checks for an issue, document the
  honest validation that was performed instead of implying stronger coverage.
- Treat vague completion like "looks good" as insufficient for closing an issue
  or opening a PR.

## Documentation Maintenance
- When durable behavior, workflow boundaries, artifact layout, or publish rules
  change, update the relevant docs in `docs/`.
- Prefer updating an existing doc over creating a duplicate page.
- Keep the architecture, repo structure, publish contract, and agent workflow
  docs aligned with each other.
- If no doc update is needed for a change, say so explicitly in the final report.

## Git Workflow
- Always create a new branch for implementation work.
- Use clear branch names scoped to the task.
- Commit logically grouped changes.
- Open a PR when the work is ready for review.
- Do not merge directly to `main` unless explicitly instructed.

## Build and Test
- Before changing code, inspect the repo and document the real install, lint,
  test, and validation commands that exist.
- If commands do not exist yet, report that clearly rather than inventing a fake
  workflow.
- For this repo's current bootstrap state, docs and scaffolding consistency are
  meaningful validation steps.

## Expected Delivery Pattern
For non-trivial work:
1. inspect the current repo state and branch/PR context
2. create a short implementation plan
3. implement on a new branch
4. review the result
5. run the relevant validation available in the repo
6. summarize risks, assumptions, and next actions

## Output Expectations
Final reports should include:
- task summary
- branch name
- files changed
- repo docs updates
- tests or validation run
- review findings
- remaining risks
- PR status or next action
