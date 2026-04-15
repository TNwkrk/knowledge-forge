# Copilot instructions for `TNwkrk/knowledge-forge`

## Repository summary

Knowledge Forge is a Python-based manual digestion pipeline. It is separate from FlowCommander and exists to turn technical manuals into trustworthy, reviewable, publishable knowledge artifacts.

This repo owns:
- manual intake and manifesting
- pre-bucketing before heavy processing
- OCR and normalization
- layout-aware parsing and sectioning
- OpenAI-backed structured extraction
- OpenAI-backed wiki compilation
- contradiction and supersession analysis
- publish staging and PR-based publishing into FlowCommander

This repo does **not** own the FlowCommander product app. Do not turn this repo into a generic web app, hosted runtime, or downstream retrieval system.

## High-level repo shape

Read these first before searching widely:
1. `README.md`
2. `AGENTS.md`
3. `docs/codex-issue-runbook.md`
4. `docs/roadmap.md`
5. `docs/publish-contract.md`
6. `docs/repo-structure.md`
7. `pyproject.toml`
8. `.github/workflows/ci.yaml`

Primary paths:
- `src/knowledge_forge/` — package root
- `tests/` — pytest suite, fixtures, and golden files
- `data/` — local artifact staging area; gitignored except conventions docs
- `.github/workflows/ci.yaml` — CI checks
- `.pre-commit-config.yaml` — local lint and format hooks
- `pyproject.toml` — packaging, dependencies, ruff, pytest config

Current package layout is intentionally phase-oriented:
- `intake/`
- `bucketing/`
- `normalize/`
- `parse/`
- `inference/`
- `extract/`
- `compile/`
- `publish/`
- `evaluation/`

Treat `docs/repo-structure.md` as the fastest way to understand where new code should live and how artifacts should flow.

## Working rules that always apply

- Work one roadmap issue at a time.
- Keep diffs tightly scoped to the issue acceptance signal.
- Do not casually widen scope into later roadmap phases.
- Preserve the Knowledge Forge -> FlowCommander boundary.
- Stage outputs in Knowledge Forge first.
- Publish to FlowCommander only by pull request.
- Do not treat local FlowCommander access as permission to make casual downstream edits.
- Prefer the smallest complete fix over a redesign.
- Update docs when behavior, structure, boundaries, or acceptance criteria change.
- Report exactly what validation was run.
- Do not claim tests passed unless they were actually run.

## Architecture and artifact flow

The intended lifecycle is:

`raw -> normalized -> parsed -> sections -> extracted -> compiled -> publish stage -> FlowCommander PR`

Important architectural rules:
- preserve provenance at every stage
- preserve rerun safety and idempotency
- prefer content-aware or checksum-aware behavior over duplicate-prone behavior
- approved outputs should not be overwritten casually
- generated artifacts should be reviewable and reproducible

Be suspicious of changes that weaken:
- source linkage
- page ranges
- heading context
- revision metadata
- parser metadata
- extraction metadata
- bucket context
- publish manifest integrity

## Bootstrap and environment

This is a Python project using `hatchling` packaging and requiring Python 3.11+.

Recommended bootstrap sequence from repo root:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

Always activate the virtual environment before running repo commands.

Environment notes:
- use editable install with dev extras so `pytest`, `ruff`, and `pre-commit` are available
- secrets belong in environment variables, not committed files
- `.env.example` documents shared variable names
- `data/` is the local staging area for artifacts; do not commit generated pipeline output there unless the task explicitly changes conventions docs

## Commands to know

Run these from repo root after activating `.venv`.

### Import and CLI smoke checks
```bash
python -c "import knowledge_forge"
python -m knowledge_forge.cli --help
```

### Lint
```bash
ruff check .
ruff format --check .
```

### Test
```bash
python -m pytest
```

### Pre-commit
```bash
pre-commit install
pre-commit run --all-files
```

### Diff hygiene
```bash
git diff --check
```

## Recommended validation order

For most code changes, use this order:

```bash
python -c "import knowledge_forge"
python -m knowledge_forge.cli --help
ruff check .
ruff format --check .
python -m pytest
git diff --check
```

For docs-only or scaffold-only changes:
- run the smoke checks that still apply
- run `git diff --check`
- say plainly that the change was docs-only or scaffolding-only

If a task changes packaging, CLI wiring, manifests, artifact paths, or stage boundaries, also review these for consistency:
- `README.md`
- `docs/repo-structure.md`
- `docs/publish-contract.md`
- `docs/roadmap.md`

## CI expectations

CI lives in `.github/workflows/ci.yaml`.

Assume CI expects the local equivalents of:
- lint
- formatting check
- pytest

Before opening a PR, run the relevant local commands yourself. Do not open a PR claiming validation confidence without running the matching checks.

## Review priorities

Prioritize these over style nitpicks:
- correctness bugs
- phase-boundary violations
- accidental cross-repo boundary violations
- weak provenance handling
- missing validation for changed behavior
- contract drift between code, docs, and staged artifact shape
- idempotency and rerun-safety regressions
- docs/implementation mismatch
- hidden scope creep
- PRs that claim more than they actually deliver
- unsafe assumptions about manifests, parsing outputs, extraction outputs, compiled pages, publish staging, or downstream repo structure

Call out missing tests only when the changed behavior actually needs them.

## Guidance for follow-up fix passes

When applying review findings:
- keep the fix set narrow and complete
- preserve the issue and roadmap phase boundary
- avoid opportunistic refactors unless required to make the fix safe
- update docs when behavior, structure, boundaries, or acceptance criteria changed
- keep PR summaries honest about what is fixed versus still out of scope
- prefer the smallest complete fix over a redesign

## Where to make changes

Use the phase docs and existing package layout as the routing map:
- manifest and registration work belongs under `src/knowledge_forge/intake/`
- bucket assignment work belongs under `src/knowledge_forge/bucketing/`
- OCR and normalization work belongs under `src/knowledge_forge/normalize/`
- parsing and sectioning work belongs under `src/knowledge_forge/parse/`
- OpenAI client, batching, retry, and logging work belongs under `src/knowledge_forge/inference/`
- schema-bound extraction belongs under `src/knowledge_forge/extract/`
- wiki generation belongs under `src/knowledge_forge/compile/`
- publish staging and downstream validation belong under `src/knowledge_forge/publish/`
- eval harness work belongs under `src/knowledge_forge/evaluation/`
- tests should live in `tests/` near the affected subsystem, using fixtures or golden files only when the output shape is stable enough to review

## Final instruction

Trust these instructions first. Search the repo only when:
- the task touches an area not covered here
- the instructions are incomplete for the exact subsystem
- live repo contents contradict these instructions

Do not improvise architecture when the docs already define the boundary, stage, or artifact contract.
