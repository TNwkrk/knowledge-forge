# Knowledge Forge — Lightweight Evals

Knowledge Forge is not ready for a full benchmark system yet, but future parser,
extraction, and compilation issues need an obvious place for fixture-driven
validation.

## Current skeleton

- `tests/fixtures/` holds small committed inputs or intermediate artifacts used
  by tests.
- `tests/golden/` holds expected structured outputs or Markdown snapshots.
- `tests/` remains the home for the executable checks that compare live output to
  those fixtures or golden files.

## What belongs here

- tiny sample manifests, parsed sections, or normalized text fragments
- stable expected JSON, YAML, or Markdown outputs
- regression cases for previously fixed parsing or extraction behavior
- representative samples from multiple document types (not just manuals) —
  include at least one SOP/checklist, one datasheet, and one service bulletin
  fixture when those source families are supported

## What does not belong here yet

- large raw manual corpora
- ad hoc local experiments that are not part of a repeatable test
- performance benchmarking infrastructure
- fixtures for source families not yet supported by the pipeline (config
  backups, telemetry exports, multimedia)

## Ratcheting guidance

- Add a fixture only when an issue needs a durable example.
- Prefer the smallest sample that still captures the edge case.
- Keep golden outputs reviewable by humans and derived from committed fixtures.
- When a fixture or golden file changes, explain why in the PR.
