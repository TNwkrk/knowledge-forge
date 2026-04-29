# Knowledge Forge — Codex Issue Runbook

Use this runbook when Codex works a single roadmap issue in Knowledge Forge.

## Working rules

- Work one issue at a time.
- Start from the current default branch, then create a focused feature branch.
- Keep the diff scoped to the issue's acceptance signal.
- Do not widen scope into later roadmap phases just because the structure is
  obvious.
- Preserve the FlowCommander boundary: stage in Knowledge Forge, publish by PR,
  and do not make casual downstream repo edits.

## Standard issue loop

1. Read the issue, `AGENTS.md`, `README.md`, and the relevant docs named there.
2. Confirm the current roadmap phase and stay inside that phase boundary.
3. Run `kf doctor` before non-trivial work when practical to record local
   Python, git, required-doc, and environment readiness without printing
   secrets.
4. Inspect the existing repo structure and the real validation commands already
   available.
5. Make the smallest complete change set that satisfies the issue.
6. Update docs when behavior, structure, boundaries, or acceptance criteria
   changed.
7. Run the relevant validation and report exactly what was run.
8. In the final report, include the `AGENTS.md` FlowCommander repo-wiki `yes` /
   `no` determination.
9. Open a PR against the default branch with scope, validation, and out-of-scope
   notes.

## Validation expectations

- Run the issue's acceptance checks when they exist.
- Run `kf docs-check` for docs, workflow, publish-boundary, or handoff changes.
- Run `kf validate` before PR when practical; it runs `ruff check .`,
  `ruff format --check .`, `python -m pytest`, and `git diff --check`.
- Always run `git diff --check` before wrapping up.
- If only docs or scaffolding changed, docs consistency and smoke checks are
  valid, but say that plainly.
- Do not claim tests, CI, or schema validation passed unless they were actually
  run.

## Blockers

- Stop and report blockers when acceptance criteria are ambiguous, required repo
  context is missing, or the issue would require crossing into a later phase.
- Offer the smallest unblock path instead of silently improvising architecture.

## PR expectations

- Branch names should be clear and issue-scoped.
- PR bodies should include the summary, files changed, commands run,
  intentionally out-of-scope items, and how the change improves future work.
- Leave merge decisions to the repo's normal review flow unless explicitly told
  otherwise.
