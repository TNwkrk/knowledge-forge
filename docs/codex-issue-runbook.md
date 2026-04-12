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
3. Inspect the existing repo structure and the real validation commands already
   available.
4. Make the smallest complete change set that satisfies the issue.
5. Update docs when behavior, structure, boundaries, or acceptance criteria
   changed.
6. Run the relevant validation and report exactly what was run.
7. Open a PR against the default branch with scope, validation, and out-of-scope
   notes.

## Validation expectations

- Run the issue's acceptance checks when they exist.
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
