---
name: knowledge-forge-symphony
description: Conservative autonomous issue execution contract for Knowledge Forge.
version: 1
repository: TNwkrk/knowledge-forge
default_branch: main
max_concurrency: 1
tracker:
  provider: linear
  team_key: FC
  repo_label: repo:knowledge-forge
  pickable_status: Ready
  review_status: Human Review
  rework_status: Rework
  merging_status: Merging
  done_status: Done
  symphony_ready_label: symphony-ready
  auto_merge: false
  require_human_review: true
workspace:
  # Prefer an operator-provided isolated workspace root. The fallback stays out
  # of this checkout and is safe for local dry runs.
  root: "${SYMPHONY_WORKSPACE_ROOT:-/tmp/knowledge-forge-symphony}"
  per_issue_subdir: true
runtime:
  python: ">=3.11"
  package: knowledge-forge
  console_scripts:
    - knowledge-forge
    - kf
hooks:
  pre_issue:
    - command: "kf doctor"
      required: false
      note: "Run before meaningful work when practical; no network, GitHub, or OpenAI calls."
  docs_boundary_change:
    - command: "kf docs-check"
      required: true
      note: "Run when docs, workflow, publish-boundary, or handoff behavior changes."
  pre_pr:
    - command: "kf validate"
      required: false
      note: "Run before PR when practical; reports only commands that actually execute."
safety:
  secrets: false
  network_required: false
  openai_calls_allowed_by_default: false
  github_network_calls_allowed_by_default: false
  direct_flowcommander_edits_allowed: false
---

# Knowledge Forge Symphony Workflow

Use this workflow for one Linear issue at a time in `TNwkrk/knowledge-forge`.
It prepares Codex/Symphony agents to inspect, validate, and hand off focused
pull requests for human review. It does not implement, vendor, or require
OpenAI Symphony itself. Linear is the planning and execution control plane;
GitHub remains code hosting, pull requests, CI, Dependabot, and security.

## Codex Issue Prompt

You are working on a single Linear issue in `TNwkrk/knowledge-forge`.

Before meaningful work:

1. Read `AGENTS.md`, `README.md`, and `docs/roadmap.md`.
2. Read `docs/codex-issue-runbook.md`.
3. Confirm the issue is in the FlowCommander team (`FC`) with status `Ready`.
4. Confirm the `symphony-ready` label is present.
5. Confirm exactly one repo label is present and that it is `repo:knowledge-forge`.
6. Do not pick issues labeled `human-only`, `symphony-blocked`, `needs-spec`, or `blocked`.
7. Do not pick issues with open blockers.
8. Confirm the issue's roadmap phase and concrete acceptance signal.
9. Run `kf doctor` when practical and treat missing optional environment
   variables as warnings unless the issue explicitly requires strict runtime
   configuration.

Execution rules:

- Work one issue at a time.
- Verify Symphony eligibility before work.
- Stop and report the blocker if the issue lacks acceptance criteria.
- Stop and report the blocker if the issue requires crossing into a later
  roadmap phase.
- Preserve the FlowCommander boundary.
- Never directly edit FlowCommander as a side effect of Knowledge Forge issue
  work.
- Include the Linear issue URL in the PR body.
- Do not implement Symphony, vendor Symphony code, or add hidden automation that
  bypasses human review.
- Do not enable auto-merge.
- Do not make OpenAI, GitHub, or other network calls unless the issue explicitly
  requires them and the operator has allowed them.
- Create a focused branch for the issue.
- Keep changes scoped to the issue's acceptance signal.
- Run `kf docs-check` for docs, workflow, publish-boundary, or handoff changes.
- Run `kf validate` before opening a PR when practical.
- Open a PR for human review when the work is ready; do not merge it.

Final PR handoff:

- Include the Linear issue URL, summary, branch, changed files, and validation
  commands with exact results.
- State whether a separate FlowCommander `repo-wiki` update is likely needed.
- If a FlowCommander update is likely needed, explain the downstream wiki or
  publish-boundary impact.
- Call out remaining risks and any checks that could not be run.
