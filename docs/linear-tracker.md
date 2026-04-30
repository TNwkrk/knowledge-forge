# Linear Tracker Cutover

Knowledge Forge uses Linear for Symphony/Codex planning while keeping GitHub as the engineering execution surface.

## Operating model

- Linear is the planning and control plane.
- GitHub remains code hosting, pull requests, CI, Dependabot, and security.
- The Linear workspace uses one team for the FlowCommander ecosystem: `FlowCommander`, team key `FC`.
- Repository routing is by labels.

## Repo labels

Exactly one repository label is required:

- `repo:flowcommander`
- `repo:knowledge-forge`

Knowledge Forge work must use `repo:knowledge-forge`.

## Statuses

- `Ready` — eligible for Symphony pickup when all eligibility rules pass.
- `Human Review` — PR is open and awaiting human review.
- `Rework` — changes requested or validation failed.
- `Merging` — approved and ready for a human-controlled merge path.
- `Done` — completed after the accepted review and merge process.

## Labels

Required Symphony label:

- `symphony-ready`

Disqualifying labels:

- `human-only`
- `symphony-blocked`
- `needs-spec`
- `blocked`

## Symphony eligibility

An issue is eligible for Symphony pickup only when all of these are true:

- Team is `FlowCommander` (`FC`).
- Status is `Ready`.
- Label `symphony-ready` is present.
- Exactly one repo label is present.
- For Knowledge Forge, that repo label is `repo:knowledge-forge`.
- None of `human-only`, `symphony-blocked`, `needs-spec`, or `blocked` is present.
- There are no open blockers.
- Acceptance criteria are concrete and testable.
- Auto-merge is not requested or enabled.

## Pull request handoff

PR bodies must include:

- Linear issue URL.
- Summary of the change.
- Files changed.
- Validation commands and exact results.
- Whether a FlowCommander repo-wiki update is likely needed.
- Hosted Supabase impact, including `none` when applicable.
- Confirmation that GitHub Project retirement was not performed.
- Confirmation that auto-merge was not enabled.

## Transition posture

GitHub Projects are not retired in this cutover. Existing GitHub Project automation remains untouched until a later, explicit retirement task.

FC-3 owns later PR-to-Linear status sync. Until then, agents should report PR status clearly in handoffs and leave Linear status transitions to the approved process.
