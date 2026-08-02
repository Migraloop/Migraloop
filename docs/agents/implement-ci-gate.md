# Implement CI gate (repo overlay)

Repo-owned closeout rules for **`/implement`**. This does **not** fork or patch upstream skill files under `.agents/skills/`. Discover it the same way as other agent contracts: short pointer in `AGENTS.md`, details in this file.

## When it applies

- The agent ran (or is closing out) **`/implement`**, **and**
- A pull request already exists for the work branch.

If there is **no PR**, skip this gate (do not open a PR solely to satisfy it).

## Definition of done

With a PR in scope, `/implement` is **not done** until **every check** on that PR is green (all checks, not only required/protected ones).

Local test green is necessary but not sufficient.

## Pending checks — exponential backoff

Poll CI until every check has a terminal state (`pass` / `fail` / equivalent), using exponential backoff:

| Parameter | Value |
| --- | --- |
| First wait before a poll | 30s |
| Backoff | ×2 after each poll (30s → 1m → 2m → 4m → 8m …) |
| Per-interval cap | 8m |
| Total wait cap | 30m |

How to read status (either is fine):

- `ManagePullRequest` → `get_ci_status` when available
- `gh pr checks <number>` / `gh pr view --json statusCheckRollup`

If the total wait cap elapses and any check is still pending, stop and report **`CI: pending-timeout`**. That is **not** a pass.

## Failed checks — attribute, then act

On any red check, **read the failing job log** and attribute before spending more turns:

| Attribution | Required action |
| --- | --- |
| Caused by this diff (tests, types, lint, handbook-guard, compile, etc.) | Fix → push → poll again until all green (same backoff). Repeat until green or the failure is re-attributed as not this diff. |
| Not this diff (permissions, secrets, runners, upstream flake, unrelated infra) | **Do not** burn turns on blind retries. Stop and report **`CI: blocked`** with the attribution and log highlights. |

Do **not** “try one more speculative fix” when the log already points at non-diff causes. The goal is CI green when the agent can honestly get there — not token burn on hopeless retries.

If the agent fixes what it can and checks remain red for this-diff causes it cannot clear, report **`CI: fail`** with what was tried and the remaining failures. That is still **not** a pass.

## Mandatory closeout line

When a PR is in scope, the agent’s final message **must** include exactly one of:

- `CI: pass`
- `CI: fail`
- `CI: blocked`
- `CI: pending-timeout`

Rules:

- Omitting this line is an incomplete closeout.
- Only `CI: pass` may be paired with language like “everything passed”, “all done”, or “ready to merge”.
- For any other CI status, state the status and facts; do **not** claim the implement finished successfully.

## Related

- Spec: GitHub issue #75
