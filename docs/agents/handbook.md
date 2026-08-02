# Handbook

How engineering skills should keep the Operator / Developer product handbook accurate when changing visible behavior.

Architecture decision: [ADR-0027](../adr/0027-multilingual-handbook-three-way-separation.md). Human portal: [`handbook/`](../../handbook/README.md).

## Three-way separation

Keep these trees separate — do not mix audiences or obligations into one place:

| Tree | Role | Path |
| --- | --- | --- |
| Human handbook | Product docs for Operators and Developers (three locales) | `handbook/{en,zh-TW,zh-CN}/` |
| Agent contracts | Skill discovery and review standards (this file and siblings) | `docs/agents/` |
| CI machine config | Touchpoints map, CLI surface snapshot, guard entrypoint | `ci/handbook/` |

`CONTEXT.md`, `docs/adr/`, and `docs/agents/` are engineering material. They are **outside** the three-locale product-doc obligation — do not translate them into zh-TW / zh-CN to satisfy handbook rules.

Discover this contract the same way as domain docs: the short pointer in `AGENTS.md`, plus this file. Do **not** fork or patch upstream skill files under `.agents/skills` to hardcode handbook reads.

## When handbook duties apply

Update the handbook when a change alters **Operator- or Developer-visible** behavior or contracts, including (non-exhaustive):

- Operator CLI subcommands, flags, env vars, or Deployment / Pipeline config fields Operators set
- Install / compose / Dockerfile paths Operators or Developers follow
- Source Prerequisites, Required Privileges, supported Source types, Target Binding, Managed Columns
- Sync Health / Delivery Health signals, status output, operations controls Operators rely on
- Developer local-setup steps (clone, build, compose Platform Store, tests)

Internal refactors, pure test changes, and engineering-only ADR / glossary edits with **no** Operator/Developer-visible impact do not require handbook page churn — use the exemption etiquette below when a hard-gated path still changed.

## Three-locale update rule

English (`handbook/en`) is canonical. Traditional Chinese (`handbook/zh-TW`) and Simplified Chinese (`handbook/zh-CN`) are required peers.

When a functional handbook page must change:

1. Update the matching page path in **all three** locale trees in the **same** change.
2. Keep locale trees **path-isomorphic** (same relative chapter paths under each locale).
3. Resolve meaning disputes to the English page; translations follow English.

## Hard guards vs soft obligation

**Hard guards** (CI / `handbook-guard`) catch reliable mechanical cases:

- Three-locale path isomorphism
- High-signal path touchpoints in `ci/handbook/touchpoints.json` requiring mapped handbook pages in all locales
- CLI surface snapshot drift (`ci/handbook/cli-surface.txt`) plus every subcommand mentioned in each locale’s `cli-and-config.md`

**Soft obligation** (this contract, the PR handbook checklist, and `/code-review` Standards) covers Operator/Developer-visible behavior CI cannot detect — for example logic or messaging changes outside gated paths that still change what Operators or Developers see or must configure.

Soft obligation does **not** weaken hard guards. Passing CI without updating the right functional chapters is still a Standards miss when visibility changed.

`/code-review` Standards must treat this file as a **documented repo standard**. Cite rules here when a diff changes Operator/Developer-visible behavior without the matching three-locale handbook updates (or a valid exemption).

## Exemption etiquette (`docs-not-needed`)

Use the exemption only when a **hard-gated** path changed with **no** Operator/Developer-visible doc impact (for example a gated-path refactor that does not change contracts Operators see).

Required together:

1. PR label `docs-not-needed`
2. A rationale line in the PR body:
   `docs-not-needed: <why there is no Operator/Developer-visible doc impact>`

Do not use the exemption to skip real visible-behavior doc updates. Local guard runs that short-circuit on the same flag still need that written rationale in the PR.

## Definition of done (Operator/Developer-visible changes)

A change that affects Operators or Developers is done only when all of the following hold:

1. Matching handbook chapters under `handbook/en`, `handbook/zh-TW`, and `handbook/zh-CN` reflect the new behavior (or a valid `docs-not-needed` exemption applies).
2. If the Operator CLI subcommand surface changed: `ci/handbook/cli-surface.txt` and `cli-and-config.md` in all three locales are updated together.
3. If a new high-signal surface needs gating: `ci/handbook/touchpoints.json` gained or updated rows (machine config only — not translated).
4. Handbook vocabulary matches `CONTEXT.md` (Deployment, Pipeline, Sync Health, Delivery Health, Operator, and related terms).
5. The Handbook guard passes the same way CI runs it (see PR template / Developer local-setup chapter).

## Related

- Domain consumer guide: [domain.md](domain.md) (glossary / ADRs; cross-links here for visible-behavior changes)
- ADR: [0027-multilingual-handbook-three-way-separation.md](../adr/0027-multilingual-handbook-three-way-separation.md)
- Spec: GitHub issue #47
