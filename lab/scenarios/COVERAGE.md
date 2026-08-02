# Lab Scenario catalog coverage

Policy: ADR-0025 / issue #66. Selectable Scenarios are manual verification—not a CI Release Quality Gate (ADR-0011).

## Catalog status

**Catalog-complete for currently shipped first-class capabilities.**

`migraloop lab scenario list` repeats this claim only when every shipped capability below has a selectable Scenario. Operators still pick individual Scenarios; do **not** add a job that runs the full catalog as a release gate.

## Shipped capabilities ↔ selectable Scenarios

| Capability | Scenario id(s) |
| --- | --- |
| Direct Pipeline Initial Load + insert/update/delete | `direct-pipeline` |
| Multi-table Transform Pipeline (`groupBy` / `sum`) | `transform-pipeline` |
| Rich Transform `project` | `rt-project` |
| Rich Transform `filter` | `rt-filter` |
| Rich Transform `groupBy` / `sum` (also under contention) | `transform-pipeline`, `concurrent-source-workload` |
| Intra-Scenario concurrent Source workload | `concurrent-source-workload` |
| Bulk load (~100k) with fail-able metric thresholds | `bulk-load` |
| Idempotent re-delivery / duplicate-safe Delivery | `idempotent-redelivery` |
| Dedicated Pipeline pause/resume CLI verbs | `pause-resume` |
| Dedicated Pipeline remove CLI verb | `remove-pipeline` |
| Pipeline revision change via `apply` (Derived rebuild / metadata-only skip) | `change-pipeline` |
| Poison Change quarantine on Operator `status` | `poison-quarantine` |

## Visible gaps (not yet shipped)

These are **not** covered and must stay listed until the capability ships with a Scenario in the same change (ADR-0025). Missing rows here mean the Lab must not claim catalog-complete for that surface.

| Capability | Why gated |
| --- | --- |
| Rich Transform rename/remove, equiLookup, unwind, count/min/max/avg, distinct/addToSet, union | Domain roadmap; not accepted by the CLI transform parser yet |

When shipping any gap above: add `lab/scenarios/<id>/`, register the runner, update this table, and only then restate catalog-complete for the expanded shipped surface.
