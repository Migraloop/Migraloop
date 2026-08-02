# Lab Scenario recipes

Feature-time authoring path for selectable **Lab Scenarios** (ADR-0025 / issues #65–#66).

Shipped-capability coverage (and visible gaps for not-yet-shipped surfaces) lives in [COVERAGE.md](./COVERAGE.md). `migraloop lab scenario list` reports whether the selectable catalog is complete for currently shipped capabilities—operators still pick Scenarios individually; this is not a CI suite. The Lab↔CI twin completeness ladder (non-ignored contract-path evidence under `rqg-integration`) is tracked in [`docs/rqg/CI_TWIN_COVERAGE.md`](../../docs/rqg/CI_TWIN_COVERAGE.md).

Each Scenario lives under `lab/scenarios/<id>/` with:

| File | Role |
| --- | --- |
| `recipe.yaml` | Catalog metadata: id, summary, Scenario Namespace, workload steps, checks, thresholds |
| `deployment.yaml` | Real product Deployment config applied via `migraloop apply` |

`migraloop lab scenario list` lists **selectable** Scenarios: registered runners that have both files under `--lab-dir`. Summaries come from `recipe.yaml` — not a separate hardcoded table.

## Recipe shape

```yaml
id: my-scenario                 # must match directory name
summary: One-line catalog blurb
namespace:
  source_tables: [LAB_MY_TABLE]
  target_collections: [lab_my_collection]
  deployment: lab-my-scenario
  pipelines: [lab-my-pipeline]
deployment_config: deployment.yaml
workload:
  concurrency: serial           # or parallel (intra-Scenario only)
  steps:
    - prepare Namespace …
    - apply via real product path
    - mutate / drive Source workload
    - sync / settle and assert
checks:
  correctness:
    - Expected Managed Base/Target/Derived outcomes
thresholds:                     # optional; equal weight with correctness
  max_settle_ms: 300000
  max_lag: 0
  max_duration_ms: 600000
  min_rows_per_s: 50.0
```

## Adding a Scenario while building a feature

1. Create `lab/scenarios/<id>/recipe.yaml` + `deployment.yaml` (reuse the operator Deployment format).
2. Implement Namespace prepare/remove, workload, checks, and thresholds in `crates/cli/src/lab_scenario.rs`, and register the runner id.
3. Confirm `migraloop lab scenario list` shows the new summary from the recipe.
4. Manually verify with `migraloop lab scenario run <id>` on a Lab Fixture (`lab up`). Keep ignored CLI-seam coverage for the happy path; do **not** add a CI Release Quality Gate that runs the full catalog (ADR-0025 / ADR-0011).

Operator/Developer handbook: `handbook/*/developer-local-setup.md` (Local Sync Lab + authoring).
