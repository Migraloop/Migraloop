# Lab Scenario recipes

Feature-time authoring path for selectable **Lab Scenarios** (ADR-0025 / issues #65–#66).

Shipped-capability coverage (and visible gaps for not-yet-shipped surfaces) lives in [COVERAGE.md](./COVERAGE.md). `migraloop lab scenario list` reports whether the selectable catalog is complete for currently shipped capabilities—operators still pick Scenarios individually; this is not a CI suite.

Each Scenario lives under `lab/scenarios/<id>/` with:

| File | Role |
| --- | --- |
| `recipe.yaml` | Recipe-driven runner interface: id, summary, Scenario Namespace, workload steps, checks, thresholds |
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
  lifecycle:                    # required with product_path prepare_namespace (#201)
    tables:
      - name: LAB_MY_TABLE      # must match source_tables
        columns: |
          ID NUMBER(10) PRIMARY KEY,
          NAME VARCHAR2(100) NOT NULL
        # supplemental_logging: true  # default
    seed_sql: |
      INSERT INTO LAB_MY_TABLE (ID, NAME) VALUES (1, 'Alice');
    # mutate_sql: |             # optional; omit for thin mutate escapes
    #   UPDATE LAB_MY_TABLE SET NAME = 'Alicia' WHERE ID = 1;
deployment_config: deployment.yaml
workload:
  concurrency: serial           # or parallel (intra-Scenario only)
  steps:                        # human-readable narrative (always required)
    - prepare Namespace …
    - apply via real product path
    - mutate / drive Source workload
    - sync / settle and assert
  product_path:                 # optional; when set, shared runner executes these
    steps:
      - prepare_namespace
      - product_apply
      - mutate
      - product_sync
      - assert
    apply:
      require_initial_load: true
      require_delivery: false     # set true when apply must report Delivery
      require_derived: false      # set true for Transform Derived materialization
    sync:
      require_logminer: true
      # allow_fail: true         # ops escapes that intentionally stop mid-sync
checks:
  correctness:                  # executable inspect vocabulary (#205)
    - surface: base             # base | derived | target | status
      table: LAB_EXAMPLE
      present:
        - { field: NAME, value: Alicia }
      absent:
        - { field: NAME, value: Bob }
    - surface: target
      collection: lab_example
      present:
        - { field: NAME, value: Alicia }
      field_absent: [EMAIL]     # optional; also contains/not_contains/amount_*/row_count/document_count
thresholds:                     # optional; equal weight with correctness
  max_settle_ms: 300000
  max_lag: 0
  max_duration_ms: 600000
  min_rows_per_s: 50.0
```

Typed `workload.product_path` steps (issues #173 / #178 / #179 / #201 / #205): `prepare_namespace`,
`product_apply`, `mutate`, `product_sync`, `assert`. The shared runner owns Namespace
lifecycle (wipe / CREATE / supplemental logging / seed, and optional `mutate_sql`),
executable `checks.correctness` (Managed field present/absent, Derived/Target inspect,
status text, row/document counts), plus apply/sync on the real product CLI path.
Scenario hooks only supply rare escapes (e.g. typed SyncOptions CLI flags for poison /
delay / fail-after / queue capacity, schema-change inject env for the DDL file bridge,
parallel Source sessions, CLI pause / remove / revision verbs, generated backlog inserts,
`sync.allow_fail` mid-window stops, settle orchestration). All selectable catalog
Scenarios declare `product_path`, `namespace.lifecycle`, and runnable `checks.correctness`.

Optional `sync.allow_fail: true` keeps going after a non-zero sync exit so ops Scenarios
(backpressure / observability) can observe mid-window stops, then finish in hooks.

## Adding a Scenario while building a feature

1. Create `lab/scenarios/<id>/recipe.yaml` + `deployment.yaml` (reuse the operator Deployment format). Prefer `workload.product_path` for the common prepare→apply→mutate→sync→assert path, declare `namespace.lifecycle` (tables + `seed_sql`, optional `mutate_sql`) so the shared runner owns Namespace wipe/prepare, and declare executable `checks.correctness` inspect expectations.
2. Register the Scenario id and implement thin hooks only for rare escapes (not copy-paste prepare/remove triples or isomorphic inspect asserts). Recipe `workload` / `namespace.lifecycle` / `checks.correctness` / `thresholds` are the recipe-driven runner interface — do not duplicate threshold values, Namespace wipe/prepare SQL, Managed present/absent inspect arms, or the common product-path sequence as Rust constants.
3. Confirm `migraloop lab scenario list` shows the new summary from the recipe.
4. Manually verify with `migraloop lab scenario run <id>` on a Lab Fixture (`lab up`). Keep ignored CLI-seam coverage for the happy path; do **not** add a CI Release Quality Gate that runs the full catalog (ADR-0025 / ADR-0011).

Operator/Developer handbook: `handbook/*/developer-local-setup.md` (Local Sync Lab + authoring).
