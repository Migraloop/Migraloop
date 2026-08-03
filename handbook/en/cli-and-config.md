# CLI & Config reference

Commands, flags, environment variables, and Deployment config fields for Operators.

## Binary

```text
migraloop <subcommand> [flags]
```

Built from `crates/app` (`Dockerfile` release binary). All Operator subcommands that talk to the Platform Store accept `--platform-store-url` or the env var below.

## Operator CLI subcommands

The `migraloop` Operator CLI currently exposes these subcommands:

### `migrate`

Apply versioned Platform Store schema migrations.

```bash
migraloop migrate --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `apply`

Apply a declarative Deployment config (YAML or JSON). Validates secrets-by-reference, Source/Target kinds, Pipeline specs, Source Prerequisites (when Pipelines reference tables), runs schema discovery + Initial Load as needed, and upserts Deployment/Pipeline state. Re-applying with a semantic Pipeline change (transform/binding and related fields) is the Operator path for a **Pipeline revision**: pause old Delivery for that Pipeline, rebuild its Derived Dataset and re-Deliver as required, then continue incremental work; Shared Bases are not rebuilt. Changing only optional `description` is metadata-only and skips rebuild.

On a real Oracle Source host (not `contract`/`stub`), apply discovers columns and Initial Loads from the live Source over OCI (requires Instant Client; see [Source System](source-system.md)). Contract/stub hosts use the in-process **contract Source catalog** for CI slices (default named fixtures for scenario readability; injectable tables via `MIGRALOOP_CONTRACT_SOURCE_CATALOG`—not a supported production Source mechanism).

```bash
migraloop apply --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL" -f deployment.yaml
```

| Flag | Meaning |
| --- | --- |
| `-f`, `--file` | Path to Deployment config |

### `status`

Report Platform Store health, Deployments, Pipelines, Base Datasets, Sync Health, Source Alignment, Delivery Health, Quarantine rows, Schema Change impacts, and Derived Datasets. Sync Health and Delivery Health both expose `lag=` (remaining work in the current bounded Incremental window). When Downstream is slow, lag rises under backpressure without pausing the Pipeline (ADR-0020). When Poison Change quarantine is active, Delivery Health is `unhealthy` and each quarantined Output Identity is listed as unhealthy / not aligned (ADR-0015). When blocking Schema Change pause is active, Delivery Health is `paused` and `status` lists Schema Change blocking rows (ADR-0009)—distinct from quarantine.

```bash
migraloop status --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `base`

Inspect Base Dataset rows for a Source table.

```bash
migraloop base --table ORDERS [--deployment oracle-to-mongo]
```

| Flag | Meaning |
| --- | --- |
| `--table` | Source table name (required) |
| `--deployment` | Disambiguate when multiple Bases share a table name |

### `target`

Inspect Target documents for a Pipeline collection.

```bash
migraloop target --collection orders [--deployment oracle-to-mongo]
```

| Flag | Meaning |
| --- | --- |
| `--collection` | Target collection name (required) |
| `--deployment` | Disambiguate shared collection names |

### `derived`

Inspect Derived Dataset rows for a Transform Pipeline.

```bash
migraloop derived --pipeline orders_by_customer [--deployment oracle-to-mongo]
```

| Flag | Meaning |
| --- | --- |
| `--pipeline` | Pipeline name (required) |
| `--deployment` | Disambiguate shared Pipeline names |

### `sync`

Run Incremental Capture into Base Datasets, maintain Derived Datasets, then Delivery.

Oracle Incremental Capture is always LogMiner-backed: real hosts use **LogMiner (OCI)**; `host: contract` / `stub` use the in-process contract harness. There is no silent fallback from a real host to the stub catalog. Missing Instant Client or OCI failures fail fast naming LogMiner/OCI.

Paused Pipelines skip Delivery/processing during `sync`; shared Base Dataset Incremental Capture continues so other Pipelines and later resume catch-up stay correct.

When a single Output Identity repeatedly fails Delivery, `sync` retries up to `MIGRALOOP_POISON_MAX_ATTEMPTS` (default `3`), then quarantines that identity with an Operator-visible **ALERT**, continues other changes, and leaves quarantine visible on `status` (ADR-0015 / issue #22).

When Incremental Capture sees **blocking** Source DDL for a Pipeline’s dependencies, `sync` emits an Operator-visible **WARN**, pauses the affected Pipeline(s), and records a Schema Change impact on `status`—without quarantine (ADR-0009 / issue #23). Unaffecting or non-blocking schema changes continue.

```bash
migraloop sync --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `align`

Run a **Source Alignment Check** for Base Datasets (issue #24): verify Base matches Source in a non-realtime, **resource-gated** way, then repair Base from the same Source check reads when misaligned. The check **never writes Source**. Required before treating Base as a reliable Drift baseline; Sync Health alone is not enough.

Default `--max-rows` is `1000` so Operators can schedule the check without a full-table slam. Larger budgets (or repeated runs) cover remaining rows; `status` shows `Source Alignment: aligned|partial|unknown` with checked/mismatched counts from the last run (`partial` means the budget truncated).

```bash
migraloop align --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL" [--table CUSTOMERS] [--deployment oracle-to-mongo] [--max-rows 1000]
```

| Flag | Meaning |
| --- | --- |
| `--table` | Source table / Base Dataset (default: all Bases) |
| `--deployment` | Disambiguate when multiple Bases share a table name |
| `--max-rows` | Max Source rows to read per Base (resource gate; default `1000`) |

### `drift`

Run a **Drift Check** for Pipelines with a Target Binding (issue #25): verify Managed fields on the Target match the platform expected dataset (Base for Direct, Derived for Transform) in a non-realtime, **resource-gated** way. By default, detected Managed drift is **auto-repaired** via the same Managed-only upsert path as Delivery; **non-Managed Target fields are ignored** and never overwritten. For Direct Pipelines, Base must already have Source Alignment (`aligned` or `partial`)—run `migraloop align` first. Auto-repair does not add Source load beyond that baseline.

Default `--max-rows` is `1000` so Operators can schedule the check without a full-collection slam. Larger budgets (or repeated runs) cover remaining Output Identities; `status` shows `Drift: ok|partial|unknown` with checked/mismatched counts from the last run (`partial` means the budget truncated).

```bash
migraloop drift --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL" [--pipeline customers] [--deployment oracle-to-mongo] [--max-rows 1000]
```

| Flag | Meaning |
| --- | --- |
| `--pipeline` | Pipeline name (default: all Pipelines with a Target Binding) |
| `--deployment` | Disambiguate when multiple Deployments share a Pipeline name |
| `--max-rows` | Max Output Identities to check per Pipeline (resource gate; default `1000`) |

### `pause`

Pause one Pipeline without restarting the Deployment (ADR-0007). Stops further Delivery/processing for that Pipeline; durable Base/checkpoint state is retained. Other Pipelines keep running.

```bash
migraloop pause --pipeline customers [--deployment oracle-to-mongo]
```

| Flag | Meaning |
| --- | --- |
| `--pipeline` | Pipeline name (required) |
| `--deployment` | Disambiguate when multiple Deployments share a Pipeline name |

### `resume`

Resume a paused Pipeline. Clears the durable pause flag and catch-up Delivers from current Platform Store Base/Derived state (including deletes for identities that disappeared while paused), then later `sync` continues Incremental Delivery.

```bash
migraloop resume --pipeline customers [--deployment oracle-to-mongo]
```

| Flag | Meaning |
| --- | --- |
| `--pipeline` | Pipeline name (required) |
| `--deployment` | Disambiguate when multiple Deployments share a Pipeline name |

### `remove`

Remove one Pipeline without restarting the Deployment (ADR-0007). Stops Delivery/processing for that Pipeline. Shared Base Datasets remain when other Pipelines still reference them; Bases no longer referenced are pruned. `status` no longer lists the Pipeline as active. Target documents already Delivered are left in place (cease Delivery, not wipe). To keep the Pipeline omitted across a later `apply`, also remove it from the declarative config.

```bash
migraloop remove --pipeline customers [--deployment oracle-to-mongo]
```

| Flag | Meaning |
| --- | --- |
| `--pipeline` | Pipeline name (required) |
| `--deployment` | Disambiguate when multiple Deployments share a Pipeline name |

### `run`

Migrate on startup, then keep the app process alive (compose default command).

```bash
migraloop run --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `lab`

Local Sync Lab Fixture and Lab Scenarios (ADR-0025). Provisions a disposable real stack—Oracle Source (Lab-satisfied Source Prerequisites), MongoDB Target, Platform Store, and app. Bring-up does **not** apply a sample Deployment or Pipelines. Operators then list/run selectable **Lab Scenarios** that apply Deployments and drive Sync/Delivery on the real product path inside a **Scenario Namespace**. Requires Docker Compose and the repo `lab/` directory (or `--lab-dir`). Scenario `apply`/`sync` need host Oracle Instant Client (`LD_LIBRARY_PATH`). Nested Docker hosts that fail image extract on overlay whiteouts need dockerd `fuse-overlayfs` or `vfs` (containerd snapshotter disabled); `lab up` prints that hint on whiteout/`EPERM` failures. **Cursor Cloud** applies `fuse-overlayfs` in environment `install`/`start`—see [Developer local setup](developer-local-setup.md) and [Deployment](deployment.md).

```bash
migraloop lab up [--lab-dir lab]
migraloop lab status [--lab-dir lab]
migraloop lab down [--lab-dir lab]
migraloop lab scenario list [--lab-dir lab]
migraloop lab scenario run <scenario-id> [--lab-dir lab] [--auto-remove]
migraloop lab scenario remove <scenario-id> [--lab-dir lab]
```

| Subcommand | Meaning |
| --- | --- |
| `up` | Bring up the disposable Fixture; print connection details when ready |
| `status` | Report Fixture readiness (engines + Oracle prerequisites + Platform Store), plus which Scenario Namespace is **active** (a run in progress) or **leftover** (kept after a finished run), or `(none)` for each. Also shows `Deployment: (none)` / `Pipeline: (none)` until you apply config or run a Lab Scenario — use the Scenario run / leftover lines instead of guessing from those alone |
| `down` | Tear down containers and volumes |
| `scenario list` | List selectable Lab Scenarios from on-disk recipes under `--lab-dir` (`lab/scenarios/<id>/recipe.yaml` + `deployment.yaml`, plus a registered runner). Summaries come from each recipe—for example `direct-pipeline`, `rt-project`, `rt-filter`, `transform-pipeline`, `concurrent-source-workload`, `bulk-load`, `idempotent-redelivery`, `pause-resume`, `remove-pipeline`, `change-pipeline`, `poison-quarantine`, `schema-change-pause`, `source-alignment`, `drift-check`, `bounded-backpressure`. The list also reports shipped-capability coverage (complete vs gaps; see `lab/scenarios/COVERAGE.md`) |
| `scenario run` | Run one Lab Scenario by id. Rejects if another Scenario run is active. Refuses Source/Target bindings that are not Lab Fixture engines (customer/production databases are out of scope for Lab — use ordinary `apply`/`sync` for those). Re-running the same Scenario fully removes its Namespace before recreate. Reports pass/fail plus `duration_ms`, rows/throughput, lag, and Scenario-defined thresholds such as settle time or bulk-load lag/throughput/duration when present (correctness and operational metrics with equal weight). `rt-project` / `rt-filter` exercise shipped Rich Transform `project` and `filter` operators; `concurrent-source-workload` runs parallel Source sessions inside one Scenario; `bulk-load` bulk-inserts ~100k Source rows and can fail on metric thresholds independently of correctness; `idempotent-redelivery` forces duplicate-safe re-Delivery of the same Output Identities and checks Managed Target outcomes stay correct; `pause-resume` exercises `pause` / `resume` CLI verbs (one Pipeline stops Delivery while another continues; resume catch-up from durable Base); `remove-pipeline` exercises `remove` (cease Delivery; Shared Base kept for remaining Pipelines; status no longer lists the Pipeline); `change-pipeline` exercises Pipeline revision via `apply` (pause old Delivery → rebuild that Pipeline's Derived / re-Deliver; Shared Bases not rebuilt; metadata-only `description` skips rebuild); `poison-quarantine` quarantines one poison Output Identity after bounded retries with an ALERT while the Pipeline continues and `status` shows unhealthy / not aligned; `schema-change-pause` warns and pauses the affected Pipeline on blocking DDL (distinct from poison quarantine); `source-alignment` detects Base≠Source, repairs Base from Source reads only, and exercises resource-gated `--max-rows`; `drift-check` detects Managed-field Target drift, default-auto-repairs Managed fields, preserves non-Managed fields, and exercises resource-gated `--max-rows`; `bounded-backpressure` applies Downstream Delivery delay with a tiny queue capacity, asserts Backpressure / lag without pause, then catch-up. A second Scenario run stays rejected. Default keep-on-finish leaves the Namespace for live `base`/`derived`/`target` inspection; pass `--auto-remove` to delete it after a successful run |
| `scenario remove` | Fully remove a Scenario Namespace (Source tables, Target collections, Platform Store Deployment) without starting a run. Rejects if another Scenario is active. Idempotent when already absent |

| Flag | Meaning |
| --- | --- |
| `--lab-dir` | Directory containing Lab `compose.yaml` (default: `lab`) |
| `--auto-remove` | On `scenario run` only: after a successful run, fully remove the Scenario Namespace (opt-in; failures still keep the Namespace for debugging) |

Lab is manual verification—not the Release Quality Gate and not the contract/stub LogMiner harness. The selectable Scenario catalog is a feature-time completeness surface (ADR-0025), not a CI suite: do not add a release-gate job that runs the entire catalog. Scenario recipe conventions, the authoring path, and shipped-capability coverage gaps are in [Developer local setup](developer-local-setup.md), `lab/scenarios/README.md`, and `lab/scenarios/COVERAGE.md`. For DB-level restore/load **outside** Scenario recipes (SQL/mongosh/dumps into Lab Oracle/Mongo using `lab status` connection details, then ordinary `apply` / `status` / inspect / `sync`), see `lab/escape-hatch/` and [Deployment](deployment.md)—that escape hatch is not a second Scenario model and not CI.

## Public environment contract

| Variable | Meaning |
| --- | --- |
| `MIGRALOOP_PLATFORM_STORE_URL` | Platform Store connection URL (`postgres://...`) used by Operator CLI commands and compose `app` |
| Secret env names referenced from config | Any names you put in `password.fromEnv` (for example `ORACLE_PASSWORD`, `MONGO_PASSWORD`) must be present in the process environment at apply/sync time |
| `LD_LIBRARY_PATH` | For real Oracle hosts: directory of Oracle Instant Client libraries (required at apply/sync runtime; not used by `contract`/`stub`) |
| `MIGRALOOP_CONTRACT_SOURCE_CATALOG` | Contract/stub hosts only: path to a JSON file that merges/overrides harness catalog tables for schema discovery + Initial Load (CI / local slices; not a production Source mechanism) |
| `MIGRALOOP_POISON_MAX_ATTEMPTS` | Bounded Delivery retries before Poison Change quarantine (default `3`; must be > 0) |
| `MIGRALOOP_DELIVERY_POISON_IDENTITIES` | Test/Lab fault injection only: comma-separated Output Identity keys that always fail Delivery so quarantine can be exercised (not a production Operator control) |
| `MIGRALOOP_INJECT_SCHEMA_CHANGES` | Test/Lab injection only: path to a JSON file of Schema Change events (`scn`, `table`, `kind`, `columns`, …) so blocking DDL warn+pause can be exercised without LogMiner DDL capture (not a production Operator control) |
| `MIGRALOOP_SYNC_QUEUE_CAPACITY` | Bounded Incremental Capture / Delivery window size (default `256`; must be > 0). Stages never materialize more pending changes than this capacity (ADR-0020) |
| `MIGRALOOP_DELIVERY_DELAY_MS` | Test/Lab fault injection only: artificial Downstream Delivery delay in milliseconds so bounded backpressure and visible lag can be exercised (not a production Operator control) |
| `MIGRALOOP_INJECT_LOGMINER_CONTENTS` | Test/Lab injection only: path to a JSON file of extra contract LogMiner contents (`contents: [{scn, operation, table_name, identity, after_image}, …]`) so a large Incremental backlog can be exercised on `contract`/`stub` hosts (not a production Operator control) |
| Lab disposable defaults | After `migraloop lab up`: `ORACLE_PASSWORD=lab_oracle`, `MONGO_PASSWORD=lab_mongo`, Platform Store URL `postgres://migraloop:migraloop@127.0.0.1:5432/migraloop`, Mongo URI `mongodb://migraloop:lab_mongo@127.0.0.1:27017/lab?authSource=admin` (local Lab only) |

### Contract-harness Source Prerequisite probes (host `stub` / `contract` only)

Env names and defaults for the in-process LogMiner harness live in [Source System](source-system.md) (`MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING`, `MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING`, `MIGRALOOP_STUB_REDO_RETENTION_HOURS`).

## Deployment config contract

| Field | Required | Notes |
| --- | --- | --- |
| `apiVersion` | yes | `migraloop.dev/v1` |
| `kind` | yes | `Deployment` |
| `metadata.name` | yes | Non-empty Deployment name |
| `spec.source` | yes | See below |
| `spec.target` | yes | See below |
| `spec.pipelines` | no | Default `[]` (Deployment-only apply) |

### `spec.source` / `spec.target`

| Field | Source | Target | Notes |
| --- | --- | --- | --- |
| `kind` | `oracle` | `mongodb` | v1 fixed pair |
| `host` | yes | yes | Source `stub`/`contract` → LogMiner harness + contract-catalog Initial Load; any other host → live OCI Initial Load + LogMiner |
| `port` | yes | yes | Valid TCP port |
| `database` | yes | yes | |
| `username` | yes | yes | Also the default Oracle schema/owner when Pipeline `source.schema` is omitted |
| `password` | yes | yes | Exactly one of `fromEnv`, `fromFile`, `fromDockerSecret` |
| `timezone` | optional | n/a | IANA or `±HH:MM` for naive temporals |

Docker secrets resolve from `/run/secrets/<name>`.

### Pipeline entries (`spec.pipelines[]`)

| Field | Notes |
| --- | --- |
| `name` | Non-empty |
| `mode` | `direct` or `transform` |
| `description` | Optional Operator-facing comment; metadata-only — changing it alone does not rebuild Derived or re-Deliver |
| `source.table` | Required; optional `source.schema` (live Oracle owner; defaults to Source `username`) |
| `target.collection` | Target Binding; omit only for Base-only experiments |
| `fields` | Map of field → `{ as: string \| omit }` |
| `outputIdentity` | Required for `transform` |
| `transform` | Declarative steps; required for `transform`; forbidden for `direct` |

Minimal Direct example:

```yaml
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: oracle-to-mongo
spec:
  source:
    kind: oracle
    host: db.example.com
    port: 1521
    database: ORCL
    username: sync_user
    password:
      fromEnv: ORACLE_PASSWORD
  target:
    kind: mongodb
    host: mongo.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
  pipelines:
    - name: orders
      mode: direct
      source:
        table: ORDERS
      target:
        collection: orders
```

## Related chapters

- Progressive path: [Start here](start-here.md)
- Secrets and TLS: [Security](security.md)
- Chapter deep-dives: [Deployment](deployment.md), [Pipeline](pipeline.md), [Source System](source-system.md)
