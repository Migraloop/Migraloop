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

Apply a declarative Deployment config (YAML or JSON). Validates secrets-by-reference, Source/Target kinds, Pipeline specs, Source Prerequisites (when Pipelines reference tables), runs schema discovery + Initial Load as needed, and upserts Deployment/Pipeline state.

On a real Oracle Source host (not `contract`/`stub`), apply discovers columns and Initial Loads from the live Source over OCI (requires Instant Client; see [Source System](source-system.md)). Contract/stub hosts keep the in-process fixture catalog for CI slices.

```bash
migraloop apply --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL" -f deployment.yaml
```

| Flag | Meaning |
| --- | --- |
| `-f`, `--file` | Path to Deployment config |

### `status`

Report Platform Store health, Deployments, Pipelines, Base Datasets, Sync Health, Delivery Health, and Derived Datasets.

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

```bash
migraloop sync --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `run`

Migrate on startup, then keep the app process alive (compose default command).

```bash
migraloop run --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `lab`

Local Sync Lab Fixture and Lab Scenarios (ADR-0025). Provisions a disposable real stack—Oracle Source (Lab-satisfied Source Prerequisites), MongoDB Target, Platform Store, and app. Bring-up does **not** apply a sample Deployment or Pipelines. Operators then list/run selectable **Lab Scenarios** that apply Deployments and drive Sync/Delivery on the real product path inside a **Scenario Namespace**. Requires Docker Compose and the repo `lab/` directory (or `--lab-dir`). Scenario `apply`/`sync` need host Oracle Instant Client (`LD_LIBRARY_PATH`).

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
| `status` | Report Fixture readiness (engines + Oracle prerequisites + Platform Store). Shows `Deployment: (none)` / `Pipeline: (none)` until you apply config or run a Lab Scenario |
| `down` | Tear down containers and volumes |
| `scenario list` | List selectable Lab Scenarios in the catalog (for example `direct-pipeline`, `transform-pipeline`, `concurrent-source-workload`) |
| `scenario run` | Run one Lab Scenario by id. Rejects if another Scenario run is active. Re-running the same Scenario fully removes its Namespace before recreate. Reports pass/fail plus `duration_ms`, rows/throughput, and Scenario-defined thresholds such as settle time when present (correctness and operational metrics with equal weight). `concurrent-source-workload` runs parallel Source sessions inside one Scenario; a second Scenario run stays rejected. Default keep-on-finish leaves the Namespace for live `base`/`derived`/`target` inspection; pass `--auto-remove` to delete it after a successful run |
| `scenario remove` | Fully remove a Scenario Namespace (Source tables, Target collections, Platform Store Deployment) without starting a run. Rejects if another Scenario is active. Idempotent when already absent |

| Flag | Meaning |
| --- | --- |
| `--lab-dir` | Directory containing Lab `compose.yaml` (default: `lab`) |
| `--auto-remove` | On `scenario run` only: after a successful run, fully remove the Scenario Namespace (opt-in; failures still keep the Namespace for debugging) |

Lab is manual verification—not the Release Quality Gate and not the contract/stub LogMiner harness. See [Deployment](deployment.md) and [Developer local setup](developer-local-setup.md).

## Public environment contract

| Variable | Meaning |
| --- | --- |
| `MIGRALOOP_PLATFORM_STORE_URL` | Platform Store connection URL (`postgres://...`) used by Operator CLI commands and compose `app` |
| Secret env names referenced from config | Any names you put in `password.fromEnv` (for example `ORACLE_PASSWORD`, `MONGO_PASSWORD`) must be present in the process environment at apply/sync time |
| `LD_LIBRARY_PATH` | For real Oracle hosts: directory of Oracle Instant Client libraries (required at apply/sync runtime; not used by `contract`/`stub`) |
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
| `host` | yes | yes | Source `stub`/`contract` → LogMiner harness + fixture Initial Load; any other host → live OCI Initial Load + LogMiner |
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
