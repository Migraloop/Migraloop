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

Apply a declarative Deployment config (YAML or JSON). Validates secrets-by-reference, Source/Target kinds, Pipeline specs, Source Prerequisites (when Pipelines reference tables), runs Initial Load as needed, and upserts Deployment/Pipeline state.

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

```bash
migraloop sync --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

### `run`

Migrate on startup, then keep the app process alive (compose default command).

```bash
migraloop run --platform-store-url "$MIGRALOOP_PLATFORM_STORE_URL"
```

## Public environment contract

| Variable | Meaning |
| --- | --- |
| `MIGRALOOP_PLATFORM_STORE_URL` | Platform Store connection URL (`postgres://...`) used by Operator CLI commands and compose `app` |
| Secret env names referenced from config | Any names you put in `password.fromEnv` (for example `ORACLE_PASSWORD`, `MONGO_PASSWORD`) must be present in the process environment at apply/sync time |

### Contract-harness Source Prerequisite probes (host `stub` / `contract` only)

| Variable | Meaning | Default |
| --- | --- | --- |
| `MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING` | `on` / `off` | `on` |
| `MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING` | `all`, empty, or comma-separated tables | `all` |
| `MIGRALOOP_STUB_REDO_RETENTION_HOURS` | Reported redo retention hours | `72` |

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
| `host` | yes | yes | Source `stub`/`contract` → LogMiner harness |
| `port` | yes | yes | Valid TCP port |
| `database` | yes | yes | |
| `username` | yes | yes | |
| `password` | yes | yes | Exactly one of `fromEnv`, `fromFile`, `fromDockerSecret` |
| `timezone` | optional | n/a | IANA or `±HH:MM` for naive temporals |

Docker secrets resolve from `/run/secrets/<name>`.

### Pipeline entries (`spec.pipelines[]`)

| Field | Notes |
| --- | --- |
| `name` | Non-empty |
| `mode` | `direct` or `transform` |
| `source.table` | Required; optional `source.schema` |
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
