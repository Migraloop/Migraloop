# Deployment

A **Deployment** pairs exactly **one Source System** with exactly **one Target System** and hosts one or more **Pipelines** between them. Wanting a different database pair means another Deployment—not multi-database fan-in inside one Deployment.

## Install shape (v1)

Default production-shaped install is **one install, two containers**:

| Service | Role |
| --- | --- |
| `platform-store` | Bundled PostgreSQL **Platform Store** (product-locked engine) |
| `app` | `migraloop` binary (`Dockerfile` builds release `migraloop-app`) |

Bring the stack up from the repo root:

```bash
docker compose up -d --build
```

Compose wires `MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@platform-store:5432/migraloop` into the app and runs `migraloop run` (continuous Incremental Capture + Delivery for applied Pipelines, plus Prometheus `/metrics` on host port `9090` via `MIGRALOOP_METRICS_ADDR`). Supply Source/Target secret refs into the app environment so continuous Sync can run. Bundled Postgres ships Platform Store Guardrails safe defaults (`shared_buffers=128MB`, `work_mem=8MB`, `maintenance_work_mem=128MB`, `max_connections=100`); the store data volume is also mounted read-only into the app (`MIGRALOOP_PLATFORM_STORE_DATA_DIR`) for the free-disk warn probe. Tune Postgres volumes/resources upward as needed; do not replace the store engine or drop settings below product minimums (see [Operations](operations.md)).

For Operator CLI on the host (against published store port `5432`):

```bash
export MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@127.0.0.1:5432/migraloop
migraloop migrate   # if you are not using `run`
migraloop apply -f deployment.yaml
migraloop status
```

Transform Pipelines in `deployment.yaml` should prefer Aggregation/SQL-like DX (`$project`, `$match`, `$group`, …); classic steps remain Upgrade Compatible—see [Rich Transform](rich-transform.md) and [Pipeline](pipeline.md).

## Local Sync Lab Fixture

For manual end-to-end verification on a disposable real stack (ADR-0025), use the **Local Sync Lab** Fixture. It extends the real install shape with Lab-provisioned Oracle and MongoDB beside Platform Store + app:

```bash
migraloop lab up      # from repo root (or pass --lab-dir)
migraloop lab status  # Fixture readiness + active/leftover Scenario Namespace + connection details; no default Pipeline
migraloop lab scenario list
migraloop lab scenario run direct-pipeline   # needs host Instant Client (LD_LIBRARY_PATH)
migraloop lab scenario run rt-project   # Rich Transform project → Derived → Delivery
migraloop lab scenario run rt-filter   # Rich Transform filter → Derived → Delivery
migraloop lab scenario run rt-field-ops   # Rich Transform addFields/rename/remove → Derived → Delivery
migraloop lab scenario run rt-equilookup   # Rich Transform equiLookup multi-Base → Derived → Delivery
migraloop lab scenario run rt-union        # Rich Transform union multi-Base → Derived → Delivery
migraloop lab scenario run rt-unwind   # Rich Transform unwind → Derived → Delivery
migraloop lab scenario run rt-distinct-addtoset   # Rich Transform distinct/addToSet + Maintenance State → Derived → Delivery
migraloop lab scenario run transform-pipeline   # multi-table groupBy sum/count/min/max/avg → Derived → Delivery
migraloop lab scenario run concurrent-source-workload   # intra-Scenario parallel Source contention
migraloop lab scenario run change-ordering               # Change Ordering / confluence (ADR-0029)
migraloop lab scenario run bulk-load   # ~100k Source inserts + lag/throughput/duration thresholds
migraloop lab scenario run idempotent-redelivery   # duplicate-safe Delivery re-apply
migraloop lab scenario run pause-resume   # pause/resume CLI verbs + catch-up Delivery
migraloop lab scenario run remove-pipeline # remove CLI verb; Shared Base kept for remaining Pipelines
migraloop lab scenario run change-pipeline # Pipeline revision via apply; Derived rebuild / metadata-only skip
migraloop lab scenario run poison-quarantine # quarantine poison identity; Pipeline continues; status unhealthy
migraloop lab scenario run schema-change-pause # blocking DDL warn+pause; status Schema Change (not quarantine)
migraloop lab scenario run source-alignment # Base≠Source detect + Base repair; resource-gated max-rows
migraloop lab scenario run drift-check # Managed Target drift detect + Managed auto-repair; non-Managed preserved
migraloop lab scenario run bounded-backpressure # Downstream slowness → bounded queues + visible lag; not paused
migraloop lab scenario run observability-surface # logs + /metrics lag/failures + status health
migraloop lab scenario run platform-store-guardrails # Guardrails reject lows; free-disk WARN only (no auto-pause)
migraloop lab scenario run initial-load-throttled # Chunked / rate-limited / pausable Initial Load
migraloop lab scenario run backward-compatible-upgrades # Upgrade migrate keeps Deployments; older SemVer config applies without wipe-rebuild
migraloop lab scenario remove direct-pipeline   # clear Namespace without re-running
# or: migraloop lab scenario run direct-pipeline --auto-remove
migraloop lab down    # remove containers and volumes
```

Compose definition: `lab/compose.yaml` (project `migraloop-lab`). The Lab `app` image (`lab/Dockerfile`) copies a host-built `migraloop` binary and installs Oracle Instant Client Basic Light so Fixture `migraloop run` can open LogMiner OCI; `migraloop lab up` builds that binary when missing. Lab Oracle init enables ARCHIVELOG and database supplemental logging for LogMiner; it does **not** pre-apply any Deployment or Pipelines—those come from a Lab Scenario or your own `migraloop apply`. Catalog Scenarios are packaged under `lab/scenarios/<id>/` (`recipe.yaml` + `deployment.yaml`); the recipe-driven runner uses recipe `workload` (including typed `product_path`) / `namespace.lifecycle` / executable `checks.correctness` / `thresholds` as its interface. Poison / delay / fail-after / queue-capacity Lab escapes pass typed SyncOptions CLI flags on real `migraloop sync`, and Initial Load throttle / pause / store-delay knobs pass typed ApplyOptions CLI flags on real `migraloop apply` (not process env as the primary adapter). `migraloop lab scenario list` reflects those selectable recipes. Current catalog entries include `direct-pipeline` (Direct Pipeline insert/update/delete), `rt-project` (Rich Transform `project`), `rt-filter` (Rich Transform `filter`), `rt-field-ops` (Rich Transform `addFields`/`rename`/`remove`), `rt-equilookup` (Rich Transform `equiLookup` multi-Base), `rt-union` (Rich Transform `union` multi-Base), `rt-unwind` (Rich Transform `unwind`), `rt-distinct-addtoset` (Rich Transform `distinct`/`addToSet` + Maintenance State), `transform-pipeline` (multi-table customers + orders with Rich Transform `groupBy` sum/count/min/max/avg → Derived → Delivery), `concurrent-source-workload` (same multi-table shape with recipe-authored parallel Source sessions inside one Scenario run; cross-Scenario concurrency remains forbidden), `change-ordering` (Change Ordering / confluence: same-key capture order, cross-key interleave, min Base recompute; ADR-0029), `bulk-load` (~100k Source inserts via Initial Load with fail-able lag, throughput, and duration thresholds), `idempotent-redelivery` (duplicate-safe / idempotent re-Delivery of Managed Target outcomes on the real apply path), and `pause-resume` (pause/resume CLI verbs: one Pipeline stops Delivery while another continues; resume catch-up from durable Base), and `remove-pipeline` (remove CLI verb: cease Delivery; Shared Base kept for remaining Pipelines), and `change-pipeline` (Pipeline revision via `apply`: pause old Delivery → rebuild that Pipeline's Derived / re-Deliver; Shared Bases not rebuilt; metadata-only `description` skips rebuild), `poison-quarantine` (bounded Delivery retries quarantine one poison Output Identity with ALERT while the Pipeline continues; `status` shows Delivery Health unhealthy / not aligned), `schema-change-pause` (blocking DDL warn+pause; `status` shows Delivery Health paused + Schema Change, distinct from poison quarantine), and `source-alignment` (Source Alignment Check detects Base≠Source, repairs Base from Source reads only, resource-gated `--max-rows`), and `drift-check` (Drift Check detects Managed-field Target drift, default Managed auto-repair, non-Managed preserved, resource-gated `--max-rows`), and `bounded-backpressure` (Downstream Delivery slowness uses bounded Incremental queues with visible Sync/Delivery lag; Pipeline is not paused for mere slowness), and `observability-surface` (structured JSON operator logs, Prometheus `/metrics` lag/failures, Sync/Delivery Health on `status`), and `platform-store-guardrails` (Platform Store Guardrails reject absurd lows; free-disk WARN + `platform_store_disk_warn` only — no auto-pause), and `backward-compatible-upgrades`, `initial-load-throttled` (upgrade migrate preserves Deployments; older SemVer-compatible `apiVersion` applies without wipe-rebuild). `migraloop lab scenario list` reports catalog-complete vs shipped-capability gaps (`lab/scenarios/COVERAGE.md`). Each prepares a Scenario Namespace, applies via the real product path (only against Lab Fixture engines — Scenario `run` refuses customer/production Source/Target bindings), and leaves Namespace state for live `base`/`derived`/`target` inspection by default. Re-running the same Scenario fully removes that Namespace before recreate; `scenario remove` and `--auto-remove` cover manual and opt-in cleanup. Distinct from the default two-container install above (root `Dockerfile`) and from the contract/stub harness / Release Quality Gate used in CI—operators choose Scenarios; the full catalog is not a CI release gate (ADR-0025). Feature-time authoring path: [Developer local setup](developer-local-setup.md).

Resource note: Lab Oracle (Free) typically needs several GB RAM and a few minutes on first image pull/boot. Lab Compose uses `network_mode: host` so the Fixture stays usable in nested Docker environments where bridge networking is blocked. Nested Docker environments that fail image extract on overlay whiteouts need a non-overlay dockerd storage driver—`fuse-overlayfs` or `vfs`—with the containerd snapshotter disabled. **Cursor Cloud** agents get that recipe from `.cursor/environment.json` (`fuse-overlayfs` via `.cursor/cloud-dind-*.sh`); other nested hosts can apply the same `daemon.json` shape manually.

### DB-level restore / load escape hatch

When you need to load or restore data **outside** a Lab Scenario recipe—SQL dumps, ad-hoc inserts, or Mongo seed/restore—use the disposable Lab engines and the connection details from `migraloop lab status` (or `lab up`). This is an escape hatch onto the real stack, **not** a second Scenario authoring model and **not** the Release Quality Gate / CI.

Samples live under `lab/escape-hatch/` (`oracle-load.sql`, `mongo-load.js`, plus a Lab-only `deployment.yaml` so you can continue on the ordinary product path). There is no `recipe.yaml` and you do **not** run `migraloop lab scenario …` for this flow.

```bash
migraloop lab up
migraloop lab status   # copy Lab Oracle / Mongo / Platform Store details

# Load into Lab Oracle (compose exec — no BYO production Source required)
docker compose -f lab/compose.yaml -p migraloop-lab exec -T oracle \
  sqlplus -s SYNC_USER/lab_oracle@FREEPDB1 < lab/escape-hatch/oracle-load.sql

# Load into Lab Mongo (Target-side seed / restore-style inspect)
docker compose -f lab/compose.yaml -p migraloop-lab exec -T mongo \
  mongosh --quiet --host 127.0.0.1 -u migraloop -p lab_mongo \
  --authenticationDatabase admin lab < lab/escape-hatch/mongo-load.js

# Continue on the real product path (apply / status / base / target / sync)
export MIGRALOOP_PLATFORM_STORE_URL=postgres://migraloop:migraloop@127.0.0.1:5432/migraloop
export ORACLE_PASSWORD=lab_oracle
export MONGO_PASSWORD=lab_mongo
# Host Instant Client required for apply/sync against Lab Oracle:
# export LD_LIBRARY_PATH=/path/to/instantclient
migraloop apply -f lab/escape-hatch/deployment.yaml
migraloop status
migraloop base --table LAB_ESCAPE_CUSTOMERS
migraloop target --collection lab_escape_customers   # after Delivery has run
# Steady-state Sync is continuous inside Lab `migraloop run`; optional one-shot:
migraloop sync                                       # Lab / operator Incremental Capture catch-up
```

**Optional dump-tool restore** (same Lab connection details; still not a Scenario / not CI). Host tools talk to Lab ports on `127.0.0.1` because Lab Compose uses `network_mode: host`:

```bash
# MongoDB archive → Lab Mongo (example; adjust dump path / ns)
mongorestore \
  --uri 'mongodb://migraloop:lab_mongo@127.0.0.1:27017/lab?authSource=admin' \
  --archive=your-lab-seed.archive

# Oracle Data Pump → Lab Oracle (run from a client with network access to Lab;
# SYS password for the disposable image is lab_oracle_sys — Lab-only)
impdp SYNC_USER/lab_oracle@//127.0.0.1:1521/FREEPDB1 \
  DUMPFILE=your_lab_seed.dmp DIRECTORY=DATA_PUMP_DIR TABLE_EXISTS_ACTION=REPLACE
```

After a dump restore into Lab Oracle tables you intend to sync, apply table-level supplemental logging (as `oracle-load.sql` does), then use ordinary `migraloop apply` / `status` / `base` / `target` / `sync` with a Lab-bound Deployment. Target-side loads/restores that are not Delivery-managed are inspected with mongosh against the Lab Mongo URI; `migraloop target` inspects collections after product Delivery.

Keep loads on the disposable Fixture only—never point this escape hatch at customer/production engines. Prefer Lab Scenarios when you want a packaged correctness + metrics recipe; use this path when you already have SQL/JS/dumps to place into Lab databases yourself.

## Runtime model

- v1 runs **one active app instance** (internally concurrent) plus the Platform Store.
- All durable Deployment state (Pipelines, Base/Derived Datasets, checkpoints) lives in the Platform Store so a replacement instance can resume.
- Automatic multi-instance failover is later; active processing remains single-leader (not multi-writer).
- Deployment **runtime** owns apply / Sync / Delivery / lifecycle / checks; the Operator CLI is a thin adapter. New Source or Target engines plug in at `SourceEngine` / `TargetEngine` without reshaping those concepts—see [Developer local setup](developer-local-setup.md#adding-a-source-or-target-engine-developer-checklist).

## Declaring a Deployment

Config is YAML or JSON. Essential top-level fields:

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
    timezone: Asia/Taipei          # optional IANA or ±HH:MM; naive DATE/TIMESTAMP fallback
    # tls:                         # optional; omit for cleartext Lab/dev
    #   enabled: true
    #   walletLocation: /etc/oracle/wallet
  target:
    kind: mongodb
    host: mongo.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
    # tls:
    #   enabled: true
    #   caFile: /etc/migraloop/certs/mongo-ca.pem
  pipelines: []                    # see Pipeline chapter
```

v1 requires `source.kind: oracle` and `target.kind: mongodb`. Passwords must be secret references—never plaintext. Optional `tls` blocks (and Platform Store `sslmode` on `MIGRALOOP_PLATFORM_STORE_URL`) are documented in [Security](security.md) and [CLI & Config](cli-and-config.md).

Apply with `migraloop apply -f <file>`. Empty `pipelines` applies Deployment metadata only (no capture yet).

## Related chapters

- Source connection and prerequisites: [Source System](source-system.md)
- Target Binding and Delivery: [Target System](target-system.md)
- Pipelines inside the Deployment: [Pipeline](pipeline.md)
- Secrets and TLS: [Security](security.md)
- Full field/flag list: [CLI & Config reference](cli-and-config.md)
