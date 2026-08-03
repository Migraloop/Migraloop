# Observability

Operators need Sync / Delivery signals, structured logs, and (as the Observability Surface lands) Prometheus metrics with alertable failure counters (ADR-0008). Distributed tracing / vendor APM is optional later.

## What to run first

```bash
migraloop status
```

`status` is the primary Operator loop today. It reports:

- **Platform Store** reachability / health and schema version
- Each **Deployment** (Source/Target identity, LogMiner mechanism: contract vs OCI)
- Each **Pipeline** (mode, source table, target collection, Delivery status)
- Each **Base Dataset** (status, row count, columns, omitted unsupported types, Initial Load / cutover watermarks, **Sync Health** with appliedChanges / lag / checkpoint, **Source Alignment** with checked/mismatched counts)
- **Delivery Health** per Pipeline (applied changes / lag / status; `unhealthy` when Poison Change quarantine is active; `paused` when blocking Schema Change pause is active; lag rises under Downstream backpressure without pausing the Pipeline)
- Active **Quarantine** rows (Output Identity, change id, attempts, last error — unhealthy / not aligned)
- **Derived Datasets** for Transform Pipelines when present

Healthy examples Operators look for after a first sync:

- Platform Store: `healthy`
- Base Dataset status progresses through Initial Load then incremental apply
- Sync Health lag trends toward caught-up (not permanently growing)
- Delivery Health shows successful apply for configured Target Bindings (`ok`, not `unhealthy` quarantine)
- Quarantine: `(none)` unless a poison identity was intentionally left quarantined
- Schema Change: `(none)` unless a blocking DDL pause was intentionally left active

## Deeper inspection commands

| Command | Use |
| --- | --- |
| `migraloop base --table <TABLE>` | Sample Base Dataset rows in the Platform Store |
| `migraloop target --collection <name>` | Sample Delivered MongoDB documents |
| `migraloop derived --pipeline <name>` | Sample Derived Dataset rows |

Add `--deployment <name>` when multiple Deployments share table/collection/pipeline names.

## Sync Health vs Delivery Health

- **Sync Health** — capture from Source into a Base Dataset is caught up and applying successfully. Necessary but not sufficient to claim Base matches Source.
- **Source Alignment** — last Source Alignment Check result for that Base (`unknown` / `aligned` / `partial`). Run `migraloop align` (resource-gated; repairs Base from Source reads; never writes Source) before treating Base as a Drift baseline. `partial` means the last check hit its `--max-rows` budget.
- **Delivery Health** — the change stream for a Pipeline’s Target Binding is caught up and applying successfully. Edits to non-Managed fields are irrelevant. Under Downstream backpressure, `lag=` reflects remaining pending Delivery work from the capture resume position (ADR-0020)—not a whole-Pipeline pause. Capture still materializes at most one bounded queue window at a time.
- **Drift** — last Drift Check result for that Pipeline (`unknown` / `ok` / `partial`). Run `migraloop drift` (resource-gated; default Managed-field auto-repair; non-Managed fields ignored) after Alignment. `partial` means the last check hit its `--max-rows` budget.

## Logs and metrics

- App/CLI emit **structured JSON** operator event lines (plus human-readable companions) on Initial Load, Incremental Capture, Delivery, Backpressure, Poison Change quarantine, and blocking Schema Change (stdout/stderr of the `migraloop` process / container logs). Look for `"event":"…"` fields such as `initial_load_complete`, `incremental_capture`, `delivery_complete`, `backpressure`, `poison_quarantine`, `schema_change_blocked`.
- `migraloop run` serves a Prometheus scrape endpoint at `http://<metrics-addr>/metrics` (default `0.0.0.0:9090`, override with `--metrics-addr` / `MIGRALOOP_METRICS_ADDR`). Compose publishes host port `9090`. Metrics include Sync/Delivery lag (`migraloop_sync_lag`, `migraloop_delivery_lag`), Pipeline pause, and alertable failure gauges (`migraloop_quarantined_changes`, `migraloop_failures_total`) read from durable Platform Store state.
- `status` remains the primary Operator loop for lag/checkpoint/error lines; scrape `/metrics` for alerting and dashboards.

## Related chapters

- Progressive path: [Start here](start-here.md)
- Day-2 failure modes: [Operations](operations.md)
- Command flags: [CLI & Config reference](cli-and-config.md)
