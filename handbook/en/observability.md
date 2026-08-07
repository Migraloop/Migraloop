# Observability

Operators need Sync / Delivery signals, structured logs, and (as the Observability Surface lands) Prometheus metrics with alertable failure counters (ADR-0008). Distributed tracing / vendor APM is optional later.

## What to run first

```bash
migraloop status
```

`status` is the primary Operator loop today. It reports:

- **Platform Store** reachability / health and schema version (Platform Store Guardrails: absurdly low Postgres settings are rejected; free disk below 1 GiB prints a warn-only `WARN` — never auto-pauses Pipelines)
- Each **Deployment** (Source/Target identity, LogMiner mechanism: contract vs OCI)
- Each **Pipeline** (mode, source table, target collection, Delivery status)
- Each **Base Dataset** (status, row count, columns, omitted unsupported types, Initial Load / cutover watermarks, **Sync Health** — `unknown` / `ok` / `lagging` / `failed` — with appliedChanges / lag / checkpoint, **Source Alignment** with checked/mismatched counts)
- **Delivery Health** per Pipeline (applied changes / lag / status; `unhealthy` when Poison Change quarantine is active; `paused` when blocking Schema Change pause is active; lag rises under Downstream backpressure without pausing the Pipeline)
- Active **Quarantine** rows (Output Identity, change id, attempts, last error — unhealthy / not aligned)
- **Derived Datasets** for Transform Pipelines when present
- **Component pressure** summaries with stable names `app`, `source`, `platform_store`, and `target` (same names in Lab Scenario reports and Capacity Estimate — ADR-0031)

Healthy examples Operators look for after a first sync:

- Platform Store: `healthy`
- Base Dataset status progresses through Initial Load then incremental apply
- Sync Health is `ok` (or trends from `lagging` toward `ok`) — lag not permanently growing
- Delivery Health shows successful apply for configured Target Bindings (`ok`, not `unhealthy` quarantine)
- Quarantine: `(none)` unless a poison identity was intentionally left quarantined
- Schema Change: `(none)` unless a blocking DDL pause was intentionally left active

## Component pressure and Capacity Estimate

Per-component pressure attributes throughput limits to the app path, Source System, Platform Store, or Target System. Live `status` / Prometheus and Lab Scenario reports use the **same stable names**: `app`, `source`, `platform_store`, `target`.

- When Source, Platform Store, or Target is saturated, Lab evidence is labeled `INFRA-SATURATED` with resize guidance — **not** a product FAIL. Resize the Lab Fixture (or live infra) and re-run.
- `migraloop capacity-estimate` is the live Operator command: it reports `limiting_component` and a coarse `max_e2e_qps`. It is **advisory only** and never mutates Source System or Target System database configuration.

## Deeper inspection commands

| Command | Use |
| --- | --- |
| `migraloop capacity-estimate` | Limiting component + coarse max end-to-end Managed Delivery QPS (read-only) |
| `migraloop base --table <TABLE>` | Sample Base Dataset rows in the Platform Store |
| `migraloop target --collection <name>` | Sample Delivered MongoDB documents |
| `migraloop derived --pipeline <name>` | Sample Derived Dataset rows |

Add `--deployment <name>` when multiple Deployments share table/collection/pipeline names.

## Sync Health vs Delivery Health

- **Sync Health** — capture from Source into a Base Dataset is caught up and applying successfully (`ok` when lag is 0 after Incremental progress; `lagging` while Source backlog remains; `failed` on durable capture/apply failure; `unknown` before Incremental progress). Necessary but not sufficient to claim Base matches Source. `status` and Prometheus derive these labels from the same Observability assembly.
- **Source Alignment** — last Source Alignment Check result for that Base (`unknown` / `aligned` / `partial`). Run `migraloop align` (resource-gated; repairs Base from Source reads; never writes Source) before treating Base as a Drift baseline. `partial` means the last check hit its `--max-rows` budget.
- **Delivery Health** — the change stream for a Pipeline’s Target Binding is caught up and applying successfully. Edits to non-Managed fields are irrelevant. Under Downstream backpressure, `lag=` reflects remaining pending Delivery work from the capture resume position (ADR-0020)—not a whole-Pipeline pause. Capture still materializes at most one bounded queue window at a time.
- **Drift** — last Drift Check result for that Pipeline (`unknown` / `ok` / `partial`). Run `migraloop drift` (resource-gated; default Managed-field auto-repair; non-Managed fields ignored) after Alignment. `partial` means the last check hit its `--max-rows` budget.

## Logs and metrics

- App/CLI emit **structured JSON** operator event lines (plus human-readable companions) on Initial Load, Incremental Capture, Delivery, Backpressure, Poison Change quarantine, blocking Schema Change, and Platform Store free-disk warn (stdout/stderr of the `migraloop` process / container logs). Look for `"event":"…"` fields such as `initial_load_progress`, `initial_load_paused`, `initial_load_backoff`, `initial_load_complete`, `incremental_capture`, `delivery_complete`, `backpressure`, `poison_quarantine`, `schema_change_blocked`, `platform_store_disk_warn`.
- `migraloop run` continuously performs Incremental Capture → Delivery for applied Pipelines and serves a Prometheus scrape endpoint at `http://<metrics-addr>/metrics` on that same single active instance (default `0.0.0.0:9090`, override with `--metrics-addr` / `MIGRALOOP_METRICS_ADDR`). Compose publishes host port `9090`. Metrics include Sync/Delivery lag (`migraloop_sync_lag`, `migraloop_delivery_lag`), Pipeline pause, alertable failure gauges (`migraloop_quarantined_changes`, `migraloop_failures`) read from durable Platform Store state, Platform Store disk gauges (`migraloop_platform_store_disk_free_bytes`, `migraloop_platform_store_disk_warn` — warn-only; never auto-pause), and component pressure gauges (`migraloop_component_pressure{component=…}`, `migraloop_component_saturated{component=…}` for `app` / `source` / `platform_store` / `target`).
- `status` remains the primary Operator loop for lag/checkpoint/error lines and component pressure; scrape `/metrics` for alerting and dashboards; use `capacity-estimate` when you need the limiting component and coarse max e2e QPS.

## Related chapters

- Progressive path: [Start here](start-here.md)
- Day-2 failure modes: [Operations](operations.md)
- Command flags: [CLI & Config reference](cli-and-config.md)
