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
- Each **Base Dataset** (status, row count, columns, omitted unsupported types, Initial Load / cutover watermarks, **Sync Health** with appliedChanges / lag / checkpoint)
- **Delivery Health** per Pipeline (applied changes / status)
- **Derived Datasets** for Transform Pipelines when present

Healthy examples Operators look for after a first sync:

- Platform Store: `healthy`
- Base Dataset status progresses through Initial Load then incremental apply
- Sync Health lag trends toward caught-up (not permanently growing)
- Delivery Health shows successful apply for configured Target Bindings

## Deeper inspection commands

| Command | Use |
| --- | --- |
| `migraloop base --table <TABLE>` | Sample Base Dataset rows in the Platform Store |
| `migraloop target --collection <name>` | Sample Delivered MongoDB documents |
| `migraloop derived --pipeline <name>` | Sample Derived Dataset rows |

Add `--deployment <name>` when multiple Deployments share table/collection/pipeline names.

## Sync Health vs Delivery Health

- **Sync Health** — capture from Source into a Base Dataset is caught up and applying successfully. Necessary but not sufficient to claim byte-identical source alignment (see Source Alignment Check in domain docs / future Operations depth).
- **Delivery Health** — the change stream for a Pipeline’s Target Binding is caught up and applying successfully. Edits to non-Managed fields are irrelevant.

## Logs and metrics

- App/CLI emit structured operational lines on Initial Load, Incremental Capture, Delivery, and failures (stdout/stderr of the `migraloop` process / container logs).
- A Prometheus scrape endpoint and alert counters are part of the Observability Surface **contract** (ADR-0008) and are not yet the primary Operator interface—use `status` lag/checkpoint/error lines until metrics land in your build.

## Related chapters

- Progressive path: [Start here](start-here.md)
- Day-2 failure modes: [Operations](operations.md)
- Command flags: [CLI & Config reference](cli-and-config.md)
