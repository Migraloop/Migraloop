# v1 Observability Surface is health, logs, and Prometheus metrics

Production v1 must expose structured logs, Sync/Delivery Health (lag, checkpoints, errors), per-Pipeline status, Prometheus metrics, and alertable failure counters. This is enough to operate a single-instance Deployment. Full distributed tracing and vendor APM bindings are deferred.

## Narrative ownership

- **Status and Prometheus** format Operator-visible health from one Deployment runtime Observability assembly (`assemble_observability_surface`). The CLI adapter owns status narrative; metrics derive the same facts — no forked Sync/Delivery Health math (issue #174).
- **Apply / Incremental Capture progress, ALERT, quarantine, and backpressure** dual-emit from the runtime: a human-readable companion line plus a structured JSON operator event (`emit_event`, fields such as `initial_load_*`, `incremental_capture`, `backpressure`, `poison_quarantine`, `schema_change_blocked`, `platform_store_disk_warn`). That dual emission is intentional product behavior (see handbook Observability), not leftover debt.
- **Optional extraction** of the remaining human companions into the CLI adapter (issue #209 / parent #199) is **declined**. Stronger #199 slices already removed the real pain (typed health assembly + structured events). Moving println ownership would require an invasive event sink across continuous `run`/`sync`, churn RQG / Lab scrapers that still assert Operator wording, and three-locale handbook edits for no Operator-visible outcome change. Revisit only if dual-format drift or println coupling becomes an active maintenance blocker.
