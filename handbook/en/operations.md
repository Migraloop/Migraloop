# Operations

Day-2 behaviors Operators should expect when running Deployments in production. Several controls below are **product contracts** recorded in ADRs and the domain glossary; confirm what your build exposes via `migraloop status` / logs as each control lands.

## Schema Change Handling

Source DDL is classified against each Pipeline’s dependencies (ADR-0009):

| Impact | Intended platform behavior |
| --- | --- |
| Does not affect the Pipeline | Processing continues; schema can catch up |
| Affects the Pipeline but apply stays safe | Processing continues |
| Blocks safe apply (retries cannot progress) | **Warn and pause** the affected Pipeline(s) |

This pause rule is for **stream-wide blockers**, not single-row poison data. When Incremental Capture sees blocking Source DDL, `migraloop sync` emits an Operator-visible **WARN**, persists a Schema Change impact, and pauses the affected Pipeline(s) via the same durable pause flag as `migraloop pause`—without quarantine. Unaffecting or non-blocking schema changes continue; `status` shows `Delivery Health: paused` plus Schema Change rows for active blocking impacts (distinct from Poison Change quarantine). Operators can also intentionally pause/resume a Pipeline with `migraloop pause --pipeline <name>` / `migraloop resume --pipeline <name>`, or remove one with `migraloop remove --pipeline <name>` (see [Pipeline](pipeline.md) and [CLI & Config](cli-and-config.md)) without restarting the Deployment. Resume clears active Schema Change impacts for that Pipeline and catch-up Delivers from durable Base/Derived state.

## Poison Change Handling

When a single change or Output Identity repeatedly fails but the rest of the stream can continue (ADR-0015), the intended path is:

1. Bounded retries
2. **Quarantine** that change/identity
3. **Alert** Operators
4. **Keep the Pipeline running**

Quarantined keys stay unhealthy / not aligned until repaired or retried—never a silent skip. Do not expect a whole-Pipeline pause for one bad row. After bounded Delivery retries, `migraloop sync` persists the quarantine, emits an Operator-visible **ALERT**, and continues other changes; `migraloop status` shows `Delivery Health: unhealthy` with each quarantined Output Identity marked unhealthy / not aligned.

## Source Alignment Check

Sync Health alone does not prove Base matches Source. Operators run a schedulable, resource-gated **Source Alignment Check** before treating a Base Dataset as a Drift baseline:

```bash
migraloop align [--table CUSTOMERS] [--max-rows 1000]
```

The check reads at most `--max-rows` Source rows (default `1000` — not a full slam), compares them to Base by primary key, and repairs Base from those Source reads when misaligned. It **never writes Source**. `status` shows `Source Alignment: aligned|partial|unknown` with checked/mismatched counts from the last run (`partial` = budget truncated). See [CLI & Config](cli-and-config.md) and [Observability](observability.md).

## Drift Check

Delivery Health alone does not prove Managed fields on the Target match the platform expected dataset. Operators run a schedulable, resource-gated **Drift Check** after Source Alignment (for Direct Pipelines) so Base/Derived is a trusted baseline:

```bash
migraloop drift [--pipeline customers] [--max-rows 1000]
```

The check reads at most `--max-rows` expected Output Identities (default `1000` — not a full slam), compares Managed fields to the Target, and by default **auto-repairs** Managed drift via Managed-only upsert. **Non-Managed Target fields are ignored** and left untouched. It does not add Source load beyond the Alignment baseline. `status` shows `Drift: ok|partial|unknown` with checked/mismatched counts (`partial` = budget truncated). See [CLI & Config](cli-and-config.md) and [Observability](observability.md).

## Backpressure

When Platform Store apply, Derived maintenance, or Target Delivery cannot keep up (ADR-0020):

- Stages use **bounded queues** (default Incremental window `MIGRALOOP_SYNC_QUEUE_CAPACITY`, 256) and slow capture/apply
- Sync Health and Delivery Health both expose `lag=` for remaining work in the current window; `sync` prints `Backpressure: queue_depth=… capacity=…` when the window is full or Downstream is delayed
- Unbounded in-memory buffering / OOM-as-backpressure is rejected
- Pausing an entire Pipeline solely because the Target is slow is **not** the default

Operators act on visible lag (scale Target, reduce load, inspect Delivery errors)—pause remains for true blockers. Lab Scenario `bounded-backpressure` exercises this path on the disposable Fixture.

## Platform Store Guardrails

The bundled PostgreSQL Platform Store ships with safe defaults and product-enforced minimums (ADR-0010 / issue #28). Operators may raise settings; the app **rejects** absurd lows so migrate / status / sync / apply / run fail with a Guardrails message rather than running under-provisioned.

| Setting | Compose default | Product minimum (floor) |
| --- | --- | --- |
| `shared_buffers` | `128MB` | ≥ `64MB` |
| `work_mem` | `8MB` | ≥ `4MB` |
| `maintenance_work_mem` | `128MB` | ≥ `64MB` |
| `max_connections` | `100` | ≥ `20` |

Root `compose.yaml` and `lab/compose.yaml` ship those defaults on the `platform-store` service.

**Free-disk warn (warn-only):** when free space on the Platform Store data volume drops below **1 GiB**, the product prints `WARN: …` on `migraloop status` (and on sync / apply / run paths), emits structured event `platform_store_disk_warn`, and exposes Prometheus gauges `migraloop_platform_store_disk_free_bytes` and `migraloop_platform_store_disk_warn`. Crossing the threshold does **not** auto-pause Pipelines—Platform Store stays healthy unless something else is wrong; acting on the warning is an Operator responsibility.

**How free disk is observed:** compose mounts the store data volume read-only into the app at `/var/lib/migraloop/platform-store-data` and sets `MIGRALOOP_PLATFORM_STORE_DATA_DIR` to that path. Operators/orchestrators may instead supply `MIGRALOOP_PLATFORM_STORE_FREE_DISK_BYTES` when a filesystem probe is unavailable.

Lab Scenario `platform-store-guardrails` exercises bundled Guardrail minimums, absurd-low rejection, and warn-only free-disk behavior on the disposable Fixture. Postgres backup remains an Operator responsibility.

## Upgrades

Upgrades are **backward compatible** (ADR-0014):

- Platform Store schema changes ship as versioned migrations applied on startup (`migraloop run` / `migraloop migrate`)
- A newer app must continue existing Deployments and accepted older config without wipe-and-rebuild
- Deployment config `apiVersion` is SemVer-compatible on major `1`: canonical `migraloop.dev/v1`, and older accepted forms such as `migraloop.dev/v1.0` / `migraloop.dev/v1.0.0` still apply; incompatible majors are rejected with a clear error
- Short sync pause during single-instance upgrade is allowed; checkpoint/data loss is not
- Downgrade support is not required in v1

Recommended upgrade loop:

1. `migraloop status` — note checkpoints and health
2. Roll the new app image / binary
3. Confirm migrations (`Schema version` in `status`)
4. Re-apply accepted older config if needed (must not Initial Load / rebuild Base from scratch)
5. `migraloop sync` / watch Sync Health and Delivery Health

Lab Scenario `backward-compatible-upgrades` exercises migrate-on-upgrade, older SemVer-compatible config apply, and no wipe-rebuild on the disposable Fixture.

## Restart resume

Durable capture and Delivery progress live in the Platform Store. After process restart, `migraloop sync` resumes Incremental Capture from the stored checkpoint (exclusive) and continues Delivery—Operators should not need local-only recovery files.

## Related chapters

- Health interpretation: [Observability](observability.md)
- Install / single-instance model: [Deployment](deployment.md)
- CLI verbs: [CLI & Config reference](cli-and-config.md)
