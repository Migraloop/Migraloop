# Operations

Day-2 behaviors Operators should expect when running Deployments in production. Several controls below are **product contracts** recorded in ADRs and the domain glossary; confirm what your build exposes via `migraloop status` / logs as each control lands.

## Schema Change Handling

Source DDL is classified against each Pipeline’s dependencies (ADR-0009):

| Impact | Intended platform behavior |
| --- | --- |
| Does not affect the Pipeline | Processing continues; schema can catch up |
| Affects the Pipeline but apply stays safe | Processing continues |
| Blocks safe apply (retries cannot progress) | **Warn and pause** the affected Pipeline(s) |

This pause rule is for **stream-wide blockers**, not single-row poison data. Dedicated pause/resume CLI verbs are part of the control-plane contract (see [Pipeline](pipeline.md)); until they ship, treat unblockable apply failures as Operator-visible errors in `status` / logs and keep only runnable Pipelines declared in config.

## Poison Change Handling

When a single change or Output Identity repeatedly fails but the rest of the stream can continue (ADR-0015), the intended path is:

1. Bounded retries
2. **Quarantine** that change/identity
3. **Alert** Operators
4. **Keep the Pipeline running**

Quarantined keys stay unhealthy / not aligned until repaired or retried—never a silent skip. Do not expect a whole-Pipeline pause for one bad row. Until quarantine surfaces in `status`, watch apply errors and Delivery Health for stuck identities.

## Backpressure

When Platform Store apply, Derived maintenance, or Target Delivery cannot keep up (ADR-0020):

- Stages use **bounded queues** and slow capture/apply
- Lag remains visible on Sync Health / Delivery Health (and metrics when exposed)
- Unbounded in-memory buffering / OOM-as-backpressure is rejected
- Pausing an entire Pipeline solely because the Target is slow is **not** the default

Operators act on visible lag (scale Target, reduce load, inspect Delivery errors)—pause remains for true blockers.

## Platform Store Guardrails

The bundled PostgreSQL Platform Store ships with safe defaults and product-enforced minimums (ADR-0010). Crossing a safe threshold (for example free disk) must **warn only**—the platform does not auto-pause solely for resource pressure. Postgres backup remains an Operator responsibility.

## Upgrades

Upgrades are **backward compatible** (ADR-0014):

- Platform Store schema changes ship as versioned migrations applied on startup (`migraloop run` / `migraloop migrate`)
- A newer app must continue existing Deployments and accepted older config without wipe-and-rebuild
- Short sync pause during single-instance upgrade is allowed; checkpoint/data loss is not
- Downgrade support is not required in v1

Recommended upgrade loop:

1. `migraloop status` — note checkpoints and health
2. Roll the new app image / binary
3. Confirm migrations (`Schema version` in `status`)
4. `migraloop sync` / watch Sync Health and Delivery Health

## Restart resume

Durable capture and Delivery progress live in the Platform Store. After process restart, `migraloop sync` resumes Incremental Capture from the stored checkpoint (exclusive) and continues Delivery—Operators should not need local-only recovery files.

## Related chapters

- Health interpretation: [Observability](observability.md)
- Install / single-instance model: [Deployment](deployment.md)
- CLI verbs: [CLI & Config reference](cli-and-config.md)
