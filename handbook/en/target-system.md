# Target System

A **Target System** is the user database the platform **Delivers** into. v1 ships **MongoDB** document Delivery.

## Connection shape

Under `spec.target` in the Deployment config:

| Field | Meaning |
| --- | --- |
| `kind` | Must be `mongodb` in v1 |
| `host` / `port` / `database` | MongoDB connection identity |
| `username` | Delivery account with rights to upsert/delete in bound collections |
| `password` | Secret reference (`fromEnv`, `fromFile`, or `fromDockerSecret`) |

Target timezone is not configured in v1—Delivery writes UTC datetime for temporal Managed fields.

## Target Binding

Each Pipeline that Delivers declares a **Target Binding**: which collection receives the Pipeline’s output.

```yaml
target:
  collection: orders
```

The binding (with the Pipeline) also implies **Output Identity** and **Managed Columns**. The Target collection may hold additional fields outside the binding.

## Managed Columns / fields

**Managed Columns** (document fields in v1) are the output shape Delivery will write.

- On MongoDB, the platform does **not** inventory non-managed fields—it simply never writes keys outside the Managed set, so other fields stay untouched.
- When an **Output Identity** no longer exists in the platform dataset, Delivery may **delete the entire target document**.
- Reliability is **at-least-once with idempotent apply**: retries may rewrite the same identity; Managed results are upserted/deleted by identity.

## Direct vs Transform Delivery

| Pipeline mode | What is Delivered |
| --- | --- |
| `direct` | The Base Dataset row shape (flattened fields). Source primary key maps to document identity (`_id` or configured id). |
| `transform` | The **Derived Dataset** produced by the Rich Transform. Operator must declare `outputIdentity`. |

Inspect delivered documents:

```bash
migraloop target --collection orders
# optional: --deployment <name> when names collide
```

## Required Privileges (Target)

The Delivery account needs rights to insert/update/delete documents in the bound collections (and to create the collection if your ops model allows). Prefer minimum grants sufficient to Deliver—not cluster-admin by default (ADR-0016).

## Supported mapping notes

Source allow-list and NUMBER/temporal rules affect what can appear in Managed output—see [Source System](source-system.md). Unsafe NUMBER columns must be mapped with Pipeline `fields` before apply succeeds.

## Related chapters

- Deployment pairing: [Deployment](deployment.md)
- Pipeline modes and `fields`: [Pipeline](pipeline.md)
- Delivery Health: [Observability](observability.md)
- Secrets / TLS: [Security](security.md)
