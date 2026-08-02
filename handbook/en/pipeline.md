# Pipeline

A **Pipeline** is a user-defined flow inside a **Deployment** that produces one target collection. The Deployment owns the Source/Target pair; the Pipeline owns mode, source table reference, optional Rich Transform, Output Identity, Target Binding, and field mapping overrides.

## Modes

| Mode | Behavior |
| --- | --- |
| `direct` | No Rich Transform. One Base Dataset is Delivered to the Target Binding. Output Identity defaults from the source primary key. |
| `transform` | Declares a declarative **Rich Transform**, materializes a **Derived Dataset**, and Delivers that. Requires non-empty `outputIdentity` and at least one transform operator. |

## Declaring Pipelines

Pipelines live under `spec.pipelines` in the Deployment document:

```yaml
pipelines:
  - name: orders_direct
    mode: direct
    source:
      table: ORDERS
      schema: APP                 # optional
    target:
      collection: orders
    # optional Managed-field overrides (unsafe NUMBER, etc.)
    fields:
      HUGE_AMOUNT:
        as: string               # or omit

  - name: orders_by_customer
    mode: transform
    source:
      table: ORDERS
    target:
      collection: orders_by_customer
    outputIdentity: [CUSTOMER_ID]
    transform:
      - groupBy:
          keys: [CUSTOMER_ID]
          aggregates:
            - op: sum
              field: AMOUNT
              as: TOTAL_AMOUNT
```

Validation rules enforced on `apply`:

- `mode` is `direct` or `transform`
- Direct Pipelines must not declare `transform`
- Transform Pipelines require `outputIdentity` and a non-empty declarative `transform`
- `fields` keys map source/Managed field names to `{ as: string }` or `{ as: omit }` (ADR-0023)

See [Rich Transform](rich-transform.md) for operator shapes.

## Lifecycle (control plane)

Product model: add, pause, resume, remove, and change Pipelines without restarting the whole Deployment (ADR-0007).

**What Operators do today with the shipped CLI:**

1. Edit the declarative Deployment document (add/change/remove Pipeline entries).
2. `migraloop apply -f deployment.yaml` — upserts Deployment + Pipeline set; runs table-level **Initial Load** for newly referenced tables; rebuilds Derived output when a Transform revision requires it; shared Base Datasets are not rebuilt for an unrelated Pipeline change.
3. `migraloop sync` — Incremental Capture + Delivery for active (non-paused) Pipelines.
4. `migraloop pause --pipeline <name>` / `migraloop resume --pipeline <name>` — stop or continue Delivery/processing for one Pipeline without restarting the Deployment. Pause is durable in the Platform Store; resume catch-up Delivers from current Base/Derived state. Other Pipelines are unaffected. `status` shows `paused` on the Pipeline and its Delivery Health.
5. `migraloop status` / `base` / `target` / `derived` — inspect progress and health.

Stream-wide blockers (for example unblockable DDL) still follow [Operations](operations.md) pause guidance; Operator-driven pause/resume is the first-class control-plane path for intentional stops.

## Capture scope

Which Source tables enter Sync is determined by Pipeline `source.table` references. Each table has at most one Base Dataset per Deployment, shared across Pipelines. New tables get table-level Initial Load only.

## Related chapters

- Source prerequisites and types: [Source System](source-system.md)
- Target Binding / Managed fields: [Target System](target-system.md)
- Transform operators: [Rich Transform](rich-transform.md)
- Config field reference: [CLI & Config reference](cli-and-config.md)
