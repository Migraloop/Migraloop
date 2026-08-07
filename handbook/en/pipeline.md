# Pipeline

A **Pipeline** is a user-defined flow inside a **Deployment** that produces one target collection. The Deployment owns the Source/Target pair; the Pipeline owns mode, source table reference, optional Rich Transform, Output Identity, Target Binding, and field mapping overrides.

## Modes

| Mode | Behavior |
| --- | --- |
| `direct` | No Rich Transform. One Base Dataset is Delivered to the Target Binding. Output Identity defaults from the source primary key. |
| `transform` | Declares a declarative **Rich Transform**, materializes a **Derived Dataset**, and Delivers that. Requires non-empty `outputIdentity` and at least one transform operator. |

## Declaring Pipelines

Pipelines live under `spec.pipelines` in the Deployment document. The enclosing Deployment `apiVersion` must be SemVer older-or-equal within major `1` (canonical `migraloop.dev/v1`; older accepted forms such as `migraloop.dev/v1.0.0` still apply Pipeline declarations without wipe-rebuild — see [Operations](operations.md) Upgrades and [CLI & Config](cli-and-config.md)):

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
      - $group:
          _id: "$CUSTOMER_ID"
          TOTAL_AMOUNT:
            $sum: "$AMOUNT"
```

Validation rules enforced on `apply`:

- `mode` is `direct` or `transform`
- Direct Pipelines must not declare `transform`
- Transform Pipelines require `outputIdentity` and a non-empty declarative `transform`
- `fields` keys map source/Managed field names to `{ as: string }` or `{ as: omit }` (ADR-0023; NUMBER classification lives next to shared `ColumnShape`)

See [Rich Transform](rich-transform.md) for operator shapes (Aggregation-only `$project` / `$match` / `$lookup` / `$group`; classic and SQL-ish aliases are rejected — ADR-0030).

## Lifecycle (control plane)

Product model: add, pause, resume, remove, and change Pipelines without restarting the whole Deployment (ADR-0007).

**What Operators do today with the shipped CLI:**

1. Edit the declarative Deployment document (add/change/remove Pipeline entries).
2. `migraloop apply -f deployment.yaml` — upserts Deployment + Pipeline set; runs table-level **Initial Load** for newly referenced tables; applies Pipeline **revisions** when a Pipeline's semantic declaration changes (mode, Source table, Target Binding, fields, Output Identity, or transform): pause that Pipeline's old Delivery, rebuild its Derived Dataset and re-Deliver as required (including delete reconciliation when identities disappear), then resume incremental work. Shared Base Datasets are not rebuilt for a Pipeline revision. Metadata-only edits to optional `description` skip rebuild/re-Delivery. Unrelated Pipelines keep running.
3. Continuous Sync via `migraloop run` (compose default) — Incremental Capture + Delivery for active (non-paused) Pipelines without an external scheduler. Optional one-shot `migraloop sync` remains for Lab / operator catch-up.
4. `migraloop pause --pipeline <name>` / `migraloop resume --pipeline <name>` — stop or continue Delivery/processing for one Pipeline without restarting the Deployment. Pause is durable in the Platform Store; resume catch-up Delivers from current Base/Derived state. Other Pipelines are unaffected. `status` shows `paused` on the Pipeline and its Delivery Health.
5. `migraloop remove --pipeline <name>` — stop the Pipeline and cease Delivery without restarting the Deployment. Shared Base Datasets remain when other Pipelines still reference them; unreferenced Bases are pruned. `status` no longer lists the Pipeline as active. To keep it omitted across a later `apply`, also remove the Pipeline entry from the declarative config.
6. `migraloop status` / `base` / `target` / `derived` — inspect progress and health.

Stream-wide blockers (for example unblockable DDL) still follow [Operations](operations.md) pause guidance; Operator-driven pause/resume/remove/change-via-apply are the first-class control-plane paths for intentional stops and revisions.

## Capture scope

Which Source tables enter Sync is determined by Pipeline `source.table` references **and** any `equiLookup.from` / `union.from` secondary Bases in Transform Pipelines. Each table has at most one Base Dataset per Deployment, shared across Pipelines. New tables get table-level Initial Load only.

Source/Target TLS, secrets, and Source `timezone` (IANA or Oracle-style `±HH:MM` for naive DATE/TIMESTAMP when the DB zone is unreadable) belong on the enclosing Deployment `spec.source` / `spec.target` (not on Pipeline entries)—see [Security](security.md), [Source System](source-system.md), and [CLI & Config](cli-and-config.md).

## Related chapters

- Source prerequisites and types: [Source System](source-system.md)
- Target Binding / Managed fields: [Target System](target-system.md)
- Transform operators: [Rich Transform](rich-transform.md)
- Config field reference: [CLI & Config reference](cli-and-config.md)
