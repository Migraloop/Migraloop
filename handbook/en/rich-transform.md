# Rich Transform

A **Rich Transform** is a user-defined, **declarative** transformation composed only of operators the platform can analyze. It reads **platform-managed** Base Datasets—never the user’s Source or Target as a compute engine. Free-form SQL/JS scripts are rejected because they make **Affect Analysis** impossible.

## When to use it

Use a **Transform Pipeline** (`mode: transform`) when the Target document grain or shape differs from a single source row—filters, projections, or aggregations. For one-row → one-document copies, prefer a **Direct Pipeline**.

Transform Pipelines must declare:

- `outputIdentity` — stable key fields for Delivery insert/update/delete
- `transform` — ordered list of declarative operator steps

## v1 operator surface (implemented)

The shipped parser currently accepts these analyzable operators (Oracle → MongoDB slice):

### `project`

Keep only listed fields:

```yaml
- project:
    fields: [ID, CUSTOMER_ID, AMOUNT]
```

### `addFields`

Add Managed fields as a literal JSON value or a copy of an existing field (exactly one of `value` or `field`):

```yaml
- addFields:
    fields:
      - as: currency
        value: USD
      - as: displayName
        field: customerName
```

### `rename`

Rename fields (`from` → `to`):

```yaml
- rename:
    fields:
      - from: NAME
        to: customerName
```

### `remove`

Drop fields from the row (unused for Affect Analysis after removal):

```yaml
- remove:
    fields: [EMAIL, NOTES]
```

### `filter`

Equality filter on one field:

```yaml
- filter:
    field: STATUS
    eq: OPEN
```

### `groupBy`

Group keys plus aggregates. v1 aggregate ops: `sum`, `count`, `min`, `max`, `avg`.
Each aggregate requires `field` and `as`. `count` counts non-null values of `field`
(SQL `COUNT(field)`). `min` / `max` / `avg` / `sum` use precision-preserving decimal
arithmetic (not IEEE double). Empty groups are omitted; `min` / `max` / `avg` over
only-null field values yield JSON `null`, while `count` is `0` and `sum` is `0`.

```yaml
- groupBy:
    keys: [CUSTOMER_ID]
    aggregates:
      - op: count
        field: ORDER_ID
        as: ORDER_COUNT
      - op: min
        field: AMOUNT
        as: MIN_AMOUNT
      - op: max
        field: AMOUNT
        as: MAX_AMOUNT
      - op: avg
        field: AMOUNT
        as: AVG_AMOUNT
      - op: sum
        field: AMOUNT
        as: TOTAL_AMOUNT
```

These aggregations do **not** invent Maintenance State: incremental updates recompute
only affected Output Identities from Base. Unused-field changes (for example ADDRESS
when aggregates read ORDER_ID/AMOUNT) skip Derived recompute.

### `equiLookup`

Left-outer equijoin against another **Base Dataset** in the same Deployment. Matching
foreign rows are embedded as an array under `as`. The Pipeline's `source.table` is the
left (primary) Base; `from` names the secondary Base (Initial Load + Incremental Capture
include both). Optional `fromSchema` overrides the secondary schema (defaults to the
Pipeline source schema).

```yaml
- equiLookup:
    from: ORDERS
    localField: ID
    foreignField: CUSTOMER_ID
    as: orders
```

Free-form Mongo `$lookup` (including `pipeline` / `let` extensions) is rejected—use this
declarative form so **Affect Analysis** stays correct. A change on either Base side
updates only the affected primary Output Identities; unused primary fields (for example
EMAIL after `project`) still skip recompute. Embedded foreign rows include full Base
fields, so foreign-side field changes recompute matching identities.

Domain roadmap also names operators such as unwind, distinct/addToSet, and union. Until
those land in the CLI config parser, declare only the operators above—unsupported
operator names fail apply.

## Output Identity

**Output Identity** locates one document on the Target for Delivery and Drift Check. It must be deterministic from transform inputs—no random UUIDs. For aggregations, identity usually matches the `groupBy` keys.

## Affect Analysis

**Affect Analysis** decides, from the transform definition and an incoming Base change, which Output Identities (if any) need Derived recomputation. Unused fields must not trigger recompute (for example an address-only update must not recompute a sum-of-amount-by-customer). Operator semantics decide value-level cases (e.g. distinct/count style updates).

When a `groupBy` key changes on a Base row, Affect Analysis reads the Base row **before** applying the change so both the old and new Output Identities are updated (adjust or remove the old identity; upsert the new one). It must not rely on overwriting Base first and then trying to recover the prior key.

Steady-state full recompute of an entire Derived Dataset is unacceptable. Prefer operator-equivalent fast paths when correct; otherwise recompute only affected identities from platform Base inputs.

Inspect Derived rows:

```bash
migraloop derived --pipeline orders_by_customer
```

## Related chapters

- Pipeline declaration: [Pipeline](pipeline.md)
- Delivery of Derived output: [Target System](target-system.md)
- Health of transform Pipelines: [Observability](observability.md)
