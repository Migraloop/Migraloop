# Rich Transform

A **Rich Transform** is a user-defined, **declarative** transformation composed only of operators the platform can analyze. It reads **platform-managed** Base Datasets—never the user’s Source or Target as a compute engine. Free-form SQL/JS scripts are rejected because they make **Affect Analysis** impossible.

## When to use it

Use a **Transform Pipeline** (`mode: transform`) when the Target document grain or shape differs from a single source row—filters, projections, or aggregations. For one-row → one-document copies, prefer a **Direct Pipeline**.

Transform Pipelines must declare:

- `outputIdentity` — stable key fields for Delivery insert/update/delete
- `transform` — ordered list of declarative operator steps

## Authoring forms (expand)

Operators may author the same analyzable surface in either form—both normalize to one IR for **Affect Analysis**:

1. **Classic steps** — `project`, `filter`, `equiLookup`, `groupBy`, … (examples below; still fully supported).
2. **Aggregation / SQL-like DX** — MongoDB Aggregation–shaped stages (`$project`, `$match`, `$lookup`, `$group`, …) plus thin SQL-ish aliases (`select`, `where`, `join`).

Example (same Pipeline as classic `project` + `filter`):

```yaml
transform:
  - $project:
      ID: 1
      NAME: 1
      ACTIVE: 1
  - $match:
      ACTIVE: 1
```

Equivalent SQL-ish aliases:

```yaml
transform:
  - select:
      fields: [ID, NAME, ACTIVE]
  - where:
      field: ACTIVE
      eq: 1
```

Constrained Aggregation stages map as follows (unanalyzable extensions stay rejected):

| Aggregation / SQL-like | Classic equivalent | Notes |
| --- | --- | --- |
| `$project` / `select` | `project` | Inclusion map (`FIELD: 1`) or `{ fields: [...] }` |
| `$match` / `where` | `filter` | Single-field equality only |
| `$addFields` / `$set` | `addFields` | `"$field"` copy or `{ $literal: ... }` / JSON literal |
| `$unset` | `remove` | Field name or array of names |
| `$rename` | `rename` | `{ FROM: TO }` map |
| `$lookup` / `join` | `equiLookup` | Equijoin only — no `pipeline` / `let` |
| `$unwind` | `unwind` | Path string or `{ path }` — no `preserveNullAndEmptyArrays` |
| `$unionWith` | `union` | `coll` / `from` / string — no nested `pipeline` |
| `$group` | `groupBy` / `distinct` / `addToSet` | `_id: "$KEY"`; accumulators `$sum`/`$count`/`$min`/`$max`/`$avg`/`$addToSet` |

Existing Deployments that use classic steps keep working (Upgrade Compatibility). Prefer one form per Pipeline for readability; mixing classic and Aggregation steps in one list is allowed when each step is valid.

## v1 operator surface (implemented)

The shipped parser currently accepts these analyzable operators (Oracle → MongoDB slice)—shown in classic form; see **Authoring forms** for Aggregation equivalents:

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

### `distinct`

One Derived row per unique combination of `fields` (SQL `DISTINCT` semantics). Output
Identity typically matches those fields.

```yaml
- distinct:
    fields: [CUSTOMER_ID]
```

### `addToSet`

Group by `keys` and collect unique non-null values of `field` into a JSON array `as`
(Mongo-style `$addToSet`). Values in the array are ordered deterministically.

```yaml
- addToSet:
    keys: [CUSTOMER_ID]
    field: AMOUNT
    as: AMOUNTS
```

`distinct` and `addToSet` **do** create **Maintenance State** (per-identity / per-member
refcounts in the Platform Store) so value-level Affect Analysis can skip useless
Derived updates—for example inserting a duplicate `CUSTOMER_ID` that is already
counted, or an `AMOUNT` already present in the set. v1 allows at most one `distinct`
or `addToSet` operator per transform. Simple `groupBy` sum/count/min/max/avg still
must not invent Maintenance State.

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

Use classic `equiLookup` or constrained Aggregation `$lookup` / `join` with the same
equijoin fields. Free-form Mongo `$lookup` extensions (`pipeline` / `let`) are
rejected so **Affect Analysis** stays correct. A change on either Base side
updates only the affected primary Output Identities; unused primary fields (for example
EMAIL after `project`) still skip recompute. Embedded foreign rows include full Base
fields, so foreign-side field changes recompute matching identities.

### `unwind`

Expand an array field into one Derived row per element (1→N grain). Typical composition
is `equiLookup` then `unwind` so Delivery can key documents by unwound Output Identity
(for example `ORDER_ID`).

```yaml
- unwind:
    path: orders
```

When an array element is an object, its fields are **merged into the parent row** and the
array path is removed (Delivery-friendly flatten). Scalar elements replace the path value
(Mongo-style). Missing, null, or empty arrays emit no rows. Use classic `unwind` or
Aggregation `$unwind` (`"$path"` or `{ path }`). Options such as
`preserveNullAndEmptyArrays` / `includeArrayIndex` are rejected so
**Affect Analysis** can expand only the affected Output Identities—including deletes when
array members disappear.

### `union`

Concatenate another **Base Dataset** into the stream (SQL `UNION ALL` / Mongo `$unionWith`
without a nested pipeline). The Pipeline's `source.table` is the primary Base; `from`
names the secondary Base (Initial Load + Incremental Capture include both). Rows already
shaped by prior steps come first; secondary Base rows are appended as-is; later steps
(for example `project`) apply to both sides. Optional `fromSchema` overrides the secondary
schema (defaults to the Pipeline source schema).

```yaml
- union:
    from: WEST_CUSTOMERS
- project:
    fields: [ID, NAME]
```

Use classic `union` or constrained Aggregation `$unionWith` (`coll` / `from` / string name).
Nested `$unionWith` `pipeline` extensions are rejected so **Affect Analysis** stays correct.
A change on either contributing Base updates only the affected Output Identities; unused
fields after a following `project` (for example EMAIL) still skip recompute. v1 does not
combine `union` with `distinct` / `addToSet`. Choose **Output Identity** values that stay
unique across contributing Bases—Delivery upserts one Target document per identity (SQL
`UNION ALL` row multiplicity does not create multiple Mongo documents for the same key).

## Output Identity

**Output Identity** locates one document on the Target for Delivery and Drift Check. It must be deterministic from transform inputs—no random UUIDs. For aggregations, identity usually matches the `groupBy` keys.

## Affect Analysis

**Affect Analysis** decides, from the transform definition and an incoming Base change, which Output Identities (if any) need Derived recomputation. Unused fields must not trigger recompute (for example an address-only update must not recompute a sum-of-amount-by-customer). For `distinct` / `addToSet`, Maintenance State enables value-level skips when a duplicate key or set member is already counted (and when a delete is not the last contributor).

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
