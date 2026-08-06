# Rich Transform

A **Rich Transform** is a user-defined, **declarative** transformation composed only of operators the platform can analyze. It reads **platform-managed** Base Datasets—never the user’s Source or Target as a compute engine. Free-form SQL/JS scripts are rejected because they make **Affect Analysis** impossible.

## When to use it

Use a **Transform Pipeline** (`mode: transform`) when the Target document grain or shape differs from a single source row—filters, projections, or aggregations. For one-row → one-document copies, prefer a **Direct Pipeline**.

Transform Pipelines must declare:

- `outputIdentity` — stable key fields for Delivery insert/update/delete
- `transform` — ordered list of declarative operator steps

## Authoring forms

Operators author the same analyzable surface in either form—both normalize to one IR for **Affect Analysis**:

1. **Aggregation / SQL-like DX (preferred)** — MongoDB Aggregation–shaped stages (`$project`, `$match`, `$lookup`, `$group`, …) plus thin SQL-ish aliases (`select`, `where`, `join`). This is the supported authoring path for new Pipelines and shipped Lab Scenarios.
2. **Classic steps (Upgrade Compatibility)** — `project`, `filter`, `equiLookup`, `groupBy`, … Existing Deployments that use classic steps keep applying without a forced rewrite.

Example (preferred Aggregation form):

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
| `$group` | `groupBy` / `distinct` / `addToSet` | `_id: "$KEY"`; accumulators `$sum`/`$count`/`$min`/`$max`/`$avg`/`$addToSet`. `$count` takes a field ref (`{ $count: "$ORDER_ID" }` = SQL `COUNT(field)`), not Mongo’s empty `{ $count: {} }` |

Prefer one form per Pipeline for readability; mixing classic and Aggregation steps in one list is allowed when each step is valid.

### Migration notes

- **No wipe required to keep classic YAML.** Upgrading the app does not force Operators to rewrite classic `project` / `filter` / `groupBy` / … Deployments (Upgrade Compatibility / ADR-0014). Classic steps still parse and Sync.
- **New authoring should use Aggregation DX.** Lab Scenarios and handbook examples use `$project` / `$match` / `$group` / … as the supported style.
- **Rewriting form in config is a Pipeline revision.** Authored `transform` JSON is stored as written. Replacing classic steps with IR-equivalent Aggregation YAML (or the reverse) changes the stored declaration, so `migraloop apply` treats it as a **semantic Pipeline revision**: pause that Pipeline’s old Delivery, rebuild its Derived Dataset, re-Deliver (including delete reconciliation), then resume. Prefer migrating when you already intend a revision window—or leave classic Deployments unchanged.
- **Capability names in catalogs stay classic.** Coverage rows and glossary still say `project` / `equiLookup` / `groupBy` for the analyzable surface; authoring may use either form.

## v1 operator surface (implemented)

The shipped parser accepts these analyzable operators (Oracle → MongoDB slice)—shown in preferred Aggregation form. Classic equivalents remain supported (see mapping table above).

### `$project`

Keep only listed fields (inclusion map or `{ fields: [...] }`):

```yaml
- $project:
    ID: 1
    CUSTOMER_ID: 1
    AMOUNT: 1
```

### `$addFields` / `$set`

Add Managed fields as a literal JSON value or a copy of an existing field:

```yaml
- $addFields:
    currency: USD
    displayName: "$customerName"
```

(`{ $literal: USD }` is also accepted for literals.)

### `$rename`

Rename fields (`FROM` → `TO`):

```yaml
- $rename:
    NAME: customerName
```

### `$unset`

Drop fields from the row (unused for Affect Analysis after removal):

```yaml
- $unset: [EMAIL, NOTES]
```

### `$match`

Equality filter on one field:

```yaml
- $match:
    STATUS: OPEN
```

### `$group` (aggregations)

Group keys plus aggregates. v1 aggregate ops: `$sum`, `$count`, `$min`, `$max`, `$avg`.
`$count` counts non-null values of the referenced field (SQL `COUNT(field)`). `$min` /
`$max` / `$avg` / `$sum` use precision-preserving decimal arithmetic (not IEEE double).
Empty groups are omitted; `$min` / `$max` / `$avg` over only-null field values yield JSON
`null`, while `$count` is `0` and `$sum` is `0`.

```yaml
- $group:
    _id: "$CUSTOMER_ID"
    ORDER_COUNT:
      $count: "$ORDER_ID"
    MIN_AMOUNT:
      $min: "$AMOUNT"
    MAX_AMOUNT:
      $max: "$AMOUNT"
    AVG_AMOUNT:
      $avg: "$AMOUNT"
    TOTAL_AMOUNT:
      $sum: "$AMOUNT"
```

These aggregations do **not** invent Maintenance State: incremental updates recompute
only affected Output Identities from Base. Unused-field changes (for example ADDRESS
when aggregates read ORDER_ID/AMOUNT) skip Derived recompute.

### `$group` for distinct

One Derived row per unique key (SQL `DISTINCT` semantics)—`$group` with `_id` only.
Output Identity typically matches those fields.

```yaml
- $group:
    _id: "$CUSTOMER_ID"
```

### `$group` with `$addToSet`

Group by `_id` and collect unique non-null values into a JSON array (Mongo-style
`$addToSet`). Values in the array are ordered deterministically.

```yaml
- $group:
    _id: "$CUSTOMER_ID"
    AMOUNTS:
      $addToSet: "$AMOUNT"
```

Distinct and `$addToSet` **do** create **Maintenance State** (per-identity / per-member
refcounts in the Platform Store) so value-level Affect Analysis can skip useless
Derived updates—for example inserting a duplicate `CUSTOMER_ID` that is already
counted, or an `AMOUNT` already present in the set. v1 allows at most one distinct
or `$addToSet` operator per transform. Simple `$group` sum/count/min/max/avg still
must not invent Maintenance State.

### `$lookup`

Left-outer equijoin against another **Base Dataset** in the same Deployment. Matching
foreign rows are embedded as an array under `as`. The Pipeline's `source.table` is the
left (primary) Base; `from` names the secondary Base (Initial Load + Incremental Capture
include both). Optional `fromSchema` overrides the secondary schema (defaults to the
Pipeline source schema).

```yaml
- $lookup:
    from: ORDERS
    localField: ID
    foreignField: CUSTOMER_ID
    as: orders
```

Use constrained Aggregation `$lookup` / `join` (or classic `equiLookup`) with the same
equijoin fields. Free-form Mongo `$lookup` extensions (`pipeline` / `let`) are
rejected so **Affect Analysis** stays correct. A change on either Base side
updates only the affected primary Output Identities; unused primary fields (for example
EMAIL after `$project`) still skip recompute. Embedded foreign rows include full Base
fields, so foreign-side field changes recompute matching identities.

### `$unwind`

Expand an array field into one Derived row per element (1→N grain). Typical composition
is `$lookup` then `$unwind` so Delivery can key documents by unwound Output Identity
(for example `ORDER_ID`).

```yaml
- $unwind: "$orders"
```

When an array element is an object, its fields are **merged into the parent row** and the
array path is removed (Delivery-friendly flatten). Scalar elements replace the path value
(Mongo-style). Missing, null, or empty arrays emit no rows. Options such as
`preserveNullAndEmptyArrays` / `includeArrayIndex` are rejected so
**Affect Analysis** can expand only the affected Output Identities—including deletes when
array members disappear.

### `$unionWith`

Concatenate another **Base Dataset** into the stream (SQL `UNION ALL` / Mongo `$unionWith`
without a nested pipeline). The Pipeline's `source.table` is the primary Base; the
`$unionWith` name is the secondary Base (Initial Load + Incremental Capture include both).
Rows already shaped by prior steps come first; secondary Base rows are appended as-is;
later steps (for example `$project`) apply to both sides. Optional `fromSchema` overrides
the secondary schema (defaults to the Pipeline source schema).

```yaml
- $unionWith: WEST_CUSTOMERS
- $project:
    ID: 1
    NAME: 1
```

Nested `$unionWith` `pipeline` extensions are rejected so **Affect Analysis** stays correct.
A change on either contributing Base updates only the affected Output Identities; unused
fields after a following `$project` (for example EMAIL) still skip recompute. v1 does not
combine `$unionWith` with distinct / `$addToSet`. Choose **Output Identity** values that stay
unique across contributing Bases—Delivery upserts one Target document per identity (SQL
`UNION ALL` row multiplicity does not create multiple Mongo documents for the same key).

## Output Identity

**Output Identity** locates one document on the Target for Delivery and Drift Check. It must be deterministic from transform inputs—no random UUIDs. For aggregations, identity usually matches the `$group` `_id` keys.

## Affect Analysis

**Affect Analysis** decides, from the transform definition and an incoming Base change, which Output Identities (if any) need Derived recomputation. Unused fields must not trigger recompute (for example an address-only update must not recompute a sum-of-amount-by-customer). For distinct / `$addToSet`, Maintenance State enables value-level skips when a duplicate key or set member is already counted (and when a delete is not the last contributor).

When a `$group` key changes on a Base row, Affect Analysis reads the Base row **before** applying the change so both the old and new Output Identities are updated (adjust or remove the old identity; upsert the new one). It must not rely on overwriting Base first and then trying to recover the prior key.

Steady-state full recompute of an entire Derived Dataset is unacceptable. Prefer operator-equivalent fast paths when correct; otherwise recompute only affected identities from platform Base inputs.

Inspect Derived rows:

```bash
migraloop derived --pipeline orders_by_customer
```

## Related chapters

- Pipeline declaration: [Pipeline](pipeline.md)
- Delivery of Derived output: [Target System](target-system.md)
- Health of transform Pipelines: [Observability](observability.md)
