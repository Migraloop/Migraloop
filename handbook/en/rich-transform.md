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

### `filter`

Equality filter on one field:

```yaml
- filter:
    field: STATUS
    eq: OPEN
```

### `groupBy`

Group keys plus aggregates. v1 aggregate op: `sum`.

```yaml
- groupBy:
    keys: [CUSTOMER_ID]
    aggregates:
      - op: sum
        field: AMOUNT
        as: TOTAL_AMOUNT
```

Domain roadmap also names operators such as rename/remove, equiLookup, unwind, count/min/max/avg, distinct/addToSet, and union. Until those land in the CLI config parser, declare only `project` / `filter` / `groupBy` above—unsupported operator names fail apply.

## Output Identity

**Output Identity** locates one document on the Target for Delivery and Drift Check. It must be deterministic from transform inputs—no random UUIDs. For aggregations, identity usually matches the `groupBy` keys.

## Affect Analysis

**Affect Analysis** decides, from the transform definition and an incoming Base change, which Output Identities (if any) need Derived recomputation. Unused fields must not trigger recompute (for example an address-only update must not recompute a sum-of-amount-by-customer). Operator semantics decide value-level cases (e.g. distinct/count style updates).

Steady-state full recompute of an entire Derived Dataset is unacceptable. Prefer operator-equivalent fast paths when correct; otherwise recompute only affected identities from platform Base inputs.

Inspect Derived rows:

```bash
migraloop derived --pipeline orders_by_customer
```

## Related chapters

- Pipeline declaration: [Pipeline](pipeline.md)
- Delivery of Derived output: [Target System](target-system.md)
- Health of transform Pipelines: [Observability](observability.md)
