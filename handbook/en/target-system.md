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
| `tls` | Optional. Set `enabled: true` for Mongo TLS; `caFile` is a filesystem CA path (not PEM inline). See [Security](security.md) |

Target timezone is not configured in v1—Delivery writes UTC datetime for temporal Managed fields (values already normalized from the Source DB timezone or Deployment Source `timezone`, which may be an IANA name or Oracle-style `±HH:MM`).

## Target Binding

Each Pipeline that Delivers declares a **Target Binding**: which collection receives the Pipeline’s output.

```yaml
target:
  collection: orders
```

The binding (with the Pipeline) also implies **Output Identity** and **Managed Columns**. The Target collection may hold additional fields outside the binding.

## Managed Columns / fields

**Managed Columns** (document fields in v1) are the output shape Delivery will write. Delivery ownership differs by Target kind (ADR-0002). **v1 ships MongoDB document Delivery only**; the relational rules below are design continuity for later relational Target Systems—not a v1 Delivery runtime.

### Document targets (v1: MongoDB)

- The platform does **not** inventory non-managed fields—it simply never writes keys outside the Managed set, so other fields stay untouched.
- When an **Output Identity** no longer exists in the platform dataset, Delivery may **delete the entire target document**.

### Relational targets (future)

On relational Target Systems, Managed Columns are **schema the platform must create and maintain** on the target table:

- Delivery **creates/maintains** only Managed Columns in the table schema.
- Non-managed columns stay **out of scope for updates**—the platform does not own, alter, or overwrite them.
- When an **Output Identity** disappears, Delivery may still **delete the entire target row** (full-row delete by Output Identity), same as document targets.

### Reliability

Reliability is **at-least-once with idempotent apply**: retries may rewrite the same identity; Managed results are upserted/deleted by identity.

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

Concrete MongoDB privileges for the Delivery account (ADR-0016). **`root` / `clusterAdmin` is not required**—use a dedicated user scoped to the Target database (and, when your ops model allows, only the bound collections).

### Required for v1 Delivery + Target inspection

Delivery performs upsert (`update` with `upsert: true`) and `delete` by Output Identity on each Pipeline’s bound collection, and `find` for `migraloop target` inspection. Minimum role on the Target database named in `spec.target.database`:

```javascript
use admin
db.createUser({
  user: "deliver_user",
  pwd: passwordPrompt(),  // or your secret-manager injection
  roles: [
    { role: "readWrite", db: "<target_database>" }
  ]
})
```

`readWrite` on that database includes `find`, `insert`, `update`, `remove`, and `createCollection` on its collections—enough for Delivery and CLI Target inspection when collections may be created on first write.

### Narrower custom role (optional)

If you pre-create every bound collection and want collection-scoped grants instead of database `readWrite`:

```javascript
use <target_database>
db.createRole({
  role: "migraloopDeliver",
  privileges: [
    {
      resource: { db: "<target_database>", collection: "<bound_collection>" },
      actions: ["find", "insert", "update", "remove"]
    }
    // repeat resource+actions per Pipeline Target Binding
  ],
  roles: []
})
use admin
db.createUser({
  user: "deliver_user",
  pwd: passwordPrompt(),
  roles: [{ role: "migraloopDeliver", db: "<target_database>" }]
})
```

Add `createCollection` on the database (or create collections ahead of time) if first Delivery must create a missing collection.

### Optional / not required

| Privilege | Status |
| --- | --- |
| `root`, `clusterAdmin`, `dbAdminAnyDatabase` | **Not required** for Delivery. Local Sync Lab’s disposable Mongo user is root for Fixture convenience only—not the production default. |
| `dropCollection` / `dropDatabase` | **Not required** for product Delivery (Lab Scenario cleanup may use broader Fixture credentials). |

Connection strings in v1 authenticate with `authSource=admin` (see Delivery URI construction). Create the user in the auth database your deployment expects, and keep the password in a secret reference ([Security](security.md)). Source sync account grants: [Source System](source-system.md).

## Supported mapping notes

Source allow-list and NUMBER/temporal rules affect what can appear in Managed output—see [Source System](source-system.md). Unsafe NUMBER columns must be mapped with Pipeline `fields` before apply succeeds.

## Related chapters

- Deployment pairing: [Deployment](deployment.md)
- Pipeline modes and `fields`: [Pipeline](pipeline.md)
- Delivery Health: [Observability](observability.md)
- Secrets, TLS, and privilege pointers: [Security](security.md#required-privileges-pointer)
- Oracle sync grants: [Source System](source-system.md#required-privileges)
