# DB Sync Platform

An open-source platform for continuous database-to-database synchronization, with first-class rich transforms that shape multi-table data into derived outputs. Transform compute runs against platform-managed data, not the user's source or target databases. The platform owns applying changes to user-configured target tables.

The product must support **many database engine kinds** over time (multiple source kinds and multiple target kinds). The domain model stays engine-agnostic so new engines plug in without reshaping Sync, Rich Transform, Delivery, or checks. The first shipping pair is **Oracle → MongoDB**; that pair is a vertical slice, not the ceiling.

A single **Deployment** connects **one Source System to one Target System** and may contain **many Pipelines**. Wanting a different database pair means another Deployment (source/target swapped or replaced), not multi-database fan-in inside one Deployment.

Base Datasets, Derived Datasets, and Maintenance State live in a dedicated **Platform Store**: an independent database solely for the platform, never the user's Source or Target System. Its engine brand is chosen by the product and locked—not a user-selectable option.

## Language

**Source System**:
A user database instance the platform captures from (e.g. an Oracle database). Identified by engine kind plus connection identity. The platform must support multiple engine kinds over time.
_Avoid_: Source DB (fine colloquially; prefer Source System when distinguishing from Target System)

**Target System**:
A user database instance the platform delivers into (e.g. a MongoDB deployment). Identified by engine kind plus connection identity. The platform must support multiple engine kinds over time.
_Avoid_: Destination database

**Deployment**:
One running configuration that pairs exactly one Source System with exactly one Target System and hosts one or more Pipelines between them.
_Avoid_: Cluster (infra-flavored), pipeline (a Deployment contains many Pipelines)

**Platform Store**:
The independent database dedicated to platform-managed data (Base Datasets, Derived Datasets, Maintenance State, checkpoints). It exists so Sync, Rich Transform, and Affect Analysis never use the user's Source or Target System as their data plane. The store's engine is a product-locked choice, not configured per user preference (see ADR-0001). Default installation ships the Platform Store with the app as a separate container beside the app (one install, two containers): engine locked, Postgres settings/volumes/resources user-tunable; not BYO alternate engines.
_Avoid_: Using source/target as platform storage, user-pluggable store engine, BYO database for the platform data plane, single-process “DB inside the app container” as the default

**Sync**:
Continuous one-way capture of changes from a Source System into platform-managed Base Datasets. Latency and resumability are first-class. Sync alone does not imply the Target System has been updated—that is Delivery. Reverse flow is not a product feature; users who need the opposite direction create a separate Deployment with source and target swapped. Capture mechanics are engine-specific; the Sync concept is not. Sync has two first-class phases: Initial Load then Incremental Capture.
_Avoid_: Replication (unless referring to the underlying mechanism), mirror-only (implies zero transform capability), bidirectional sync, active-active

**Initial Load**:
The first materialization of needed Base Datasets (and then Derived Datasets / Delivery) from the Source System so a Pipeline can start from existing data, not only future changes. It inherently reads large volumes from the source, but the platform must be designed so it does not overwhelm the Source System: chunked reads, rate limits, pause/resume, and backoff under pressure. v1 uses the same Source connection for Initial Load and Incremental Capture; splitting read/CDC connections is optional later, not required for safety.
_Avoid_: Zero-impact backfill, unbounded full-table slam, assuming a separate replica connection is required for correctness

**Incremental Capture**:
Ongoing change capture into Base Datasets after Initial Load, driving Affect Analysis, Derived updates, and Delivery.
_Avoid_: Initial Load (different phase)

**Base Dataset**:
A platform-managed copy of a source table or collection, kept aligned by Sync, close to the source shape. It is the unit Rich Transforms and direct Pipelines may read; it is not the user's source or target database. Within a Deployment, each source table/collection has at most one Base Dataset, shared by every Pipeline that needs it—never captured or stored once per Pipeline.
_Avoid_: Raw table (ambiguous), source mirror, target table, per-pipeline copy of the same source table

**Rich Transform**:
A user-defined transformation composed of operators the platform can analyze. It reads only platform-managed data—never the user's source or target DB as a compute engine. From the Pipeline definition alone, the platform must determine which Base fields and values each Derived result depends on, so incremental maintenance knows what must be recomputed and what must not. Definitions are **declarative** (DSL/config of supported operators only). Free-form user scripts are out of scope because they make Affect Analysis impossible. A UI may author the same declarative definition later; the declarative form remains the source of truth. The v1 operator surface is anchored on **Oracle → MongoDB** (document-friendly, incrementally maintainable)—including project/addFields/rename/remove, filter, equiLookup, unwind, groupBy with sum/count/min/max/avg, distinct/addToSet, and union. Broader Mongo stages that break Affect Analysis stay out. Later source/target engines adapt or subset this surface; they do not each get a separate transform language.
_Avoid_: Thin mapping, light transform, ETL job (too generic), opaque free-form scripts the platform cannot analyze, arbitrary SQL/JS as the transform definition, engine-specific transform dialects per pair

**Affect Analysis**:
Strict determination, from the Pipeline's Rich Transform definition and an incoming Base change, of which Output Identities (if any) require Derived recomputation. Unused fields must not trigger recompute (e.g. an order address update does not recompute a sum-of-price-by-customer). Operator semantics decide value-level cases (e.g. distinct-customer count updates for a new customer id, but not for a duplicate already-counted id).
_Avoid_: Heuristic invalidation, always-recompute, best-effort skip

**Maintenance State**:
Platform-internal state kept only when an operator needs it for correct incremental Affect Analysis or updates beyond what the Derived Dataset and the change themselves already provide. Example: per-`customerId` row counts to know whether a distinct-customer aggregate must change. It must not be created blindly—e.g. `sum(price) by customerId` should not invent extra structures if the Derived totals plus the change suffice. Never stored in the user's source or target for this purpose.
_Avoid_: Always-on side tables per Pipeline, dumping maintenance data into the Target System

**Derived Dataset**:
The platform-managed output produced by a Rich Transform; a dataset the platform materializes and maintains, not a verbatim copy of a single source table. When Base Datasets change, the platform must update the Derived Dataset **incrementally by Output Identity**, driven by Affect Analysis: never recompute work that Affect Analysis proves unnecessary. Prefer operator-equivalent fast paths (e.g. adjusting a per-identity sum using pre-apply Base values) when they are correct; otherwise recompute from the platform Base inputs for only the affected identities. If an identity’s input set is huge, that per-identity cost may be unavoidable. Steady-state full recompute of an entire Derived Dataset is unacceptable. Incremental maintenance must be **correct**—semantically equivalent to re-evaluating the Rich Transform for affected identities.
_Avoid_: View (implies non-materialized / DB-native only), sink table (implementation-flavored), periodic full-table recompute as the normal path, recomputing unaffected identities

**Pipeline**:
A user-defined flow inside a Deployment that produces one target table/collection. A Deployment has many Pipelines; a Pipeline does not define the Source/Target System pair—that is the Deployment. Every Pipeline is in one of two modes: Direct or Transform.
_Avoid_: The whole system config, Deployment, single global job

**Direct Pipeline**:
A Pipeline with no Rich Transform: one Base Dataset is Delivered to the Target Binding. For Oracle → MongoDB, the default shape is one source row → one document with flattened fields; the source primary key maps to the document identity (`_id` or a configured id field). There is no useful alternate default without a transform.
_Avoid_: Mirror mode (implies zero field mapping/config), raw dump

**Transform Pipeline**:
A Pipeline that runs a Rich Transform over one or more Base Datasets, materializes a Derived Dataset, and Delivers that to the Target Binding. It must declare an Output Identity before it can run.
_Avoid_: ETL job (too generic), thin mapping-only pipeline

**Output Identity**:
The stable key that locates one output row/document on the Target System for Delivery insert/update/delete (and Drift Check). For a Direct Pipeline it defaults to the source primary key. For a Transform Pipeline the user must define it over the Rich Transform output. It must be deterministic from the input/transform data—no randomness (e.g. generated UUIDs), so the same logical result always resolves to the same target key.
_Avoid_: Surrogate random id, guessed primary key, source PK (when the transform’s grain differs)

**Target Binding**:
The part of a Pipeline that maps its output dataset (Base or Derived) to a specific table/collection in the Target System. It declares Managed Columns/fields and (with the Pipeline) the Output Identity; the target table/collection may have additional fields outside the binding.
_Avoid_: Destination (vague), sink (implementation-flavored)

**Managed Columns**:
The columns/fields the Pipeline defines as its output shape and that Delivery will write. On document Target Systems (e.g. MongoDB), the platform does not inventory “non-managed” fields—it simply never writes keys outside the Managed set, so other fields are naturally untouched. On relational Target Systems (e.g. PostgreSQL), Managed Columns are the schema the platform must create/maintain on the target table; other columns remain out of scope for updates. This document-vs-relational distinction is part of the domain model even though v1 only ships MongoDB as a Target System.
_Avoid_: All columns, full document ownership (unless the binding literally lists every field), requiring a catalog of non-managed fields on document stores

**Delivery**:
The platform-owned process that applies insert/update/delete for a Pipeline's Output Identity on the Target System so the user does not implement write logic themselves. Updates write only Managed Columns/fields. On document targets, unknown other fields are simply not touched. On relational targets, Delivery also implies establishing Managed Columns in the table schema—specified now for design continuity, implemented when a relational Target System ships. When an Output Identity no longer exists, Delivery may **delete the entire target document/row**. Writing to the target for Delivery is allowed; using the target as Rich Transform input/compute is not. v1 implements document Delivery (MongoDB) only.
_Avoid_: Load job (too batch-flavored), sync (overloaded—Sync is capture into the platform), overwriting document fields outside the Managed set, dropping relational columns the platform does not own

**Sync Health**:
Whether capture from source into a Base Dataset is caught up and applying successfully (lag, checkpoints, capture/apply failures). Necessary but not sufficient to claim the Base Dataset matches the source.
_Avoid_: Sync success (ambiguous), replication lag (mechanism-specific)

**Source Alignment Check**:
A non-real-time, resource-gated verification that a Base Dataset matches its source. Required before the platform may treat that Base Dataset as a reliable baseline for Drift Check. Must keep source reads lightweight and run only when the source has enough spare capacity. When misalignment is found, the platform repairs the Base Dataset from source using data already required for the check where possible—not by writing to the source.
_Avoid_: Trusting Sync Health alone, full table dump

**Delivery Health**:
Whether the change stream for a Pipeline's Target Binding is caught up and applying successfully (lag, checkpoints, apply failures). Edits to non-Managed Columns are irrelevant to this signal.
_Avoid_: Sync success (ambiguous), replication lag (mechanism-specific)

**Drift Check**:
A non-real-time, resource-gated verification that Managed Columns on the target match the platform's expected dataset for that Pipeline. Uses the platform dataset as baseline only when Source Alignment (for Bases) or equivalent Derived correctness guarantees hold. By default, detected drift on Managed Columns is auto-repaired back to the Pipeline's expected values; non-Managed Columns are ignored. Auto-repair must not imply extra source load beyond what alignment/verification already requires.
_Avoid_: Sync check (ambiguous), audit (too vague), preserving manual edits on Managed Columns
