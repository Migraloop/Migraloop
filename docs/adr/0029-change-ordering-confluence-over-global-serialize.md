# Change Ordering: same-key order, confluence over global serialize

Sync must keep **same source key** changes in capture order within a Base Dataset. It must **not** require a global Deployment total order. Across different source keys, reordering or parallelism is allowed only when Direct Delivery or Transform maintenance still reaches the correct eventual Managed state (see glossary **Change Ordering** / **Eventual Consistency**).

For Transform Pipelines, serialize an Output Identity (or its contributing inputs) only when incremental maintenance **cannot** be shown confluent under reorder—for example when a running aggregate lacks enough information after a delete or “worsening” update. In those cases prefer **Maintenance State** or **per-Output-Identity recompute from Base**, not “fix it later” via Source Alignment Check / Drift Check on the normal path. Those checks remain edge-case safety nets, not permission to abandon Incremental Capture correctness.

**Rejected alternatives:** (1) serialize every Pipeline / every Output Identity for simplicity—blocks meaningful throughput work; (2) treat all Rich Transforms as order-insensitive without a confluence/recompute rule—risks silent wrong aggregates; (3) accelerate only Direct Pipelines—Transform is first-class and must get the same performance attention once the shipped-capability Lab gate is green.

**Consequences:** Parallelism and batching designs may overlap work across keys and across Direct / Transform Pipelines, but must preserve per-key order into Base apply. Post-correctness performance work is explicitly **both** Direct and Transform—not Direct-only.
