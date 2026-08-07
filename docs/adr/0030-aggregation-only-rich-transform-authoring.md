# Rich Transform authoring is Aggregation-only (breaking)

Operators author Rich Transforms only in a **MongoDB Aggregation–shaped** declarative surface: supported capabilities use the **same stage/accumulator names** as MongoDB (e.g. `$match`, `$project`, `$lookup`, `$group`, `$unwind`, `$unionWith`). Full Aggregation feature parity is not required; unsupported stages reject clearly. The platform still evaluates and maintains Derived Datasets via Affect Analysis—it does **not** run the pipeline on Target MongoDB as a compute engine.

Classic step-name authoring (`project` / `filter` / `groupBy` / …) and thin SQL-ish aliases (`select` / `where` / `join`) are **removed** with **no** read-compat window and **no** automated migration (deliberate exception to the usual Upgrade Compatibility migration expectation; see glossary). Lab Scenarios, handbook, and samples must use `$…` names before the parsers are deleted.

**Rejected alternatives:** (1) keep classic forever beside Aggregation—splits Operator DX and docs; (2) Aggregation-preferred with permanent classic read-compat—leaves a second surface indefinitely; (3) require Mongo server semantic parity or execute transforms on Mongo—conflicts with Platform Store compute and Affect Analysis; (4) keep SQL-ish aliases as sugar—reintroduces dual naming for the same capability.
