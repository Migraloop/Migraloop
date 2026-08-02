# v1 release requires correctness, performance, and basic fault tests

A production release must pass CI for: unit and integration correctness (including Affect Analysis, Initial Load↔Incremental Capture hand-off, idempotent Delivery), Oracle→Mongo contract tests, performance/load benchmarks with regression thresholds, and basic fault/error cases (restart resume, obvious failure paths). We do not require multi-week chaos engineering or full production-scale endurance as a v1 gate; those remain optional/periodic hardening.

Extended by ADR-0028: the same gate runs on every PR/push, and each shipped capability with a Lab Scenario also needs a contract-path CI twin (Lab catalog still is not CI).
