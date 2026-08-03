# CI twin coverage matrix (Lab ↔ Release Quality Gate)

Policy: ADR-0028 / issue #96 (parent #94). Analogous in spirit to Lab [`lab/scenarios/COVERAGE.md`](../../lab/scenarios/COVERAGE.md)—not a Lab Scenario runner.

Rows are **currently shipped** Lab catalog capabilities. Evidence cells must point at **non-ignored** `migraloop-app` contract-path tests (or equivalent always-on CI evidence) that `rqg-integration` runs via `cargo test -p migraloop-app`. Ignored Lab Fixture / Lab Scenario / live Oracle tests are **not** gate evidence.

## Matrix status

**Complete for currently shipped Lab-covered capabilities** at the contract-path depth ADR-0028 / #96 require: every row below has non-ignored `rqg-integration` evidence. Where the Lab Scenario adds real-engine or OS-parallel surface that CI cannot host, Notes mark that remainder as Lab-manual (still not a completeness gap for the gate).

When shipping a new first-class capability: add a Lab Scenario (ADR-0025), add a non-ignored contract-path CI twin, update this matrix, and only then restate completeness for the expanded shipped surface.

## Shipped Lab capabilities ↔ non-ignored CI twin evidence

| Capability (Lab COVERAGE row) | Lab Scenario id(s) | Non-ignored CI twin evidence (`crates/app/tests/…`) | Notes |
| --- | --- | --- | --- |
| Direct Pipeline Initial Load + insert/update/delete | `direct-pipeline` | `cli_direct_pipeline_initial_load.rs`, `cli_direct_pipeline_delivery.rs`, `cli_stub_incremental.rs`, `cli_logminer_incremental.rs`, `cli_contract_catalog_initial_load.rs` | Contract/stub + delivery outcomes |
| Multi-table Transform Pipeline (`groupBy` / `sum`) | `transform-pipeline` | `cli_groupby_sum_affect.rs`, `cli_multi_table_incremental.rs` | Affect Analysis + multi-table Direct/Transform settle |
| Rich Transform `project` | `rt-project` | `cli_transform_pipeline.rs` (`project` paths) | |
| Rich Transform `filter` | `rt-filter` | `cli_transform_pipeline.rs` (`filter` paths) | |
| Rich Transform `groupBy` / `sum` (also under contention) | `transform-pipeline`, `concurrent-source-workload` | `cli_groupby_sum_affect.rs`, `cli_multi_table_incremental.rs` | CI: multi-table settle after both Bases change. OS-parallel Source sessions stay Lab-manual |
| Intra-Scenario concurrent Source workload | `concurrent-source-workload` | `cli_multi_table_incremental.rs` | CI twin = same multi-table Pipeline shape settling after Incremental on both tables (correctness where sensible). Parallel sqlplus / contention timing stay Lab-manual |
| Bulk load (~100k) with fail-able metric thresholds | `bulk-load` | `cli_direct_pipeline_initial_load.rs`, `cli_contract_catalog_initial_load.rs`, `cli_direct_pipeline_delivery.rs` | CI twin = Initial Load + Delivery correctness on contract/stub. ~100k volume and lag/throughput/duration **thresholds stay Lab-manual** (ADR-0025 / ADR-0028)—never RQG evidence |
| Idempotent re-delivery / duplicate-safe Delivery | `idempotent-redelivery` | `cli_idempotent_redelivery.rs` (also overlap absorb: `cli_cutover_no_gap.rs`; Managed-only upsert: `cli_direct_pipeline_delivery.rs`, `cli_stub_incremental.rs`) | Force re-Delivery via Platform Store Delivery status + product `apply` |
| Dedicated Pipeline pause/resume CLI verbs | `pause-resume` | `cli_pause_resume_pipeline.rs` | Pause stops Delivery for one Pipeline; resume catch-up from durable Base; other Pipelines unaffected |
| Dedicated Pipeline remove CLI verb | `remove-pipeline` | `cli_remove_pipeline.rs` | Remove ceases Delivery; Shared Base kept when still referenced; status no longer lists Pipeline; Deployment remains |
| Pipeline revision change via `apply` (Derived rebuild / metadata-only skip) | `change-pipeline` | `cli_change_pipeline_revision.rs` | Semantic transform/binding change pauses old Delivery, rebuilds that Pipeline's Derived, re-Delivers with delete reconciliation, resumes incremental; Shared Bases not rebuilt; metadata-only `description` skips rebuild |
| Poison Change quarantine on Operator `status` | `poison-quarantine` | `cli_poison_quarantine.rs` | Bounded Delivery retries → quarantine + alert; Pipeline continues other identities; `status` Delivery Health unhealthy / not aligned for quarantined keys |
| Blocking DDL Schema Change warn+pause | `schema-change-pause` | `cli_schema_change_pause.rs` | Blocking DDL → WARN + pause affected Pipeline; unaffecting ADD continues Delivery; distinct from poison quarantine (`status` paused + Schema Change, not quarantine unhealthy) |
| Source Alignment Check for Base Datasets | `source-alignment` | `cli_source_alignment.rs` | Detect Base≠Source; repair Base from Source reads (never write Source); resource-gated `--max-rows` (default 1000); `status` shows Source Alignment |
| Drift Check with Managed-field auto-repair | `drift-check` | `cli_drift_check.rs` | Detect Managed-field Target drift; default auto-repair via Managed upsert; non-Managed fields preserved; resource-gated `--max-rows` (default 1000); requires Source Alignment baseline for Direct; `status` shows Drift |
| Bounded backpressure with visible lag | `bounded-backpressure` | `cli_bounded_backpressure.rs` | Downstream Delivery delay + tiny queue capacity → Backpressure signal; queue_depth ≤ capacity; Sync/Delivery Health lag > 0 mid-sync; Pipeline not paused; catch-up lag → 0 |
| Observability Surface (logs, health, Prometheus) | `observability-surface` | `cli_observability_surface.rs` | Structured JSON operator events on sync; `status` Sync/Delivery Health + Pipeline; `run --metrics-addr` Prometheus `/metrics` exposes lag + alertable failure counters |
| Platform Store Guardrails and warn-only disk thresholds | `platform-store-guardrails` | `cli_platform_store_guardrails.rs` | Absurdly low store settings rejected on migrate; free-disk warn on `status` / metrics (`platform_store_disk_warn`) while store stays healthy; Pipeline not auto-paused for disk pressure |
| Backward-compatible upgrades / Platform Store migrations | `backward-compatible-upgrades` | `cli_backward_compatible_upgrades.rs` | Prior-schema `migrate` preserves Deployment/Base; SemVer-compatible older `apiVersion` (`migraloop.dev/v1.0.0`) applies without Initial Load; upgrade smoke without wipe-rebuild. Lab Scenario covers operator migrate + older-config re-apply on a live Fixture (prior-schema cut is CI twin) |

## Explicitly not gate evidence

Do **not** cite these as Release Quality Gate / CI twin proof (they stay `#[ignore]` and out of `rqg-integration` execution):

- Full Lab Scenario runs (`lab_scenario_*_run_and_inspect` in `cli_lab_scenario.rs`)
- Lab Fixture lifecycle (`cli_lab_fixture.rs` ignored paths)
- Live Oracle Instant Client paths (`cli_live_oracle_direct.rs`)

Lab catalog control-plane seams that are always-on (list/help/isolation / outcome probes) may appear in `rqg-integration` but are not substitutes for capability behavior twins above.

Performance regression thresholds are owned by `rqg-perf` (separate job; see parent #94 / #97 and `ci/rqg/`)—not this matrix. Cutover, restart-resume, and other ADR-0011 slices stay covered by existing non-ignored app tests outside this Lab-catalog twin table.
