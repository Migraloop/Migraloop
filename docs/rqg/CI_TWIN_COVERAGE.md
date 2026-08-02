# CI twin coverage matrix (Lab ↔ Release Quality Gate)

Policy: ADR-0028 / issue #96 (parent #94). Analogous in spirit to Lab [`lab/scenarios/COVERAGE.md`](../../lab/scenarios/COVERAGE.md)—not a Lab Scenario runner.

Rows are **currently shipped** Lab catalog capabilities. Evidence cells must point at **non-ignored** `migraloop-app` contract-path tests (or equivalent always-on CI evidence) that `rqg-integration` runs via `cargo test -p migraloop-app`. Ignored Lab Fixture / Lab Scenario / live Oracle tests are **not** gate evidence.

## Matrix status

**Complete for currently shipped Lab-covered capabilities.**

When shipping a new first-class capability: add a Lab Scenario (ADR-0025), add a non-ignored contract-path CI twin, update this matrix, and only then restate completeness for the expanded shipped surface.

## Shipped Lab capabilities ↔ non-ignored CI twin evidence

| Capability (Lab COVERAGE row) | Lab Scenario id(s) | Non-ignored CI twin evidence (`crates/app/tests/…`) | Notes |
| --- | --- | --- | --- |
| Direct Pipeline Initial Load + insert/update/delete | `direct-pipeline` | `cli_direct_pipeline_initial_load.rs`, `cli_direct_pipeline_delivery.rs`, `cli_stub_incremental.rs`, `cli_logminer_incremental.rs`, `cli_contract_catalog_initial_load.rs` | Contract/stub + delivery outcomes |
| Multi-table Transform Pipeline (`groupBy` / `sum`) | `transform-pipeline` | `cli_groupby_sum_affect.rs`, `cli_multi_table_incremental.rs` | Affect Analysis + multi-table Direct/Transform settle |
| Rich Transform `project` | `rt-project` | `cli_transform_pipeline.rs` (`project` paths) | |
| Rich Transform `filter` | `rt-filter` | `cli_transform_pipeline.rs` (`filter` paths) | |
| Rich Transform `groupBy` / `sum` (also under contention) | `transform-pipeline`, `concurrent-source-workload` | `cli_groupby_sum_affect.rs`, `cli_multi_table_incremental.rs` | Contention OS parallelism stays Lab; CI covers multi-table settle correctness |
| Intra-Scenario concurrent Source workload | `concurrent-source-workload` | `cli_multi_table_incremental.rs` | Contract-path correctness where sensible (multi-table Incremental settle); parallel sqlplus sessions remain Lab-manual |
| Bulk load (~100k) with fail-able metric thresholds | `bulk-load` | Correctness: `cli_direct_pipeline_initial_load.rs`, `cli_contract_catalog_initial_load.rs`, `cli_direct_pipeline_delivery.rs`. Control-plane probes: `cli_lab_scenario.rs` (`lab_scenario_bulk_load_*_via_cli_probe`) | Large-volume / lag / throughput / duration **thresholds stay Lab-manual** (ADR-0025 / ADR-0028)—not RQG evidence |
| Idempotent re-delivery / duplicate-safe Delivery | `idempotent-redelivery` | `cli_idempotent_redelivery.rs` (also overlap absorb: `cli_cutover_no_gap.rs`; Managed-only upsert: `cli_direct_pipeline_delivery.rs`, `cli_stub_incremental.rs`) | Force re-Delivery via Platform Store Delivery status + product `apply` |

## Explicitly not gate evidence

Do **not** cite these as Release Quality Gate / CI twin proof (they stay `#[ignore]` and out of `rqg-integration` execution):

- Full Lab Scenario runs (`lab_scenario_*_run_and_inspect` in `cli_lab_scenario.rs`)
- Lab Fixture lifecycle (`cli_lab_fixture.rs` ignored paths)
- Live Oracle Instant Client paths (`cli_live_oracle_direct.rs`)

Lab catalog control-plane seams that are always-on (list/help/isolation / outcome probes) may appear in `rqg-integration` but are not substitutes for capability behavior twins above.

## Related RQG slices (not Lab COVERAGE rows)

These are required by ADR-0011 / ADR-0028 and already covered by non-ignored app tests; they are listed here so gaps are not confused with Lab catalog rows:

| RQG slice | Non-ignored evidence |
| --- | --- |
| Cutover / Initial↔Incremental hand-off | `cli_cutover_no_gap.rs` |
| Restart-resume / basic fault | `cli_restart_resume.rs` |
| Oracle→Mongo contract/stub harness | `cli_stub_incremental.rs`, `cli_logminer_incremental.rs`, `cli_contract_catalog_initial_load.rs`, `cli_source_prerequisites.rs` |

Performance regression thresholds are owned by `rqg-perf` (separate job; see parent #94 / #97)—not this matrix.
