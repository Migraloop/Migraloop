# Live catalog inventory (Lab Fixture)

**Issue:** [#222](https://github.com/Migraloop/Migraloop/issues/222) (child of epic [#221](https://github.com/Migraloop/Migraloop/issues/221))  
**Fixture:** Local Sync Lab (`migraloop lab up` / `lab status` → ready: platform-store, oracle, mongo, app)  
**Method:** `migraloop lab scenario run <id> --auto-remove` against live Base/Target (host CLI + Instant Client; Fixture `app` paused for exclusive host apply/sync). Not contract/stub alone.  
**Catalog:** selectable ids from `migraloop lab scenario list` / [`COVERAGE.md`](./COVERAGE.md) (25 shipped Scenarios).

**Tally:** **25 PASS / 0 FAIL**

**Basic-complete go/no-go (epic #221):** **go** — every shipped Scenario id in `COVERAGE.md` is live-green on the Lab Scenario seam (`lab scenario run` / real product apply+sync). Lab pauses Fixture continuous `run` only for the duration of each Scenario so host Sync is the sole Incremental Capture consumer (then resumes `app`); this is Fixture coordination, not a stub Sync/Delivery path. Direct+Transform performance work and Rich Transform DX (epic post-gate children) may open without re-litigating catalog completeness.

## Change Ordering / confluence ([#225](https://github.com/Migraloop/Migraloop/issues/225))

Ran new Scenario `change-ordering` on a ready Fixture (host Instant Client + product `apply`/`sync`; no Lab-only shortcuts; no Source Alignment Check / Drift Check on the path). **PASS** — same-key A→B→C finals (`NameC`), serial cross-key interleave finals (`Key2Final`), min extreme delete → eventual `MIN_AMOUNT=20` (stale `10` gone) via Incremental Affect / Base recompute. Matching RQG twin: `cli_change_ordering`. True OS-parallel cross-key contention remains covered by already-green `concurrent-source-workload`.

| Scenario id | Result | Notes |
|-------------|--------|-------|
| `change-ordering` | PASS | ADR-0029; LogMiner OCI; capture-order finals for same-key + interleave + min recompute |

## Direct Sync cluster re-verify ([#223](https://github.com/Migraloop/Migraloop/issues/223))

Re-ran the Direct Sync Lab Scenario cluster on a fresh ready Fixture (host Instant Client + product `apply`/`sync`; no Lab-only shortcuts). All four acceptance Scenarios **PASS**; no additional product-path fixes required beyond the inventory fixes already on `main`. Relevant Direct Sync RQG contract twins stay green (`cli_direct_pipeline_*`, `cli_idempotent_redelivery`, `cli_initial_load_chunked`, `cli_stub_incremental`, `cli_logminer_incremental`, `cli_contract_catalog_initial_load`, `cli_cutover_no_gap`).

| Scenario id | Result | Notes |
|-------------|--------|-------|
| `direct-pipeline` | PASS | Initial Load → mutate → LogMiner Incremental + Delivery |
| `idempotent-redelivery` | PASS | Duplicate-safe re-Delivery; non-Managed Target field preserved |
| `initial-load-throttled` | PASS | Chunked progress, pause/resume, rate_limit, backoff, watermark retained |
| `bulk-load` | PASS | 100000 rows; lag=0; thresholds pass (`duration_ms` ≪ `max_duration_ms`, `rows_per_s` ≫ `min_rows_per_s`) |

## Transform Sync cluster re-verify ([#224](https://github.com/Migraloop/Migraloop/issues/224))

Re-ran the Transform Sync Lab Scenario cluster on a ready Fixture (host Instant Client + product `apply`/`sync`; no Lab-only shortcuts). All nine acceptance Scenarios **PASS**; no additional product-path fixes required beyond the inventory fixes already on `main`. Relevant Transform Sync RQG contract twins stay green (`cli_groupby_sum_affect`, `cli_groupby_rich_aggs_affect`, `cli_multi_table_incremental`, `cli_transform_pipeline`, `cli_transform_field_ops`, `cli_equilookup_affect`, `cli_union_affect`, `cli_unwind_affect`, `cli_distinct_addtoset_affect`).

| Scenario id | Result | Notes |
|-------------|--------|-------|
| `transform-pipeline` | PASS | Multi-table Direct + `groupBy` sum/count/min/max/avg → Derived → Delivery |
| `rt-project` | PASS | |
| `rt-filter` | PASS | |
| `rt-field-ops` | PASS | |
| `rt-equilookup` | PASS | |
| `rt-union` | PASS | |
| `rt-unwind` | PASS | |
| `rt-distinct-addtoset` | PASS | |
| `concurrent-source-workload` | PASS | Parallel sqlplus settle; `settle_ms=744` ≪ `max_settle_ms=300000` |

## Matrix

| Scenario id | Result | Notes |
|-------------|--------|-------|
| `direct-pipeline` | PASS | |
| `transform-pipeline` | PASS | Requires `avg` → Decimal128 schema inference. |
| `rt-project` | PASS | |
| `rt-filter` | PASS | |
| `rt-field-ops` | PASS | |
| `rt-equilookup` | PASS | |
| `rt-union` | PASS | |
| `rt-unwind` | PASS | |
| `rt-distinct-addtoset` | PASS | Requires scale-preserving NUMBER JSON; host exclusive Sync (see below). |
| `concurrent-source-workload` | PASS | |
| `change-ordering` | PASS | Same-key order + cross-key interleave + min Base recompute (ADR-0029 / #225). |
| `bulk-load` | PASS | ~100k rows; thresholds met with exclusive host Initial Load (no Fixture `run` race). |
| `idempotent-redelivery` | PASS | |
| `pause-resume` | PASS | |
| `remove-pipeline` | PASS | |
| `change-pipeline` | PASS | |
| `poison-quarantine` | PASS | |
| `schema-change-pause` | PASS | Distinct from poison quarantine; quarantine rows cascade on Deployment delete. |
| `source-alignment` | PASS | |
| `drift-check` | PASS | |
| `bounded-backpressure` | PASS | |
| `observability-surface` | PASS | |
| `platform-store-guardrails` | PASS | |
| `backward-compatible-upgrades` | PASS | |
| `initial-load-throttled` | PASS | Empty Incremental windows no longer rewrite Base mid–Initial Load. |

## Product fixes required for live-green

1. **Incremental Capture / LogMiner (Oracle 23)** — do not wrap `DBMS_LOGMNR.MINE_VALUE` in `FETCH FIRST` / nested `ROWNUM`; apply backpressure limits after OCI fetch (`crates/capture`).
2. **Lab app Instant Client** — Basic Light in `lab/Dockerfile` so Fixture `migraloop run` can open LogMiner OCI.
3. **`groupBy` `avg` schema** — `NUMBER(34,10)` → Decimal128 (`crates/transform`).
4. **NUMBER scale in Capture JSON** — scale>0 emits fixed-scale decimal strings (`crates/capture`).
5. **Poison quarantine cleanup** — `delete_deployment` + migration `0022` CASCADE so Namespace wipe leaves no orphan quarantine rows.
6. **Empty Incremental window** — persist Sync metadata without rewriting `base_rows` (avoids racing Initial Load).
7. **Scenario exclusive Sync** — `lab scenario run` pauses Fixture `app` so host apply/sync is the sole Incremental Capture consumer; Scenario readiness does not require `app` healthy.
8. **Lab inspect hooks** — Mongo `$numberDecimal`, Delivery name boundary match, distinct/addToSet amount shapes.

## Operator notes

- Prefer `--auto-remove` on PASS. After FAIL, always `migraloop lab scenario remove <id>` (auto-remove is PASS-only).
- After host binary/schema changes, rebuild Fixture app (`docker compose -f lab/compose.yaml -p migraloop-lab build app && up -d --force-recreate app`) so continuous `run` matches host migrations.
- Host Scenario apply/sync needs Instant Client on `LD_LIBRARY_PATH` (handbook local setup / deployment).
