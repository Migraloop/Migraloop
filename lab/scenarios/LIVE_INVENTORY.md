# Live catalog inventory (Lab Fixture)

**Issue:** [#222](https://github.com/Migraloop/Migraloop/issues/222) (child of epic [#221](https://github.com/Migraloop/Migraloop/issues/221))  
**Fixture:** Local Sync Lab (`migraloop lab up` / `lab status` → ready: platform-store, oracle, mongo, app)  
**Method:** `migraloop lab scenario run <id> --auto-remove` against live Base/Target (host CLI + Instant Client; Fixture `app` paused for exclusive host apply/sync). Not contract/stub alone.  
**Catalog:** selectable ids from `migraloop lab scenario list` / [`COVERAGE.md`](./COVERAGE.md) (24 shipped Scenarios).

**Tally:** **24 PASS / 0 FAIL**

**Basic-complete go/no-go (epic #221):** **go** — every shipped Scenario id in `COVERAGE.md` is live-green on the Lab Scenario seam (`lab scenario run` / real product apply+sync). Lab pauses Fixture continuous `run` only for the duration of each Scenario so host Sync is the sole Incremental Capture consumer (then resumes `app`); this is Fixture coordination, not a stub Sync/Delivery path. Direct+Transform performance work and Rich Transform DX (epic post-gate children) may open without re-litigating catalog completeness.

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
