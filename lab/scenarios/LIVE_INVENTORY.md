# Lab Scenario live inventory (Oracle → Mongo)

Evidence for GitHub issues #221 / #222. Primary verification seam:
`migraloop lab scenario run <id>` on a real Local Sync Lab Fixture (Oracle Free 23 +
MongoDB 7 + Platform Store + app). No Lab-only fake Sync/Delivery shortcuts.

## Fixture

| Item | Value |
| --- | --- |
| Date (UTC) | 2026-08-06 |
| Host | Cursor Cloud agent + nested DinD (`fuse-overlayfs`) |
| Fixture | `migraloop lab up` → `lab status` ready (platform-store, oracle, mongo, app) |
| Product binary | `target/debug/migraloop` (branch work for #221) |
| Host Instant Client | Basic Light 23.9 (`LD_LIBRARY_PATH=/opt/oracle/instantclient_23_9`) |
| Fixture app Instant Client | Packaged in `lab/Dockerfile` (Basic Light) so `migraloop run` can open LogMiner OCI |
| Product path | recipe `workload.product_path` → real `migraloop apply` / `migraloop sync` |

## Catalog results

Shipped Scenario ids from [COVERAGE.md](./COVERAGE.md). Status after product fixes landed in this PR
(LogMiner ORA-01323, Lab Instant Client, `avg` Decimal128 schema, NUMBER scale strings,
Lab hook boundaries). Re-verified on a clean Fixture with explicit
`lab scenario remove` after reds (see Operator notes).

| Scenario id | Result | Duration (approx) | Notes |
| --- | --- | --- | --- |
| `direct-pipeline` | **PASS** | ~3s | |
| `transform-pipeline` | **PASS** | ~3s | Was red: `avg` → Long Delivery; fixed via Decimal128 schema + inspect |
| `rt-project` | **PASS** | ~3s | |
| `rt-filter` | **PASS** | ~3s | |
| `rt-field-ops` | **PASS** | ~4s | |
| `rt-equilookup` | **PASS** | ~3s | |
| `rt-union` | **PASS** | ~4s | |
| `rt-unwind` | **PASS** | ~4s | |
| `rt-distinct-addtoset` | **PASS** | ~2s | Was red: NUMBER(12,2) collapsed to ints; scale-preserving strings fixed |
| `concurrent-source-workload` | **PASS** | ~5s | |
| `bulk-load` | **FAIL** | ~608s | Expected 100k Base/Target; observed `base_rows=89000` `target_rows=91000` inside `max_duration_ms=600000` (`rows_per_s≈165`). Follow-up: #223 |
| `idempotent-redelivery` | **PASS** | ~12s | |
| `pause-resume` | **PASS** | ~16s | |
| `remove-pipeline` | **PASS** | ~3s | Was false-red: `lab-rp-customers` prefix matched `-reporting` |
| `change-pipeline` | **PASS** | ~20s | |
| `poison-quarantine` | **PASS** | ~15s | |
| `schema-change-pause` | **PASS** | ~3s | First FAIL was leftover Deployments polluting one-shot `sync` |
| `source-alignment` | **PASS** | ~3s | |
| `drift-check` | **PASS** | ~4s | |
| `bounded-backpressure` | **PASS** | ~13s | |
| `observability-surface` | **PASS** | ~4s | First FAIL was leftover bulk-load metrics lag=0 |
| `platform-store-guardrails` | **PASS** | ~2s | |
| `backward-compatible-upgrades` | **PASS** | ~2s | |
| `initial-load-throttled` | **PASS** | ~5s | |

**Live catalog tally:** 23 PASS / 1 FAIL (`bulk-load`).

## Product defects fixed while gathering inventory

1. **ORA-01323 on Incremental Capture** — Oracle 23 rejects `DBMS_LOGMNR.MINE_VALUE` with SQL `FETCH FIRST` / nested `ROWNUM`. Backpressure limits stop the OCI cursor in Rust (`crates/capture`).
2. **Lab Fixture `app` DPI-1047** — `lab/Dockerfile` installs Instant Client Basic Light for continuous `migraloop run`.
3. **`groupBy` `avg` Long mapping** — Derived schema inherited integer source scale; `infer_derived_columns` maps `avg` → Decimal128-safe NUMBER(34,10). Lab inspect accepts `$numberDecimal`.
4. **NUMBER(p,s>0) JSON collapse** — Initial Load / LogMiner text→JSON now keeps scale-preserving decimal strings (`10` → `"10.00"`).
5. **Lab hook pipeline-name prefix** — `remove-pipeline` / `pause-resume` Delivery assertions use token boundaries.
6. **Cloud DinD `/var/run` 0700** — `cloud-dind-start.sh` chmods `/var/run` for non-root `docker.sock` access.

## Operator notes for re-runs

- `--auto-remove` only cleans Namespace on **PASS** (US35 keeps failures for inspect). Multi-Scenario inventory must `lab scenario remove <id>` after reds or leftovers share one-shot `migraloop sync`.
- Host Scenario CLI still requires Instant Client on the Developer machine (`LD_LIBRARY_PATH`).

## Go/no-go (parent #221)

**Not yet go.** One shipped Scenario remains live-red: `bulk-load` (#223). Child tickets #223–#228 / #229 stay open until that settles and any residual correctness/ops gaps are cleared.
