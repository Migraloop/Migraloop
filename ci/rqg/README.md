# Release Quality Gate — performance microbench

Part of ADR-0011 / ADR-0028 (PRD #30). `rqg-perf` runs fixed Direct **and**
Transform Pipeline microbenches on the **contract/stub** path (seed N → Initial
Load → Incremental → Delivery) and compares wall-clock duration and throughput
to the committed baselines below. Transform covers Affect Analysis → Derived →
Delivery (ADR-0029 — Transform is a first-class perf target equal to Direct).

| Artifact | Purpose |
| --- | --- |
| `direct_pipeline_microbench_baseline.json` | Direct: committed `seed_rows`, `duration_ms`, `rows_per_s`, `allowed_regression_pct` (~55 for GHA noise) |
| `run_direct_pipeline_microbench.sh` | CI entry: ignored Direct microbench; **up to 3 attempts** on failure |
| `transform_pipeline_microbench_baseline.json` | Transform (`project`+`filter` on CUSTOMERS): same shape |
| `run_transform_pipeline_microbench.sh` | CI entry: ignored Transform microbench; **up to 3 attempts** on failure |

Lab Scenario `bulk-load` (~100k + metric thresholds) stays **manual** and is never
invoked by this job (ADR-0025 / ADR-0028).

## Issue #230 evidence (Direct Pipeline throughput)

Post basic-complete go (#229), Direct Sync/IL hot-path work landed:

- Platform Store per-identity Base persist (`record_sync_row_progress`) — Incremental
  Capture no longer DELETE+rewrites every `base_rows` peer per change
- Bulk `UNNEST` insert for full Base snapshot / Initial Load chunks
- Mongo Delivery: process-wide client reuse + batched `update`/`delete` commands
  (MongoDB 7–compatible; not the 8.0+ `bulkWrite` API)

Committed baseline moved from `duration_ms=3400` / `rows_per_s=290` to
`duration_ms=800` / `rows_per_s=1200` (same `seed_rows=1000`, same ~55%
regression band). Local contract/stub timed runs after the change clustered near
~440–490ms / ~2000–2300 rows/s; the committed numbers stay intentionally
conservative for hosted-runner noise.

## Issue #231 evidence (Transform Pipeline throughput)

Post basic-complete go (#229), Transform Sync/IL hot-path work landed:

- Platform Store per-identity Derived persist (`apply_derived_identity_changes`) —
  Incremental Affect recompute no longer DELETE+rewrites every `derived_rows` peer
- Bulk `UNNEST` insert for full Derived snapshot (Initial Load materialize)
- Skip redundant primary Base reload for columns on the Incremental Transform path
- Row-grain identity recompute prefilters primary contributors when Affect
  identities still share Base field names (rename-only identities fall back safely)

Pre-change local contract/stub timed run (`seed_rows=1000`, same Transform
`project`+`filter` Scenario shape): `duration_ms=848` / `rows_per_s≈1179`.
Post-change local runs clustered near `duration_ms≈465` / `rows_per_s≈2150`.
Committed Transform baseline: `duration_ms=550` / `rows_per_s=1700` (same ~55%
regression band) — intentionally between local post-change and pre-change so a
revert of the Derived persist path tends to red `rqg-perf` while hosted-runner
noise still has headroom.

## Updating a baseline

1. On a representative `ubuntu-latest`-class machine with Platform Store + Mongo
   matching CI env defaults, run the matching script or test:

   ```bash
   bash ci/rqg/run_direct_pipeline_microbench.sh
   bash ci/rqg/run_transform_pipeline_microbench.sh
   # or:
   cargo test -p migraloop-app --test rqg_perf_direct_pipeline \
     direct_pipeline_microbench_meets_committed_baseline \
     -- --ignored --nocapture
   cargo test -p migraloop-app --test rqg_perf_transform_pipeline \
     transform_pipeline_microbench_meets_committed_baseline \
     -- --ignored --nocapture
   ```

2. Copy the printed timed-run `duration_ms` / `rows_per_s` (the `seed_rows=N`
   line after warmup) into the matching `*_microbench_baseline.json`. Prefer
   a representative (not best-case) `ubuntu-latest` measurement.

3. Keep `allowed_regression_pct` at ~55 unless intentionally retuning the gate.
   Hosted runners spike well beyond 20% (merge of #102: 4309–5137ms vs a
   2802ms green PR run on the same docs-only tree). The runner scripts also
   retry up to 3 attempts so a single noisy neighbor does not red `main`.
