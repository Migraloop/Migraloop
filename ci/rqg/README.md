# Release Quality Gate — performance microbench

Part of ADR-0011 / ADR-0028 (PRD #30). `rqg-perf` runs a fixed Direct Pipeline
microbench on the **contract/stub** path (seed N → Initial Load → Incremental →
Delivery) and compares wall-clock duration and throughput to the committed
baseline below.

| Artifact | Purpose |
| --- | --- |
| `direct_pipeline_microbench_baseline.json` | Committed `seed_rows`, `duration_ms`, `rows_per_s`, `allowed_regression_pct` (~55 for GHA noise) |
| `run_direct_pipeline_microbench.sh` | CI entry: runs the ignored microbench test; **up to 3 attempts** on failure |

Lab Scenario `bulk-load` (~100k + metric thresholds) stays **manual** and is never
invoked by this job (ADR-0025 / ADR-0028).

## Updating the baseline

1. On a representative `ubuntu-latest`-class machine with Platform Store + Mongo
   matching CI env defaults, run:

   ```bash
   cargo test -p migraloop-app --test rqg_perf_direct_pipeline \
     direct_pipeline_microbench_meets_committed_baseline \
     -- --ignored --nocapture
   ```

2. Copy the printed timed-run `duration_ms` / `rows_per_s` (the `seed_rows=N`
   line after warmup) into `direct_pipeline_microbench_baseline.json`. Prefer
   a representative (not best-case) `ubuntu-latest` measurement.

3. Keep `allowed_regression_pct` at ~55 unless intentionally retuning the gate.
   Hosted runners spike well beyond 20% (merge of #102: 4309–5137ms vs a
   2802ms green PR run on the same docs-only tree). The runner script also
   retries up to 3 attempts so a single noisy neighbor does not red `main`.
