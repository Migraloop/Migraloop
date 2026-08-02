# Release Quality Gate — performance microbench

`rqg-perf` runs a fixed Direct Pipeline microbench on the **contract/stub** path
(seed N → Initial Load → Incremental → Delivery) and compares wall-clock duration
and throughput to the committed baseline below.

| Artifact | Purpose |
| --- | --- |
| `direct_pipeline_microbench_baseline.json` | Committed `seed_rows`, `duration_ms`, `rows_per_s`, `allowed_regression_pct` (~20) |
| `run_direct_pipeline_microbench.sh` | CI entry: runs the ignored microbench test; **one retry** on failure |

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

2. Copy the printed `duration_ms` / `rows_per_s` into
   `direct_pipeline_microbench_baseline.json` (prefer a slightly conservative
   duration / throughput so ordinary runner noise stays inside the allowed %).

3. Keep `allowed_regression_pct` at ~20 unless intentionally retuning the gate.
