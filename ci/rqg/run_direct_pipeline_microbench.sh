#!/usr/bin/env bash
# rqg-perf: Direct Pipeline contract/stub microbench with one retry on failure.
# Lab Scenario bulk-load is never invoked (ADR-0025 / ADR-0028 / issue #97).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

export MIGRALOOP_RQG_PERF_BASELINE="${MIGRALOOP_RQG_PERF_BASELINE:-$ROOT/ci/rqg/direct_pipeline_microbench_baseline.json}"

run_once() {
  cargo test -p migraloop-app --test rqg_perf_direct_pipeline \
    direct_pipeline_microbench_meets_committed_baseline \
    -- --ignored --nocapture
}

echo "rqg-perf: attempt 1/2 (baseline=$MIGRALOOP_RQG_PERF_BASELINE)"
if run_once; then
  echo "rqg-perf: pass on attempt 1"
  exit 0
fi

echo "rqg-perf: attempt 1 failed; retrying once"
echo "rqg-perf: attempt 2/2"
if run_once; then
  echo "rqg-perf: pass on attempt 2 (retry)"
  exit 0
fi

echo "rqg-perf: failed after one retry"
exit 1
