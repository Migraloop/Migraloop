#!/usr/bin/env bash
# rqg-perf: Transform Pipeline contract/stub microbench with retries on failure.
# Covers Affect Analysis → Derived → Delivery (issue #231 / ADR-0029).
# Lab Scenario bulk-load is never invoked (ADR-0025 / ADR-0028 / issue #97).
#
# Retries absorb ubuntu-latest noisy-neighbor spikes (same policy as Direct).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

# Prefer Transform-specific override; do not inherit Direct's MIGRALOOP_RQG_PERF_BASELINE.
export MIGRALOOP_RQG_PERF_BASELINE="${MIGRALOOP_RQG_PERF_TRANSFORM_BASELINE:-$ROOT/ci/rqg/transform_pipeline_microbench_baseline.json}"
MAX_ATTEMPTS="${MIGRALOOP_RQG_PERF_ATTEMPTS:-3}"

run_once() {
  cargo test -p migraloop-app --test rqg_perf_transform_pipeline \
    transform_pipeline_microbench_meets_committed_baseline \
    -- --ignored --nocapture
}

attempt=1
while [ "$attempt" -le "$MAX_ATTEMPTS" ]; do
  echo "rqg-perf transform: attempt ${attempt}/${MAX_ATTEMPTS} (baseline=$MIGRALOOP_RQG_PERF_BASELINE)"
  if run_once; then
    if [ "$attempt" -eq 1 ]; then
      echo "rqg-perf transform: pass on attempt 1"
    else
      echo "rqg-perf transform: pass on attempt ${attempt} (retry)"
    fi
    exit 0
  fi
  if [ "$attempt" -lt "$MAX_ATTEMPTS" ]; then
    echo "rqg-perf transform: attempt ${attempt} failed; retrying"
  fi
  attempt=$((attempt + 1))
done

echo "rqg-perf transform: failed after ${MAX_ATTEMPTS} attempts"
exit 1
