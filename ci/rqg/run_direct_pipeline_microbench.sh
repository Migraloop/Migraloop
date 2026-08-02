#!/usr/bin/env bash
# rqg-perf: Direct Pipeline contract/stub microbench with retries on failure.
# Lab Scenario bulk-load is never invoked (ADR-0025 / ADR-0028 / issue #97).
#
# Retries absorb ubuntu-latest noisy-neighbor spikes (merge of docs-only #102
# failed on main at 5137ms / 4309ms against a 4080ms ceiling while the PR run
# at 2802ms was green).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

export MIGRALOOP_RQG_PERF_BASELINE="${MIGRALOOP_RQG_PERF_BASELINE:-$ROOT/ci/rqg/direct_pipeline_microbench_baseline.json}"
MAX_ATTEMPTS="${MIGRALOOP_RQG_PERF_ATTEMPTS:-3}"

run_once() {
  cargo test -p migraloop-app --test rqg_perf_direct_pipeline \
    direct_pipeline_microbench_meets_committed_baseline \
    -- --ignored --nocapture
}

attempt=1
while [ "$attempt" -le "$MAX_ATTEMPTS" ]; do
  echo "rqg-perf: attempt ${attempt}/${MAX_ATTEMPTS} (baseline=$MIGRALOOP_RQG_PERF_BASELINE)"
  if run_once; then
    if [ "$attempt" -eq 1 ]; then
      echo "rqg-perf: pass on attempt 1"
    else
      echo "rqg-perf: pass on attempt ${attempt} (retry)"
    fi
    exit 0
  fi
  if [ "$attempt" -lt "$MAX_ATTEMPTS" ]; then
    echo "rqg-perf: attempt ${attempt} failed; retrying"
  fi
  attempt=$((attempt + 1))
done

echo "rqg-perf: failed after ${MAX_ATTEMPTS} attempts"
exit 1
