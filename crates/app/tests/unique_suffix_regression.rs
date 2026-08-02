//! Regression: ephemeral DB / Mongo names must stay unique under parallel cargo tests.
//!
//! Merge of PR #99 failed on main with:
//! `duplicate key value violates unique constraint "pg_database_datname_index"`
//! for `migraloop_test_<nanos>` while the PR checks were green — classic
//! nanos-only race across parallel tests.

mod common;

use std::collections::HashSet;
use std::thread;

#[test]
fn unique_suffix_no_collisions_across_parallel_threads() {
    let mut handles = Vec::new();
    for _ in 0..32 {
        handles.push(thread::spawn(|| {
            (0..128).map(|_| common::unique_suffix()).collect::<Vec<_>>()
        }));
    }
    let mut seen = HashSet::new();
    for handle in handles {
        for suffix in handle.join().expect("worker") {
            assert!(
                seen.insert(suffix.clone()),
                "duplicate unique_suffix: {suffix}"
            );
        }
    }
    assert_eq!(seen.len(), 32 * 128);
}

#[test]
fn old_nanos_only_db_name_formula_is_not_unique_for_identical_ticks() {
    // Exact CI collision shape from merge commit 3c2041d / run 30748407623.
    let tick = 1_785_674_699_524_920_225u128;
    let a = format!("migraloop_test_{tick}");
    let b = format!("migraloop_test_{tick}");
    assert_eq!(a, b);
    // Fixed helper must differ even if wall clock matches.
    let fixed_a = format!("migraloop_test_{}", common::unique_suffix());
    let fixed_b = format!("migraloop_test_{}", common::unique_suffix());
    assert_ne!(fixed_a, fixed_b);
}
