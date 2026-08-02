//! Black-box tests for the Handbook guard locale-parity check.
//!
//! Agreed seam (issue #49): invoke the single guard entrypoint against fixture
//! handbook trees. No product runtime / Oracle / Mongo required.

use std::path::{Path, PathBuf};
use std::process::Command;

fn bin() -> String {
    env!("CARGO_BIN_EXE_handbook-guard").to_string()
}

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(name)
}

fn run_check(handbook: &Path) -> std::process::Output {
    Command::new(bin())
        .args(["check", "--handbook"])
        .arg(handbook)
        .output()
        .expect("run handbook-guard")
}

#[test]
fn isomorphic_locale_trees_pass() {
    let output = run_check(&fixture("isomorphic"));
    assert!(
        output.status.success(),
        "expected success on isomorphic tree:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn divergent_locale_page_sets_fail() {
    let output = run_check(&fixture("missing-page"));
    assert!(
        !output.status.success(),
        "expected failure when a locale is missing a page:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        combined.contains("missing deployment.md"),
        "expected a page-set divergence reason naming deployment.md, got:\n{combined}"
    );
}
