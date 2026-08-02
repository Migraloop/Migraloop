//! Operator-visible seam: Lab Scenario list / run (Direct Pipeline MVP).
//!
//! Agreed seam (issue #60 / PRD #55): CLI Lab Scenario commands. Always-on tests
//! cover catalog listing, help surface, and one-at-a-time rejection. Full Scenario
//! run against the Lab Fixture is ignored by default (Docker + Instant Client).

use std::fs;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn bin() -> String {
    env!("CARGO_BIN_EXE_migraloop").to_string()
}

fn lab_dir() -> String {
    let manifest = env!("CARGO_MANIFEST_DIR");
    format!("{manifest}/../../lab")
}

#[tokio::test]
async fn lab_help_lists_scenario() {
    let help = Command::new(bin())
        .args(["lab", "--help"])
        .output()
        .expect("run lab --help");
    assert!(
        help.status.success(),
        "lab --help failed: {}",
        String::from_utf8_lossy(&help.stderr)
    );
    let stdout = String::from_utf8_lossy(&help.stdout);
    assert!(
        stdout.contains("scenario"),
        "lab --help should list `scenario`, got:\n{stdout}"
    );
}

#[tokio::test]
async fn lab_scenario_list_includes_direct_pipeline() {
    let list = Command::new(bin())
        .args(["lab", "scenario", "list", "--lab-dir", &lab_dir()])
        .output()
        .expect("run lab scenario list");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&list.stdout),
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(list.status.success(), "lab scenario list failed:\n{out}");
    assert!(
        out.contains("Lab Scenarios:") || out.contains("direct-pipeline"),
        "expected catalog header / direct-pipeline, got:\n{out}"
    );
    assert!(
        out.contains("direct-pipeline"),
        "catalog must list direct-pipeline, got:\n{out}"
    );
}

#[tokio::test]
async fn lab_scenario_run_unknown_id_fails() {
    let run = Command::new(bin())
        .args([
            "lab",
            "scenario",
            "run",
            "not-a-real-scenario",
            "--lab-dir",
            &lab_dir(),
        ])
        .output()
        .expect("run unknown scenario");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(
        !run.status.success(),
        "unknown scenario should fail, got success:\n{out}"
    );
    assert!(
        out.to_ascii_lowercase().contains("unknown")
            || out.contains("not-a-real-scenario")
            || out.contains("Lab Scenario"),
        "expected clear unknown-scenario error, got:\n{out}"
    );
}

#[tokio::test]
async fn lab_scenario_run_rejects_when_another_is_active() {
    let lab = lab_dir();
    let lock_path = format!("{lab}/.migraloop-scenario.lock");
    let pid = std::process::id();
    let started = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    fs::write(
        &lock_path,
        format!(
            "{{\"scenario\":\"direct-pipeline\",\"pid\":{pid},\"started_at_unix\":{started}}}\n"
        ),
    )
    .expect("write scenario lock");

    let run = Command::new(bin())
        .args(["lab", "scenario", "run", "direct-pipeline", "--lab-dir", &lab])
        .output()
        .expect("run with active lock");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );

    let _ = fs::remove_file(&lock_path);

    assert!(
        !run.status.success(),
        "second Scenario run should be rejected while one is active, got:\n{out}"
    );
    assert!(
        out.contains("rejected") || out.to_ascii_lowercase().contains("active"),
        "expected one-at-a-time rejection message, got:\n{out}"
    );
    assert!(
        out.contains("direct-pipeline"),
        "rejection should name the active Scenario, got:\n{out}"
    );
}

/// Full Direct Pipeline Lab Scenario against Docker Lab Fixture + Instant Client.
///
/// ```bash
/// export LD_LIBRARY_PATH=/path/to/instantclient
/// cargo test -p migraloop-app --test cli_lab_scenario -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "requires Docker Compose Lab Fixture + Oracle Instant Client; not part of Release Quality Gate"]
async fn lab_scenario_direct_pipeline_run_and_inspect() {
    let lab = lab_dir();
    let store_url = "postgres://migraloop:migraloop@127.0.0.1:5432/migraloop";

    let down_first = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output()
        .expect("lab down cleanup");
    assert!(
        down_first.status.success(),
        "lab down (cleanup) failed: {}",
        String::from_utf8_lossy(&down_first.stderr)
    );

    let up = Command::new(bin())
        .args(["lab", "up", "--lab-dir", &lab])
        .output()
        .expect("lab up");
    let up_out = format!(
        "{}{}",
        String::from_utf8_lossy(&up.stdout),
        String::from_utf8_lossy(&up.stderr)
    );
    assert!(up.status.success(), "lab up failed:\n{up_out}");

    let run = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args(["lab", "scenario", "run", "direct-pipeline", "--lab-dir", &lab])
        .output()
        .expect("lab scenario run");
    let run_out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(
        run.status.success(),
        "lab scenario run failed:\n{run_out}"
    );
    assert!(
        run_out.contains("Lab Scenario: PASS"),
        "expected Scenario PASS, got:\n{run_out}"
    );
    assert!(
        run_out.contains("duration_ms="),
        "expected duration metric, got:\n{run_out}"
    );
    assert!(
        run_out.contains("rows_per_s=")
            || run_out.contains("throughput")
            || run_out.contains("rows_applied="),
        "expected rows/throughput metric, got:\n{run_out}"
    );
    assert!(
        run_out.to_ascii_lowercase().contains("logminer")
            || run_out.contains("Incremental Capture"),
        "Scenario must use real capture path, got:\n{run_out}"
    );

    // Namespace left in place: Base / Target still inspectable after the run.
    let base = Command::new(bin())
        .args([
            "base",
            "--platform-store-url",
            store_url,
            "--table",
            "LAB_DP_CUSTOMERS",
        ])
        .output()
        .expect("base inspect");
    let base_out = String::from_utf8_lossy(&base.stdout);
    assert!(
        base.status.success(),
        "base inspect failed: {}",
        String::from_utf8_lossy(&base.stderr)
    );
    assert!(
        base_out.contains("Alicia") && base_out.contains("Carol") && !base_out.contains("Bob"),
        "Base must reflect insert/update/delete after Scenario, got:\n{base_out}"
    );

    let target = Command::new(bin())
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "target",
            "--platform-store-url",
            store_url,
            "--collection",
            "lab_dp_customers",
        ])
        .output()
        .expect("target inspect");
    let target_out = String::from_utf8_lossy(&target.stdout);
    assert!(
        target.status.success(),
        "target inspect failed: {}",
        String::from_utf8_lossy(&target.stderr)
    );
    assert!(
        target_out.contains("Alicia")
            && target_out.contains("Carol")
            && !target_out.contains("Bob"),
        "Target Managed outcomes must match Scenario workload, got:\n{target_out}"
    );

    // Concurrent rejection while we fake an active lock after a finished run still works.
    // (Finished run releases the lock; Namespace remains.)
    let lock_path = format!("{lab}/.migraloop-scenario.lock");
    assert!(
        !std::path::Path::new(&lock_path).exists(),
        "finished Scenario must release the active-run lock (Namespace stays)"
    );

    let _ = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output();
}
