//! Operator-visible seam: Local Sync Lab Fixture up / status / down.
//!
//! Agreed seam (issue #59 / PRD #55): CLI Lab commands against the real stack.
//! Always-on tests cover Fixture-not-ready and CLI surface. Full bring-up is
//! ignored by default (needs Docker Compose + Lab Oracle image pull).

use std::process::Command;

fn bin() -> String {
    env!("CARGO_BIN_EXE_migraloop").to_string()
}

fn lab_dir() -> String {
    // Workspace root's `lab/` package (compose + Oracle init), relative to CARGO_MANIFEST_DIR
    // for the app crate (`crates/app`).
    let manifest = env!("CARGO_MANIFEST_DIR");
    format!("{manifest}/../../lab")
}

#[tokio::test]
async fn lab_status_reports_fixture_not_ready_when_stack_is_down() {
    // Ensure any prior Lab stack is absent so this assertion is stable in CI.
    let _ = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab_dir()])
        .output();

    let status = Command::new(bin())
        .args(["lab", "status", "--lab-dir", &lab_dir()])
        .output()
        .expect("run lab status");

    let stdout = String::from_utf8_lossy(&status.stdout);
    let stderr = String::from_utf8_lossy(&status.stderr);
    let combined = format!("{stdout}{stderr}");

    assert!(
        !status.status.success(),
        "lab status should fail when Fixture is not up, got success:\n{combined}"
    );
    assert!(
        combined.contains("Lab Fixture: not ready"),
        "expected Fixture not-ready report, got:\n{combined}"
    );
    // Status must not imply a default Pipeline exists.
    assert!(
        !combined.to_ascii_lowercase().contains("pipeline: orders")
            && !combined.contains("default Deployment"),
        "lab status must not advertise a default Deployment/Pipeline, got:\n{combined}"
    );
}

#[tokio::test]
async fn lab_help_lists_up_status_down() {
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
    for sub in ["up", "status", "down"] {
        assert!(
            stdout.contains(sub),
            "lab --help should list `{sub}`, got:\n{stdout}"
        );
    }
}

/// Full Lab Fixture lifecycle against Docker Compose + real Oracle/Mongo.
///
/// ```bash
/// cargo test -p migraloop-app --test cli_lab_fixture -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "requires Docker Compose and Lab Oracle image; not part of Release Quality Gate"]
async fn lab_up_status_down_fixture_lifecycle() {
    let lab = lab_dir();

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
    assert!(
        up_out.contains("Lab Fixture: ready") || up_out.contains("connection"),
        "lab up should report readiness / connection details, got:\n{up_out}"
    );
    assert!(
        up_out.contains("ORACLE_PASSWORD") || up_out.contains("Oracle"),
        "lab up should include Oracle connection details, got:\n{up_out}"
    );
    assert!(
        up_out.contains("MONGO_PASSWORD") || up_out.contains("Mongo"),
        "lab up should include MongoDB connection details, got:\n{up_out}"
    );

    let status = Command::new(bin())
        .args(["lab", "status", "--lab-dir", &lab])
        .output()
        .expect("lab status");
    let status_out = format!(
        "{}{}",
        String::from_utf8_lossy(&status.stdout),
        String::from_utf8_lossy(&status.stderr)
    );
    assert!(
        status.status.success(),
        "lab status after up failed:\n{status_out}"
    );
    assert!(
        status_out.contains("Lab Fixture: ready"),
        "expected Fixture ready, got:\n{status_out}"
    );
    assert!(
        status_out.contains("Deployment: (none)"),
        "bring-up must not pre-apply a Deployment, got:\n{status_out}"
    );
    assert!(
        status_out.contains("Pipeline: (none)"),
        "bring-up must not pre-apply Pipelines, got:\n{status_out}"
    );

    // Product status against Lab Platform Store must also show empty Deployment/Pipeline.
    let store_url = "postgres://migraloop:migraloop@127.0.0.1:5432/migraloop";
    let product_status = Command::new(bin())
        .args(["status", "--platform-store-url", store_url])
        .output()
        .expect("migraloop status");
    let product_out = String::from_utf8_lossy(&product_status.stdout);
    assert!(
        product_status.status.success(),
        "product status failed: {}",
        String::from_utf8_lossy(&product_status.stderr)
    );
    assert!(
        product_out.contains("Deployment: (none)"),
        "expected no Deployment after Fixture up, got:\n{product_out}"
    );
    assert!(
        product_out.contains("Pipeline: (none)"),
        "expected no Pipeline after Fixture up, got:\n{product_out}"
    );

    let down = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output()
        .expect("lab down");
    assert!(
        down.status.success(),
        "lab down failed: {}",
        String::from_utf8_lossy(&down.stderr)
    );

    let after = Command::new(bin())
        .args(["lab", "status", "--lab-dir", &lab])
        .output()
        .expect("lab status after down");
    let after_out = format!(
        "{}{}",
        String::from_utf8_lossy(&after.stdout),
        String::from_utf8_lossy(&after.stderr)
    );
    assert!(
        !after.status.success(),
        "lab status should fail after down, got:\n{after_out}"
    );
    assert!(
        after_out.contains("Lab Fixture: not ready"),
        "expected not ready after down, got:\n{after_out}"
    );
}
