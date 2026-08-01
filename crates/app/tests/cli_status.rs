//! Operator-visible seam: CLI status / migrate against a real Platform Store.
//!
//! Agreed seam (issue #4 / PRD): verify via CLI output, not private internals.

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn admin_url() -> String {
    std::env::var("MIGRALOOP_TEST_ADMIN_URL").unwrap_or_else(|_| {
        "postgres://migraloop:migraloop@127.0.0.1:5432/postgres".to_string()
    })
}

fn bin() -> String {
    env!("CARGO_BIN_EXE_migraloop").to_string()
}

async fn ephemeral_database_url() -> String {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let db_name = format!("migraloop_test_{suffix}");
    let admin = admin_url();

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&admin)
        .await
        .expect("connect to admin database for test setup");

    sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
        .execute(&pool)
        .await
        .expect("create ephemeral Platform Store database");

    // Rewrite path to the ephemeral database name.
    let base = admin
        .rsplit_once('/')
        .map(|(prefix, _)| prefix.to_string())
        .expect("admin url must include a database path");
    format!("{base}/{db_name}")
}

#[tokio::test]
async fn status_reports_platform_store_healthy_after_migrate() {
    let url = ephemeral_database_url().await;

    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", &url])
        .output()
        .expect("run migrate");
    assert!(
        migrate.status.success(),
        "migrate failed: {}",
        String::from_utf8_lossy(&migrate.stderr)
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("run status");
    assert!(
        status.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );

    let stdout = String::from_utf8_lossy(&status.stdout);
    assert!(
        stdout.contains("Platform Store: healthy"),
        "expected healthy Platform Store in status output, got:\n{stdout}"
    );
    assert!(
        stdout.contains("Schema version:"),
        "expected schema version in status output, got:\n{stdout}"
    );
}

#[tokio::test]
async fn status_reports_unreachable_for_bad_platform_store_url() {
    let status = Command::new(bin())
        .args([
            "status",
            "--platform-store-url",
            "postgres://migraloop:migraloop@127.0.0.1:1/does_not_exist",
        ])
        .output()
        .expect("run status");

    assert!(
        !status.status.success(),
        "status should fail when Platform Store is unreachable"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&status.stdout),
        String::from_utf8_lossy(&status.stderr)
    );
    assert!(
        combined.contains("Platform Store: unreachable"),
        "expected unreachable Platform Store report, got:\n{combined}"
    );
}

#[tokio::test]
async fn status_reports_unhealthy_when_reachable_but_not_migrated() {
    let url = ephemeral_database_url().await;

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("run status");

    assert!(
        !status.status.success(),
        "status should fail when Platform Store is not migrated"
    );
    let stdout = String::from_utf8_lossy(&status.stdout);
    assert!(
        stdout.contains("Platform Store: unhealthy"),
        "expected unhealthy (reachable but not migrated), got:\n{stdout}"
    );
    assert!(
        !stdout.contains("Platform Store: unreachable"),
        "must not mislabel a reachable unmigrated store as unreachable:\n{stdout}"
    );
}
