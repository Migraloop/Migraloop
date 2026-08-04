//! Operator-visible seam: resume Capture/Delivery from Platform Store checkpoints.
//!
//! Agreed seam (issue #10 / PRD): CLI config/status + resulting Base/Target outcomes.
//! After a mid-incremental process stop, a fresh process resumes from durable Platform
//! Store checkpoints without redoing completed work incorrectly. Resume must not rely
//! on ephemeral local-only state. Status shows resumed health/lag coherently.

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

fn admin_url() -> String {
    std::env::var("MIGRALOOP_TEST_ADMIN_URL").unwrap_or_else(|_| {
        "postgres://migraloop:migraloop@127.0.0.1:5432/postgres".to_string()
    })
}

fn mongo_host() -> String {
    std::env::var("MIGRALOOP_TEST_MONGO_HOST").unwrap_or_else(|_| "127.0.0.1".to_string())
}

fn mongo_port() -> u16 {
    std::env::var("MIGRALOOP_TEST_MONGO_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(27017)
}

fn bin() -> String {
    env!("CARGO_BIN_EXE_migraloop").to_string()
}

async fn ephemeral_database_url() -> String {
    let suffix = common::unique_suffix();
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

    let base = admin
        .rsplit_once('/')
        .map(|(prefix, _)| prefix.to_string())
        .expect("admin url must include a database path");
    format!("{base}/{db_name}")
}

fn write_config(dir: &TempDir, name: &str, contents: &str) -> PathBuf {
    let path = dir.path().join(name);
    fs::write(&path, contents).expect("write config");
    path
}

fn unique_mongo_database() -> String {
    let suffix = common::unique_suffix();
    format!("appdb_{suffix}")
}

fn deployment_with_direct_delivery(table: &str, collection: &str, mongo_database: &str) -> String {
    format!(
        r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: oracle-to-mongo
spec:
  source:
    kind: oracle
    host: stub
    port: 1521
    database: STUB
    username: sync_user
    password:
      fromEnv: ORACLE_PASSWORD
  target:
    kind: mongodb
    host: {host}
    port: {port}
    database: {mongo_database}
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
  pipelines:
    - name: customers
      mode: direct
      source:
        table: {table}
      target:
        collection: {collection}
"#,
        host = mongo_host(),
        port = mongo_port(),
    )
}

fn migrate_and_apply(url: &str, config: &Path, doubles: &common::NamedScenarioDoubles) {
    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", url])
        .output()
        .expect("run migrate");
    assert!(
        migrate.status.success(),
        "migrate failed: {}",
        String::from_utf8_lossy(&migrate.stderr)
    );

    let mut apply = Command::new(bin());
    apply
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value");
    doubles.apply_env(&mut apply);
    let apply = apply
        .args([
            "apply",
            "--platform-store-url",
            url,
            "--file",
            config.to_str().unwrap(),
        ])
        .output()
        .expect("run apply");

    assert!(
        apply.status.success(),
        "apply failed: stdout={} stderr={}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
}

fn run_sync(url: &str, doubles: &common::NamedScenarioDoubles) -> std::process::Output {
    let mut cmd = Command::new(bin());
    cmd
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value");
    doubles.apply_env(&mut cmd);
    cmd
        .args(["sync", "--platform-store-url", url])
        .output()
        .expect("run sync")

}

fn run_sync_fail_after(url: &str, after: u32, doubles: &common::NamedScenarioDoubles) -> std::process::Output {
    let mut cmd = Command::new(bin());
    cmd
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .env("MIGRALOOP_SYNC_FAIL_AFTER_CHANGES", after.to_string());
    doubles.apply_env(&mut cmd);
    cmd
        .args(["sync", "--platform-store-url", url])
        .output()
        .expect("run sync with fail-after")

}

fn run_status(url: &str) -> String {
    let status = Command::new(bin())
        .args(["status", "--platform-store-url", url])
        .output()
        .expect("run status");
    assert!(
        status.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    String::from_utf8_lossy(&status.stdout).to_string()
}

fn extract_sync_field(status: &str, field: &str) -> Option<String> {
    status
        .lines()
        .find(|l| l.contains("Sync Health") && l.contains(&format!("{field}=")))
        .and_then(|l| l.split(&format!("{field}=")).nth(1))
        .and_then(|rest| rest.split_whitespace().next().map(|s| s.to_string()))
}

fn extract_checkpoint(status: &str) -> Option<i64> {
    status
        .lines()
        .find(|l| l.contains("checkpoint="))
        .and_then(|l| l.split("checkpoint=").nth(1))
        .and_then(|rest| {
            rest.split(|c: char| c.is_whitespace() || c == ',' || c == ')')
                .next()
                .and_then(|n| n.parse().ok())
        })
}

#[tokio::test]
async fn kill_mid_incremental_resumes_capture_and_delivery_from_checkpoint() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_delivery("CUSTOMERS", "customers", &mongo_database),
    );

    migrate_and_apply(&url, &config, &doubles);

    // Simulate process kill after the first Incremental change is durably checkpointed.
    let interrupted = run_sync_fail_after(&url, 1, &doubles);
    assert!(
        !interrupted.status.success(),
        "mid-incremental kill simulation must fail the sync process, got success: {}",
        String::from_utf8_lossy(&interrupted.stdout)
    );
    let interrupted_out = String::from_utf8_lossy(&interrupted.stdout);
    assert!(
        interrupted_out.to_ascii_lowercase().contains("checkpoint")
            || interrupted_out.to_ascii_lowercase().contains("resume")
            || interrupted_out.to_ascii_lowercase().contains("applied"),
        "interrupted sync should report checkpoint progress, got:\n{interrupted_out}"
    );

    let status_mid = run_status(&url);
    let checkpoint_mid = extract_checkpoint(&status_mid);
    assert_eq!(
        checkpoint_mid,
        Some(1050),
        "Platform Store checkpoint must advance after first durable change, status:\n{status_mid}"
    );
    let lag_mid = extract_sync_field(&status_mid, "lag");
    assert_eq!(
        lag_mid.as_deref(),
        Some("2"),
        "status must show remaining Sync lag after mid-incremental stop, got:\n{status_mid}"
    );
    let applied_mid = extract_sync_field(&status_mid, "appliedChanges");
    assert_eq!(
        applied_mid.as_deref(),
        Some("1"),
        "only the checkpointed change should count as applied, got:\n{status_mid}"
    );
    assert!(
        status_mid.contains("Sync Health: lagging")
            && status_mid.contains("lag=2")
            && status_mid.contains("checkpoint=1050"),
        "status should show lagging Sync Health with coherent lag/checkpoint after interrupt, got:\n{status_mid}"
    );

    let base_mid = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", "CUSTOMERS"])
        .output()
        .expect("base mid");
    assert!(base_mid.status.success());
    let base_mid_out = String::from_utf8_lossy(&base_mid.stdout);
    assert!(
        base_mid_out.contains("Alicia"),
        "first checkpointed change (Alice→Alicia) must be in Base, got:\n{base_mid_out}"
    );
    assert!(
        base_mid_out.contains("Bob"),
        "delete of Bob must not apply before its checkpoint, got:\n{base_mid_out}"
    );

    // Fresh process: only Platform Store URL — no local resume files.
    let local_junk = dir.path().join("should-not-be-read-for-resume.json");
    fs::write(&local_junk, r#"{"checkpoint": 999999}"#).expect("write decoy local state");

    let resumed = run_sync(&url, &doubles);
    assert!(
        resumed.status.success(),
        "resume sync failed: stdout={} stderr={}",
        String::from_utf8_lossy(&resumed.stdout),
        String::from_utf8_lossy(&resumed.stderr)
    );
    let resumed_out = String::from_utf8_lossy(&resumed.stdout);
    assert!(
        resumed_out.to_ascii_lowercase().contains("resume")
            || resumed_out.to_ascii_lowercase().contains("checkpoint"),
        "resume sync should report checkpoint resume, got:\n{resumed_out}"
    );

    let base_after = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", "CUSTOMERS"])
        .output()
        .expect("base after resume");
    assert!(base_after.status.success());
    let base_out = String::from_utf8_lossy(&base_after.stdout);
    assert!(
        base_out.contains("Alicia") && base_out.contains("Carol") && !base_out.contains("Bob"),
        "resume must finish remaining Incremental changes without gaps, got:\n{base_out}"
    );

    let target = Command::new(bin())
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "target",
            "--platform-store-url",
            &url,
            "--collection",
            "customers",
        ])
        .output()
        .expect("target after resume");
    assert!(target.status.success());
    let target_out = String::from_utf8_lossy(&target.stdout);
    assert!(
        target_out.contains("Alicia") && target_out.contains("Carol") && !target_out.contains("Bob"),
        "Delivery must resume from checkpoint to final Target state, got:\n{target_out}"
    );

    let status_after = run_status(&url);
    assert_eq!(
        extract_checkpoint(&status_after),
        Some(1070),
        "final checkpoint must be last applied position, status:\n{status_after}"
    );
    assert_eq!(
        extract_sync_field(&status_after, "lag").as_deref(),
        Some("0"),
        "lag must be coherent (caught up) after resume completes, got:\n{status_after}"
    );
    assert_eq!(
        extract_sync_field(&status_after, "appliedChanges").as_deref(),
        Some("3"),
        "completed work must not be redone incorrectly (appliedChanges=3), got:\n{status_after}"
    );
    assert!(
        status_after.contains("Delivery Health: ok")
            && status_after.contains("appliedChanges=6"),
        "Delivery Health must remain coherent after Initial Load + resumed Incremental, got:\n{status_after}"
    );

    // Idempotent: another fresh process must not inflate counters.
    let again = run_sync(&url, &doubles);
    assert!(again.status.success());
    let status_again = run_status(&url);
    assert_eq!(
        extract_sync_field(&status_again, "appliedChanges").as_deref(),
        Some("3"),
        "replay after resume must not redo completed Sync work, got:\n{status_again}"
    );
}

#[tokio::test]
async fn resume_does_not_depend_on_ephemeral_local_state() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    // Keep injectable doubles outside the wiped operator workspace.
    let doubles_dir = TempDir::new().expect("doubles dir");
    let doubles = common::NamedScenarioDoubles::install(doubles_dir.path());
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_delivery("CUSTOMERS", "customers", &mongo_database),
    );

    migrate_and_apply(&url, &config, &doubles);
    let interrupted = run_sync_fail_after(&url, 1, &doubles);
    assert!(!interrupted.status.success());

    // Wipe the only local workspace the operator had; resume with Platform Store URL alone.
    drop(dir);

    let cwd = TempDir::new().expect("empty cwd");
    let mut resumed = Command::new(bin());
    resumed
        .current_dir(cwd.path())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .env_remove("MIGRALOOP_SYNC_FAIL_AFTER_CHANGES");
    doubles.apply_env(&mut resumed);
    let resumed = resumed
        .args(["sync", "--platform-store-url", &url])
        .output()
        .expect("resume from empty local dir");
    assert!(
        resumed.status.success(),
        "resume must work from Platform Store alone: stdout={} stderr={}",
        String::from_utf8_lossy(&resumed.stdout),
        String::from_utf8_lossy(&resumed.stderr)
    );

    let status = run_status(&url);
    assert_eq!(extract_sync_field(&status, "lag").as_deref(), Some("0"));
    assert_eq!(
        extract_sync_field(&status, "appliedChanges").as_deref(),
        Some("3")
    );
}
