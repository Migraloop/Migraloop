//! Operator-visible seam: no-gap Initial↔Incremental cutover with overlap dedupe.
//!
//! Agreed seam (issue #9 / ADR-0004 / PRD): CLI apply/sync/status + Base/Target outcomes.
//! Low-watermark is established before Initial Load; Incremental Capture starts from that
//! watermark (overlap); duplicate overlap applies are absorbed idempotently; starting
//! Incremental without a watermark is rejected in the running path.

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

fn migrate_and_apply(url: &str, config: &Path, doubles: &common::NamedScenarioDoubles) -> std::process::Output {
    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", url])
        .output()
        .expect("run migrate");
    assert!(
        migrate.status.success(),
        "migrate failed: {}",
        String::from_utf8_lossy(&migrate.stderr)
    );

    let mut cmd = Command::new(bin());
    cmd
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value");
    doubles.apply_env(&mut cmd);
    cmd
        .args([
            "apply",
            "--platform-store-url",
            url,
            "--file",
            config.to_str().unwrap(),
        ])
        .output()
        .expect("run apply")

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

#[tokio::test]
async fn cutover_establishes_low_watermark_and_loses_no_source_changes() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_delivery("CUSTOMERS", "customers", &mongo_database),
    );

    let apply = migrate_and_apply(&url, &config, &doubles);
    assert!(
        apply.status.success(),
        "apply failed: stdout={} stderr={}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
    let apply_out = String::from_utf8_lossy(&apply.stdout);
    assert!(
        apply_out.to_ascii_lowercase().contains("low-watermark")
            || apply_out.to_ascii_lowercase().contains("low_watermark")
            || apply_out.to_ascii_lowercase().contains("watermark"),
        "Initial Load must establish a low-watermark before snapshot, got:\n{apply_out}"
    );

    // Snapshot has Alice (pre-update), Bob, and Carol (overlap with Incremental INSERT).
    // Alicia change is after watermark and must not be lost at cutover.
    let base_before = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", "CUSTOMERS"])
        .output()
        .expect("base before sync");
    assert!(base_before.status.success());
    let before = String::from_utf8_lossy(&base_before.stdout);
    assert!(
        before.contains("Alice")
            && before.contains("Bob")
            && before.contains("Carol")
            && !before.contains("Alicia"),
        "snapshot must include overlap Carol but not post-watermark Alicia, got:\n{before}"
    );
    assert!(
        before.to_ascii_lowercase().contains("low-watermark")
            || before.to_ascii_lowercase().contains("watermark"),
        "Base inspect must surface cutover low-watermark, got:\n{before}"
    );

    let sync = run_sync(&url, &doubles);
    assert!(
        sync.status.success(),
        "sync failed: stdout={} stderr={}",
        String::from_utf8_lossy(&sync.stdout),
        String::from_utf8_lossy(&sync.stderr)
    );
    let sync_out = String::from_utf8_lossy(&sync.stdout);
    assert!(
        sync_out.to_ascii_lowercase().contains("overlap")
            || sync_out.to_ascii_lowercase().contains("watermark")
            || sync_out.to_ascii_lowercase().contains("incremental"),
        "sync should report Incremental Capture from cutover watermark, got:\n{sync_out}"
    );

    // Post-watermark update Alice→Alicia must not be lost at cutover (ADR-0004).
    let base_after = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", "CUSTOMERS"])
        .output()
        .expect("base after sync");
    assert!(base_after.status.success());
    let base_out = String::from_utf8_lossy(&base_after.stdout);
    assert!(
        base_out.contains("Alicia") && base_out.contains("alicia@example.com"),
        "cutover must apply post-watermark source update (no gap), got:\n{base_out}"
    );
    assert!(
        base_out.contains("Carol") && !base_out.contains("Bob"),
        "cutover Incremental batch must also apply insert/delete, got:\n{base_out}"
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
        .expect("target after sync");
    assert!(target.status.success());
    let target_out = String::from_utf8_lossy(&target.stdout);
    assert!(
        target_out.contains("Alicia") && target_out.contains("Carol") && !target_out.contains("Bob"),
        "Target must reflect no-gap cutover apply, got:\n{target_out}"
    );
}

#[tokio::test]
async fn overlapping_cutover_changes_are_idempotent_on_reapply() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_delivery("CUSTOMERS", "customers", &mongo_database),
    );

    let apply = migrate_and_apply(&url, &config, &doubles);
    assert!(apply.status.success());

    let sync1 = run_sync(&url, &doubles);
    assert!(
        sync1.status.success(),
        "first sync failed: stdout={} stderr={}",
        String::from_utf8_lossy(&sync1.stdout),
        String::from_utf8_lossy(&sync1.stderr)
    );

    let base_after_first = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", "CUSTOMERS"])
        .output()
        .expect("base after first sync");
    assert!(base_after_first.status.success());
    let first_base = String::from_utf8_lossy(&base_after_first.stdout).to_string();

    let status_after_first = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("status after first sync");
    assert!(status_after_first.status.success());
    let first_status = String::from_utf8_lossy(&status_after_first.stdout).to_string();

    // Re-run sync: overlapping/replayed change_ids must not corrupt Base/Target.
    let sync2 = run_sync(&url, &doubles);
    assert!(
        sync2.status.success(),
        "second sync (overlap reapply) failed: stdout={} stderr={}",
        String::from_utf8_lossy(&sync2.stdout),
        String::from_utf8_lossy(&sync2.stderr)
    );
    let sync2_out = String::from_utf8_lossy(&sync2.stdout);
    assert!(
        sync2_out.to_ascii_lowercase().contains("dedup")
            || sync2_out.to_ascii_lowercase().contains("idempotent")
            || sync2_out.to_ascii_lowercase().contains("0 changes")
            || sync2_out.to_ascii_lowercase().contains("already applied"),
        "second sync should report dedupe/idempotent overlap absorb, got:\n{sync2_out}"
    );

    let base_after_second = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", "CUSTOMERS"])
        .output()
        .expect("base after second sync");
    assert!(base_after_second.status.success());
    let second_base = String::from_utf8_lossy(&base_after_second.stdout);
    assert!(
        second_base.contains("Alicia")
            && second_base.contains("Carol")
            && !second_base.contains("Bob"),
        "overlap reapply must not corrupt Base rows, got:\n{second_base}"
    );
    // Row identity set must remain stable (no duplicate Carol / resurrected Bob).
    let alicia_count = second_base.matches("Alicia").count();
    let carol_count = second_base.matches("Carol").count();
    assert!(
        alicia_count >= 1 && carol_count >= 1,
        "expected Alicia and Carol still present once logically, got:\n{second_base}"
    );
    assert_eq!(
        first_base.matches("\"ID\"").count(),
        second_base.matches("\"ID\"").count(),
        "Base row count must not grow from duplicate overlap apply:\nfirst:\n{first_base}\nsecond:\n{second_base}"
    );

    let status_after_second = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("status after second sync");
    assert!(status_after_second.status.success());
    let second_status = String::from_utf8_lossy(&status_after_second.stdout);
    // Deduped replay must not inflate Sync appliedChanges.
    let extract_sync_applied = |status: &str| -> Option<i32> {
        status
            .lines()
            .find(|l| l.contains("Sync Health") && l.contains("appliedChanges="))
            .and_then(|l| l.split("appliedChanges=").nth(1))
            .and_then(|rest| {
                rest.split_whitespace()
                    .next()
                    .and_then(|n| n.parse().ok())
            })
    };
    let first_applied = extract_sync_applied(&first_status);
    let second_applied = extract_sync_applied(&second_status);
    assert_eq!(
        first_applied, second_applied,
        "dedupe must not inflate Sync appliedChanges on overlap replay:\nfirst:\n{first_status}\nsecond:\n{second_status}"
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
        .expect("target after second sync");
    assert!(target.status.success());
    let target_out = String::from_utf8_lossy(&target.stdout);
    assert!(
        target_out.contains("Alicia") && target_out.contains("Carol") && !target_out.contains("Bob"),
        "overlap reapply must not corrupt Target, got:\n{target_out}"
    );
}

#[tokio::test]
async fn incremental_without_low_watermark_is_rejected() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_delivery("CUSTOMERS", "customers", &mongo_database),
    );

    let apply = migrate_and_apply(&url, &config, &doubles);
    assert!(apply.status.success());

    // Simulate a Base Dataset that somehow completed a snapshot without cutover watermark
    // (gap-tolerant hand-off). The running Incremental path must refuse this.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect ephemeral store");
    sqlx::query(
        r#"
        UPDATE base_datasets
        SET capture_low_watermark = NULL,
            capture_checkpoint = NULL
        WHERE source_table = 'CUSTOMERS'
        "#,
    )
    .execute(&pool)
    .await
    .expect("clear cutover watermark for reject fixture");

    let sync = run_sync(&url, &doubles);
    assert!(
        !sync.status.success(),
        "Incremental without low-watermark overlap must be rejected, but sync succeeded:\n{}",
        String::from_utf8_lossy(&sync.stdout)
    );
    let err = format!(
        "{}{}",
        String::from_utf8_lossy(&sync.stdout),
        String::from_utf8_lossy(&sync.stderr)
    );
    let lower = err.to_ascii_lowercase();
    assert!(
        lower.contains("watermark")
            || lower.contains("overlap")
            || lower.contains("cutover"),
        "rejection must mention watermark/overlap/cutover, got:\n{err}"
    );
}
