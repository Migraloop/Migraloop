//! Operator-visible seam: same-SCN LogMiner multi-change apply + resume-safe catch-up.
//!
//! Agreed seam (issue #143): CLI config/status + Base/Target outcomes on the contract
//! Deployment path. Multiple LogMiner rows sharing one SCN must land distinctly; a
//! mid-window process stop must not drop remaining same-SCN changes. Overlapping
//! replay remains idempotent (prefer duplicates over gaps).

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use migraloop_capture::{LogMinerContent, LogMinerOperation};
use serde_json::json;
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

/// Same-SCN stream: two UPDATEs on id=1 (A1→A2) then INSERT id=4, all at SCN 1050.
fn same_scn_extra_contents() -> Vec<LogMinerContent> {
    // Replace the default distinct-SCN CUSTOMERS fixture by installing only these
    // extras on top would still include Alicia@1050 / Carol@1060 / delete@1070.
    // Instead we install a dedicated inject that overrides via install helper below.
    vec![
        LogMinerContent::new(
            1050,
            LogMinerOperation::Update,
            "APP",
            "CUSTOMERS",
            BTreeMap::from([("ID".into(), json!(1))]),
            Some(BTreeMap::from([
                ("ID".into(), json!(1)),
                ("NAME".into(), json!("A1")),
                ("EMAIL".into(), json!("a1@example.com")),
                ("ACTIVE".into(), json!(1)),
                ("BIO".into(), json!("blob-a1")),
            ])),
        )
        .with_order("0xSAME.0001", 1),
        LogMinerContent::new(
            1050,
            LogMinerOperation::Update,
            "APP",
            "CUSTOMERS",
            BTreeMap::from([("ID".into(), json!(1))]),
            Some(BTreeMap::from([
                ("ID".into(), json!(1)),
                ("NAME".into(), json!("A2")),
                ("EMAIL".into(), json!("a2@example.com")),
                ("ACTIVE".into(), json!(1)),
                ("BIO".into(), json!("blob-a2")),
            ])),
        )
        .with_order("0xSAME.0001", 2),
        LogMinerContent::new(
            1050,
            LogMinerOperation::Insert,
            "APP",
            "CUSTOMERS",
            BTreeMap::from([("ID".into(), json!(4))]),
            Some(BTreeMap::from([
                ("ID".into(), json!(4)),
                ("NAME".into(), json!("Dana")),
                ("EMAIL".into(), json!("dana@example.com")),
                ("ACTIVE".into(), json!(1)),
                ("BIO".into(), json!("blob-dana")),
            ])),
        )
        .with_order("0xSAME.0002", 1),
    ]
}

/// Install catalog doubles but replace Incremental contents with same-SCN-only rows.
fn install_same_scn_doubles(dir: &Path) -> common::NamedScenarioDoubles {
    let doubles = common::NamedScenarioDoubles::install(dir);
    // Overwrite LogMiner inject with same-SCN-only CUSTOMERS stream (no ORDERS noise).
    let inject = json!({ "contents": same_scn_extra_contents() });
    fs::write(
        &doubles.logminer_path,
        serde_json::to_string_pretty(&inject).expect("serialize same-SCN inject"),
    )
    .expect("overwrite logminer inject");
    doubles
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
    cmd.env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value");
    doubles.apply_env(&mut cmd);
    cmd.args(["sync", "--platform-store-url", url])
        .output()
        .expect("run sync")
}

fn run_sync_fail_after(
    url: &str,
    after: u32,
    doubles: &common::NamedScenarioDoubles,
    queue_capacity: Option<u32>,
) -> std::process::Output {
    let mut cmd = Command::new(bin());
    cmd.env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value");
    doubles.apply_env(&mut cmd);
    cmd.args(["sync", "--platform-store-url", url]);
    common::SyncCliOptions {
        poison_identities: vec![],
        poison_max_attempts: None,
        queue_capacity: queue_capacity.map(|c| c as usize),
        delivery_delay_ms: None,
        fail_after_changes: Some(after),
    }
    .append_to(&mut cmd);
    cmd.output().expect("run sync with fail-after")
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
async fn same_scn_multi_change_resume_does_not_drop_siblings() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = install_same_scn_doubles(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_delivery("CUSTOMERS", "customers", &mongo_database),
    );

    migrate_and_apply(&url, &config, &doubles);

    // Kill after first durable same-SCN change; remaining peers must still apply on resume.
    // Tiny queue exercises bounded-window cut mid-SCN as well.
    let interrupted = run_sync_fail_after(&url, 1, &doubles, Some(1));
    assert!(
        !interrupted.status.success(),
        "mid-same-SCN kill simulation must fail the sync process, got success: {}",
        String::from_utf8_lossy(&interrupted.stdout)
    );

    let status_mid = run_status(&url);
    assert_eq!(
        extract_checkpoint(&status_mid),
        Some(1050),
        "checkpoint may sit on the shared SCN after first durable apply, status:\n{status_mid}"
    );
    assert_eq!(
        extract_sync_field(&status_mid, "appliedChanges").as_deref(),
        Some("1"),
        "only the first same-SCN change should count as applied, got:\n{status_mid}"
    );

    let base_mid = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", "CUSTOMERS"])
        .output()
        .expect("base mid");
    assert!(base_mid.status.success());
    let base_mid_out = String::from_utf8_lossy(&base_mid.stdout);
    assert!(
        base_mid_out.contains("A1"),
        "first same-SCN UPDATE (→A1) must be in Base, got:\n{base_mid_out}"
    );
    assert!(
        !base_mid_out.contains("A2") && !base_mid_out.contains("Dana"),
        "remaining same-SCN peers must not apply before resume, got:\n{base_mid_out}"
    );

    let resumed = run_sync(&url, &doubles);
    assert!(
        resumed.status.success(),
        "resume sync failed: stdout={} stderr={}",
        String::from_utf8_lossy(&resumed.stdout),
        String::from_utf8_lossy(&resumed.stderr)
    );

    let base_after = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", "CUSTOMERS"])
        .output()
        .expect("base after resume");
    assert!(base_after.status.success());
    let base_out = String::from_utf8_lossy(&base_after.stdout);
    assert!(
        base_out.contains("A2") && base_out.contains("Dana") && !base_out.contains("A1"),
        "resume must finish remaining same-SCN changes (final NAME=A2 + Dana), got:\n{base_out}"
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
        target_out.contains("A2") && target_out.contains("Dana"),
        "Delivery must catch up same-SCN multi-change apply, got:\n{target_out}"
    );

    let status_after = run_status(&url);
    assert_eq!(
        extract_checkpoint(&status_after),
        Some(1050),
        "final checkpoint remains the shared SCN after draining peers, status:\n{status_after}"
    );
    assert_eq!(
        extract_sync_field(&status_after, "lag").as_deref(),
        Some("0"),
        "lag must be caught up after same-SCN resume, got:\n{status_after}"
    );
    assert_eq!(
        extract_sync_field(&status_after, "appliedChanges").as_deref(),
        Some("3"),
        "all three same-SCN changes must apply exactly once, got:\n{status_after}"
    );

    // Idempotent replay: overlapping re-sync must not inflate counters / corrupt Base.
    let again = run_sync(&url, &doubles);
    assert!(again.status.success());
    let status_again = run_status(&url);
    assert_eq!(
        extract_sync_field(&status_again, "appliedChanges").as_deref(),
        Some("3"),
        "replay after same-SCN resume must absorb duplicates idempotently, got:\n{status_again}"
    );
}
