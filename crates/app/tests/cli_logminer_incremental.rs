//! Operator-visible seam: LogMiner-backed Incremental Capture on Direct Pipeline.
//!
//! Agreed seam (issue #13 / PRD #3): CLI config/status + Base/Target outcomes.
//! Contract Oracle LogMiner harness drives Incremental Capture (same cutover,
//! checkpoint resume, and type rules as prior stub path). Operator still drives
//! via config + CLI + status; sync reports LogMiner as the capture mechanism.

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

fn deployment_contract_oracle(table: &str, collection: &str, mongo_database: &str) -> String {
    format!(
        r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: oracle-to-mongo
spec:
  source:
    kind: oracle
    host: contract
    port: 1521
    database: ORCL
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

#[tokio::test]
async fn logminer_contract_incremental_updates_base_and_mongo_on_direct_pipeline() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_contract_oracle("CUSTOMERS", "customers", &mongo_database),
    );

    migrate_and_apply(&url, &config, &doubles);

    let sync = run_sync(&url, &doubles);
    assert!(
        sync.status.success(),
        "sync failed: stdout={} stderr={}",
        String::from_utf8_lossy(&sync.stdout),
        String::from_utf8_lossy(&sync.stderr)
    );
    let sync_out = String::from_utf8_lossy(&sync.stdout);
    assert!(
        sync_out.to_ascii_lowercase().contains("logminer"),
        "expected LogMiner Incremental Capture on product path, got:\n{sync_out}"
    );

    let base_after = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", "CUSTOMERS"])
        .output()
        .expect("base after sync");
    assert!(base_after.status.success());
    let base_out = String::from_utf8_lossy(&base_after.stdout);
    assert!(
        base_out.contains("Alicia") && base_out.contains("Carol") && !base_out.contains("Bob"),
        "LogMiner Incremental must update/insert/delete Base rows, got:\n{base_out}"
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
    assert!(
        target.status.success(),
        "target inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&target.stdout),
        String::from_utf8_lossy(&target.stderr)
    );
    let target_out = String::from_utf8_lossy(&target.stdout);
    assert!(
        target_out.contains("Alicia") && target_out.contains("Carol") && !target_out.contains("Bob"),
        "Mongo Delivery must follow LogMiner Incremental Capture, got:\n{target_out}"
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("status after sync");
    assert!(status.status.success());
    let status_out = String::from_utf8_lossy(&status.stdout);
    assert!(
        status_out.contains("LogMiner")
            || status_out.to_ascii_lowercase().contains("logminer")
            || status_out.contains("Cutover:")
                && status_out.contains("Sync Health:")
                && status_out.contains("Delivery Health:"),
        "operator status must remain usable with LogMiner path, got:\n{status_out}"
    );
    assert!(
        status_out.contains("checkpoint=") && status_out.contains("appliedChanges="),
        "status must still expose checkpoint/appliedChanges for resume visibility, got:\n{status_out}"
    );
}

#[tokio::test]
async fn real_oracle_host_without_oci_fails_fast_naming_logminer() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    // Non-contract host selects the OCI LogMiner adapter (not the stub catalog).
    let config = write_config(
        &dir,
        "deployment.yaml",
        &format!(
            r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: oracle-to-mongo
spec:
  source:
    kind: oracle
    host: oracle.example.internal
    port: 1521
    database: ORCL
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
        table: CUSTOMERS
      target:
        collection: customers
"#,
            host = mongo_host(),
            port = mongo_port(),
        ),
    );

    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", &url])
        .output()
        .expect("migrate");
    assert!(migrate.status.success());

    // Prerequisites probe uses the same LogMiner backend as Incremental Capture, so
    // apply fails fast when OCI Instant Client is unavailable (no stub fallback).
    let apply = Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "apply",
            "--platform-store-url",
            &url,
            "--file",
            config.to_str().unwrap(),
        ])
        .output()
        .expect("apply");
    assert!(
        !apply.status.success(),
        "OCI LogMiner without Instant Client must fail apply, got success: {}",
        String::from_utf8_lossy(&apply.stdout)
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
    let lower = combined.to_ascii_lowercase();
    assert!(
        lower.contains("logminer") && (lower.contains("oci") || lower.contains("instant client")),
        "expected clear OCI/LogMiner failure, got:\n{combined}"
    );
}
