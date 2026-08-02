//! Operator-visible seam: blocking DDL Schema Change warn+pause (issue #23 / ADR-0009).
//!
//! Agreed seams (PRD / issue #3 / ADR-0009):
//! - CLI `sync` / `status` / `base` / `target` + resulting Base/Target outcomes
//! - Blocking schema impact → WARN + pause affected Pipeline(s)
//! - Unaffecting schema change → continue (Delivery proceeds)
//! - Distinct from poison quarantine (no Quarantine / unhealthy quarantine path)
//!
//! This is the non-ignored contract/stub CI twin of Lab Scenario
//! `schema-change-pause`. It must not run Lab Fixture / live Oracle.
//!
//! Schema Change events are injected via `MIGRALOOP_INJECT_SCHEMA_CHANGES`
//! (JSON file path) — test/Lab seam until LogMiner DDL capture lands on OCI.

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

fn unique_mongo_database() -> String {
    let suffix = common::unique_suffix();
    format!("appdb_{suffix}")
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

fn write_schema_inject(dir: &TempDir, contents: &str) -> PathBuf {
    let path = dir.path().join("schema_changes.json");
    fs::write(&path, contents).expect("write schema inject");
    path
}

fn deployment_with_direct_delivery(mongo_database: &str) -> String {
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
        table: CUSTOMERS
      target:
        collection: customers
"#,
        host = mongo_host(),
        port = mongo_port(),
    )
}

fn migrate_and_apply(url: &str, config: &Path) {
    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", url])
        .output()
        .expect("run migrate");
    assert!(
        migrate.status.success(),
        "migrate failed: {}",
        String::from_utf8_lossy(&migrate.stderr)
    );

    let apply = Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
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

#[tokio::test]
async fn blocking_ddl_warns_and_pauses_affected_pipeline_not_quarantine() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_delivery(&mongo_database),
    );
    // Drop managed NAME column before Incremental DML (SCN 1045 < 1050 update).
    let inject = write_schema_inject(
        &dir,
        r#"{
  "changes": [
    {
      "scn": 1045,
      "table": "CUSTOMERS",
      "schema": "APP",
      "kind": "drop_column",
      "columns": ["NAME"],
      "summary": "ALTER TABLE CUSTOMERS DROP COLUMN NAME"
    }
  ]
}"#,
    );

    migrate_and_apply(&url, &config);

    let sync = Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .env(
            "MIGRALOOP_INJECT_SCHEMA_CHANGES",
            inject.to_str().unwrap(),
        )
        .args(["sync", "--platform-store-url", &url])
        .output()
        .expect("run sync");
    let sync_out = format!(
        "{}{}",
        String::from_utf8_lossy(&sync.stdout),
        String::from_utf8_lossy(&sync.stderr)
    );
    assert!(
        sync.status.success(),
        "sync must succeed after warn+pause: {sync_out}"
    );
    let sync_lower = sync_out.to_ascii_lowercase();
    assert!(
        sync_lower.contains("warn")
            && sync_lower.contains("schema change")
            && sync_lower.contains("paused"),
        "expected WARN + Schema Change + paused, got:\n{sync_out}"
    );
    assert!(
        !sync_lower.contains("alert: poison")
            && !sync_out.contains("Quarantine:")
            && sync_lower.contains("not poison quarantine"),
        "blocking DDL pause must be distinct from poison quarantine, got:\n{sync_out}"
    );

    // Base Capture may still advance after pause; Target Delivery must not.
    // Contract Initial Load already includes Alice/Bob/Carol — pause must freeze
    // that snapshot (no Alicia update, no Bob delete) rather than strip Carol.
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
        .expect("target after blocking DDL");
    assert!(
        target.status.success(),
        "target inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&target.stdout),
        String::from_utf8_lossy(&target.stderr)
    );
    let target_out = String::from_utf8_lossy(&target.stdout);
    assert!(
        target_out.contains("Alice")
            && target_out.contains("Bob")
            && target_out.contains("Carol")
            && !target_out.contains("Alicia"),
        "paused Pipeline must keep Initial Load Target snapshot (no Incremental Delivery), got:\n{target_out}"
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("status after blocking DDL");
    assert!(
        status.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    let status_out = String::from_utf8_lossy(&status.stdout);
    let status_lower = status_out.to_ascii_lowercase();
    assert!(
        status_out.contains("customers")
            && (status_out.contains("paused") || status_lower.contains("paused"))
            && status_lower.contains("schema change")
            && status_lower.contains("blocking"),
        "expected paused Pipeline + Schema Change blocking visibility, got:\n{status_out}"
    );
    assert!(
        status_out.contains("Delivery Health: paused")
            || status_lower.contains("delivery health: paused"),
        "expected Delivery Health paused (not quarantine unhealthy), got:\n{status_out}"
    );
    assert!(
        status_out.contains("Quarantine: (none)")
            || !status_lower.contains("unhealthy / not aligned"),
        "blocking DDL must not create poison quarantine rows, got:\n{status_out}"
    );
}

#[tokio::test]
async fn unaffecting_schema_change_continues_delivery() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_delivery(&mongo_database),
    );
    // ADD unused NOTES column — does not affect Direct Pipeline dependencies.
    let inject = write_schema_inject(
        &dir,
        r#"{
  "changes": [
    {
      "scn": 1045,
      "table": "CUSTOMERS",
      "schema": "APP",
      "kind": "add_column",
      "columns": ["NOTES"],
      "summary": "ALTER TABLE CUSTOMERS ADD NOTES VARCHAR2(100)"
    }
  ]
}"#,
    );

    migrate_and_apply(&url, &config);

    let sync = Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .env(
            "MIGRALOOP_INJECT_SCHEMA_CHANGES",
            inject.to_str().unwrap(),
        )
        .args(["sync", "--platform-store-url", &url])
        .output()
        .expect("run sync");
    let sync_out = format!(
        "{}{}",
        String::from_utf8_lossy(&sync.stdout),
        String::from_utf8_lossy(&sync.stderr)
    );
    assert!(
        sync.status.success(),
        "sync must succeed for unaffecting DDL: {sync_out}"
    );
    let sync_lower = sync_out.to_ascii_lowercase();
    assert!(
        sync_lower.contains("schema change") && sync_lower.contains("unaffecting"),
        "expected unaffecting Schema Change continue, got:\n{sync_out}"
    );
    assert!(
        !sync_lower
            .lines()
            .any(|line| line.contains("paused") && line.contains("customers")),
        "unaffecting DDL must not pause the Pipeline, got:\n{sync_out}"
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
        .expect("target after unaffecting DDL");
    assert!(target.status.success());
    let target_out = String::from_utf8_lossy(&target.stdout);
    assert!(
        target_out.contains("Alicia")
            && target_out.contains("Carol")
            && !target_out.contains("Bob"),
        "unaffecting DDL must allow Incremental Delivery to continue, got:\n{target_out}"
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("status after unaffecting DDL");
    assert!(status.status.success());
    let status_out = String::from_utf8_lossy(&status.stdout);
    let status_lower = status_out.to_ascii_lowercase();
    assert!(
        !status_out.contains("Delivery Health: paused")
            && !status_lower.contains("impact=blocking"),
        "unaffecting DDL must leave Pipeline running, got:\n{status_out}"
    );
}
