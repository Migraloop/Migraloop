//! Operator-visible seam: remove Pipeline (issue #20 / ADR-0007).
//!
//! Agreed seam: CLI `remove` / `sync` / `status` / `base` / `target` + Platform
//! Store Pipeline deletion. Remove stops the Pipeline and ceases Delivery
//! without restarting the Deployment; Shared Base Datasets remain when other
//! Pipelines still reference them; `status` no longer lists the Pipeline as
//! active.
//!
//! This is the non-ignored contract/stub CI twin of Lab Scenario `remove-pipeline`.
//! It must not run Lab Fixture / live Oracle.

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

fn two_direct_pipelines(mongo_database: &str) -> String {
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
    - name: orders
      mode: direct
      source:
        table: ORDERS
      target:
        collection: orders
"#,
        host = mongo_host(),
        port = mongo_port(),
    )
}

fn two_pipelines_shared_customers(mongo_database: &str) -> String {
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
    - name: customers_reporting
      mode: direct
      source:
        table: CUSTOMERS
      target:
        collection: customers_reporting
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

fn run_sync(url: &str) -> String {
    let sync = Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args(["sync", "--platform-store-url", url])
        .output()
        .expect("run sync");
    assert!(
        sync.status.success(),
        "sync failed: stdout={} stderr={}",
        String::from_utf8_lossy(&sync.stdout),
        String::from_utf8_lossy(&sync.stderr)
    );
    String::from_utf8_lossy(&sync.stdout).into_owned()
}

fn remove_pipeline(url: &str, pipeline: &str) -> String {
    let out = Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "remove",
            "--platform-store-url",
            url,
            "--pipeline",
            pipeline,
        ])
        .output()
        .expect("run remove");
    assert!(
        out.status.success(),
        "remove failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn status(url: &str) -> String {
    let status = Command::new(bin())
        .args(["status", "--platform-store-url", url])
        .output()
        .expect("run status");
    assert!(
        status.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    String::from_utf8_lossy(&status.stdout).into_owned()
}

fn target_stdout(url: &str, collection: &str) -> String {
    let target = Command::new(bin())
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "target",
            "--platform-store-url",
            url,
            "--collection",
            collection,
        ])
        .output()
        .expect("run target");
    assert!(
        target.status.success(),
        "target inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&target.stdout),
        String::from_utf8_lossy(&target.stderr)
    );
    String::from_utf8_lossy(&target.stdout).into_owned()
}

/// Direct Target wire read — `migraloop target` needs a live Pipeline binding.
fn mongo_collection_dump(database: &str, collection: &str) -> String {
    let script = format!(
        r#"
from pymongo import MongoClient
c = MongoClient(
    "mongodb://deliver_user:mongo-secret-value@{host}:{port}/?authSource=admin"
)
docs = list(c["{database}"]["{collection}"].find().sort("_id", 1))
for d in docs:
    print(d)
"#,
        host = mongo_host(),
        port = mongo_port(),
        database = database,
        collection = collection,
    );
    let out = Command::new("python3")
        .args(["-c", &script])
        .output()
        .expect("run pymongo dump");
    assert!(
        out.status.success(),
        "pymongo dump failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn base_stdout(url: &str, table: &str) -> String {
    let base = Command::new(bin())
        .args(["base", "--platform-store-url", url, "--table", table])
        .output()
        .expect("run base");
    assert!(
        base.status.success(),
        "base inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&base.stdout),
        String::from_utf8_lossy(&base.stderr)
    );
    String::from_utf8_lossy(&base.stdout).into_owned()
}

fn status_has_pipeline(status_out: &str, pipeline: &str) -> bool {
    status_out.contains(&format!("Pipeline: {pipeline} ("))
}

fn delivery_complete_for(stdout: &str, pipeline: &str) -> bool {
    let needle = format!("Delivery complete: Pipeline {pipeline} ");
    stdout.contains(&needle)
        || stdout
            .lines()
            .any(|line| line.trim_end() == format!("Delivery complete: Pipeline {pipeline}"))
}

#[tokio::test]
async fn remove_stops_pipeline_delivery_status_inactive_deployment_remains() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.yaml",
        &two_direct_pipelines(&mongo_database),
    );

    migrate_and_apply(&url, &config);

    let remove_out = remove_pipeline(&url, "customers");
    assert!(
        remove_out.to_ascii_lowercase().contains("removed")
            && remove_out.contains("customers"),
        "remove must report removed Pipeline customers, got:\n{remove_out}"
    );

    let status_after = status(&url);
    assert!(
        !status_has_pipeline(&status_after, "customers"),
        "status must no longer list removed Pipeline customers as active, got:\n{status_after}"
    );
    assert!(
        status_has_pipeline(&status_after, "orders"),
        "status must still list unaffected Pipeline orders, got:\n{status_after}"
    );
    assert!(
        status_after.contains("oracle-to-mongo")
            || status_after.to_ascii_lowercase().contains("deployment"),
        "Deployment must remain up after Pipeline remove, got:\n{status_after}"
    );

    // Pre-remove Target snapshot (Initial Load Alice/Bob) via wire protocol —
    // `migraloop target` requires a live Pipeline Target Binding.
    let customers_target_before_sync = mongo_collection_dump(&mongo_database, "customers");
    assert!(
        customers_target_before_sync.contains("Alice")
            && customers_target_before_sync.contains("Bob"),
        "removed Pipeline Target retains last Delivered state, got:\n{customers_target_before_sync}"
    );

    let sync_out = run_sync(&url);
    assert!(
        !delivery_complete_for(&sync_out, "customers"),
        "removed Pipeline must not Deliver during sync, got:\n{sync_out}"
    );
    assert!(
        delivery_complete_for(&sync_out, "orders")
            || sync_out.to_ascii_lowercase().contains("orders"),
        "unaffected Pipeline orders must still Deliver during sync, got:\n{sync_out}"
    );

    // Removed Pipeline Target must not receive Incremental updates (Alice→Alicia, Bob delete).
    let customers_target = mongo_collection_dump(&mongo_database, "customers");
    assert!(
        customers_target.contains("Alice") && customers_target.contains("Bob"),
        "removed Pipeline Target must retain pre-remove Managed fields, got:\n{customers_target}"
    );
    assert!(
        !customers_target.contains("Alicia"),
        "removed Pipeline Target must not receive Incremental Managed updates, got:\n{customers_target}"
    );

    let orders_target = target_stdout(&url, "orders");
    assert!(
        orders_target.contains("50.00") || orders_target.contains("\"50\""),
        "unaffected Pipeline orders must Deliver Incremental updates, got:\n{orders_target}"
    );
}

#[tokio::test]
async fn remove_keeps_shared_base_for_remaining_pipeline() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.yaml",
        &two_pipelines_shared_customers(&mongo_database),
    );

    migrate_and_apply(&url, &config);

    let status_before = status(&url);
    assert!(
        status_has_pipeline(&status_before, "customers")
            && status_has_pipeline(&status_before, "customers_reporting"),
        "both Pipelines must be active before remove, got:\n{status_before}"
    );
    assert!(
        status_before.contains("Base Dataset: CUSTOMERS"),
        "shared CUSTOMERS Base must exist before remove, got:\n{status_before}"
    );

    remove_pipeline(&url, "customers");

    let status_after = status(&url);
    assert!(
        !status_has_pipeline(&status_after, "customers"),
        "removed Pipeline must leave status, got:\n{status_after}"
    );
    assert!(
        status_has_pipeline(&status_after, "customers_reporting"),
        "remaining Pipeline must stay active, got:\n{status_after}"
    );
    assert!(
        status_after.contains("Base Dataset: CUSTOMERS"),
        "Shared Base must remain for Pipelines still using it, got:\n{status_after}"
    );

    let base = base_stdout(&url, "CUSTOMERS");
    assert!(
        base.contains("Alice") || base.contains("Alicia"),
        "Shared Base rows must remain inspectable after remove, got:\n{base}"
    );

    let sync_out = run_sync(&url);
    assert!(
        !delivery_complete_for(&sync_out, "customers"),
        "removed Pipeline must not Deliver, got:\n{sync_out}"
    );
    assert!(
        delivery_complete_for(&sync_out, "customers_reporting")
            || sync_out.contains("customers_reporting"),
        "remaining Pipeline must still Deliver from Shared Base, got:\n{sync_out}"
    );

    let reporting = target_stdout(&url, "customers_reporting");
    assert!(
        reporting.contains("Alicia") || reporting.contains("Carol") || reporting.contains("Alice"),
        "remaining Pipeline must Deliver from Shared Base after remove, got:\n{reporting}"
    );
}
