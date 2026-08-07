//! Operator-visible seam: pause and resume Pipeline (issue #19 / ADR-0007).
//!
//! Agreed seam: CLI `pause` / `resume` / `sync` / `status` / `target` + Platform
//! Store durable pause state. Pause stops further Delivery/processing for that
//! Pipeline without restarting the Deployment; resume continues from durable
//! Base state (catch-up Delivery). Other Pipelines keep Delivering.
//!
//! This is the non-ignored contract/stub CI twin of Lab Scenario `pause-resume`.
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

fn transform_and_direct_pipelines(mongo_database: &str) -> String {
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
    - name: active_customers
      mode: transform
      source:
        table: CUSTOMERS
      target:
        collection: active_customers
      outputIdentity: [ID]
      transform:
        - $project:
            fields: [ID, NAME, ACTIVE]
        - $match:
            ACTIVE: 1
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

fn run_sync(url: &str, doubles: &common::NamedScenarioDoubles) -> String {
    let mut sync = Command::new(bin());
    sync
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value");
    doubles.apply_env(&mut sync);
    let sync = sync
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

fn pause_pipeline(url: &str, pipeline: &str) -> String {
    let out = Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "pause",
            "--platform-store-url",
            url,
            "--pipeline",
            pipeline,
        ])
        .output()
        .expect("run pause");
    assert!(
        out.status.success(),
        "pause failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn resume_pipeline(
    url: &str,
    pipeline: &str,
    doubles: &common::NamedScenarioDoubles,
) -> String {
    // Resume catch-up Delivery rediscovers Source columns for Managed validation.
    let mut out = Command::new(bin());
    out.env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value");
    doubles.apply_env(&mut out);
    let out = out
        .args([
            "resume",
            "--platform-store-url",
            url,
            "--pipeline",
            pipeline,
        ])
        .output()
        .expect("run resume");
    assert!(
        out.status.success(),
        "resume failed: stdout={} stderr={}",
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

#[tokio::test]
async fn pause_stops_pipeline_delivery_resume_catch_up_other_pipelines_unaffected() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &two_direct_pipelines(&mongo_database),
    );

    migrate_and_apply(&url, &config, &doubles);

    let pause_out = pause_pipeline(&url, "customers");
    assert!(
        pause_out.to_ascii_lowercase().contains("paused")
            && pause_out.contains("customers"),
        "pause must report paused Pipeline customers, got:\n{pause_out}"
    );

    let status_paused = status(&url);
    assert!(
        status_paused.contains("customers")
            && status_paused.to_ascii_lowercase().contains("paused"),
        "status must show customers paused, got:\n{status_paused}"
    );
    assert!(
        status_paused.contains("orders"),
        "status must still list unaffected Pipeline orders, got:\n{status_paused}"
    );

    let sync_out = run_sync(&url, &doubles);
    assert!(
        !sync_out.contains("Delivery complete: Pipeline customers"),
        "paused Pipeline must not Deliver during sync, got:\n{sync_out}"
    );
    assert!(
        sync_out.contains("Delivery complete: Pipeline orders")
            || sync_out.to_ascii_lowercase().contains("orders"),
        "unaffected Pipeline orders must still Deliver during sync, got:\n{sync_out}"
    );

    // Base keeps durable Incremental Capture for the paused Pipeline's table.
    let customers_base = base_stdout(&url, "CUSTOMERS");
    assert!(
        customers_base.contains("Alicia")
            && customers_base.contains("Carol")
            && !customers_base.contains("Bob"),
        "Base must continue Incremental Capture while Pipeline is paused, got:\n{customers_base}"
    );

    // Target for paused Pipeline must still reflect pre-pause Delivery (Initial Load):
    // Alice/Bob/Carol snapshot — not the Incremental Alice→Alicia update or Bob delete.
    let customers_target_paused = target_stdout(&url, "customers");
    assert!(
        customers_target_paused.contains("Alice") && customers_target_paused.contains("Bob"),
        "paused Pipeline Target must retain Initial Load Managed fields, got:\n{customers_target_paused}"
    );
    assert!(
        !customers_target_paused.contains("Alicia"),
        "paused Pipeline Target must not receive Incremental Managed updates, got:\n{customers_target_paused}"
    );

    // Other Pipeline Target advanced.
    let orders_target = target_stdout(&url, "orders");
    assert!(
        orders_target.contains("50.00") || orders_target.contains("\"50\""),
        "unaffected Pipeline orders must Deliver Incremental updates, got:\n{orders_target}"
    );

    let resume_out = resume_pipeline(&url, "customers", &doubles);
    assert!(
        resume_out.to_ascii_lowercase().contains("resum")
            && resume_out.contains("customers"),
        "resume must report resumed Pipeline customers, got:\n{resume_out}"
    );
    assert!(
        resume_out.to_ascii_lowercase().contains("delivery"),
        "resume must catch up Delivery from durable Base state, got:\n{resume_out}"
    );

    let customers_target = target_stdout(&url, "customers");
    assert!(
        customers_target.contains("Alicia"),
        "resume catch-up must upsert Managed update from durable Base, got:\n{customers_target}"
    );
    assert!(
        customers_target.contains("Carol"),
        "resume catch-up must insert new identity from durable Base, got:\n{customers_target}"
    );
    assert!(
        !customers_target.contains("Bob")
            && !(customers_target.contains("\"_id\": 2")
                || customers_target.contains("\"_id\":2")),
        "resume catch-up must delete disappeared identity from durable Base, got:\n{customers_target}"
    );

    let status_resumed = status(&url);
    let lower = status_resumed.to_ascii_lowercase();
    assert!(
        status_resumed.contains("customers")
            && !lower
                .lines()
                .any(|line| line.contains("customers") && line.contains("paused")),
        "status must not keep customers paused after resume, got:\n{status_resumed}"
    );
}

#[tokio::test]
async fn pause_stops_transform_processing_resume_rebuilds_from_durable_base() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &transform_and_direct_pipelines(&mongo_database),
    );

    migrate_and_apply(&url, &config, &doubles);
    pause_pipeline(&url, "active_customers");

    let sync_out = run_sync(&url, &doubles);
    assert!(
        !sync_out.contains("Delivery complete: Pipeline active_customers")
            && !sync_out.contains("Affect Analysis: Pipeline active_customers"),
        "paused Transform Pipeline must skip Delivery/processing, got:\n{sync_out}"
    );
    assert!(
        sync_out.contains("Delivery complete: Pipeline orders")
            || sync_out.to_ascii_lowercase().contains("orders"),
        "unaffected Direct Pipeline must still Deliver, got:\n{sync_out}"
    );

    let target_paused = target_stdout(&url, "active_customers");
    assert!(
        target_paused.contains("Alice") && !target_paused.contains("Alicia"),
        "paused Transform Target must retain pre-pause Derived Delivery, got:\n{target_paused}"
    );

    let resume_out = resume_pipeline(&url, "active_customers", &doubles);
    assert!(
        resume_out.to_ascii_lowercase().contains("delivery")
            || resume_out.to_ascii_lowercase().contains("derived"),
        "resume must catch up Transform Delivery from durable Base, got:\n{resume_out}"
    );

    let target = target_stdout(&url, "active_customers");
    assert!(
        target.contains("Alicia") && target.contains("Carol") && !target.contains("Bob"),
        "resume must rebuild Derived/Delivery from durable Base, got:\n{target}"
    );
}
