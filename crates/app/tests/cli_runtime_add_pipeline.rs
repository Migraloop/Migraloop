//! Operator-visible seam: runtime add Pipeline with table-level Initial Load.
//!
//! Agreed seam (issue #14 / PRD / ADR-0007 / ADR-0019): while a Deployment is
//! running, apply a new Pipeline that needs a new Source table; only that table
//! gets table-level Initial Load; pre-existing Base Datasets stay on incremental
//! paths. No full Deployment process restart.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

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
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("appdb_{suffix}")
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

fn deployment_with_pipelines(pipelines_yaml: &str, mongo_database: &str) -> String {
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
{pipelines_yaml}
"#,
        host = mongo_host(),
        port = mongo_port(),
    )
}

fn customers_only_pipelines() -> &'static str {
    r#"    - name: customers
      mode: direct
      source:
        table: CUSTOMERS
      target:
        collection: customers
"#
}

fn customers_and_orders_pipelines() -> &'static str {
    r#"    - name: customers
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
"#
}

fn migrate_and_apply(url: &str, config: &Path) -> String {
    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", url])
        .output()
        .expect("run migrate");
    assert!(
        migrate.status.success(),
        "migrate failed: {}",
        String::from_utf8_lossy(&migrate.stderr)
    );

    apply(url, config)
}

fn apply(url: &str, config: &Path) -> String {
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
    String::from_utf8_lossy(&apply.stdout).into_owned()
}

fn run_sync(url: &str) {
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

fn start_run(url: &str) -> Child {
    Command::new(bin())
        .args(["run", "--platform-store-url", url])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn migraloop run")
}

fn process_alive(child: &Child) -> bool {
    // kill(pid, 0) via /proc is portable enough for Linux CI/agents.
    Path::new(&format!("/proc/{}", child.id())).exists()
}

fn base_followup_line(status_out: &str, table: &str, needle: &str) -> String {
    let marker = format!("Base Dataset: {table}");
    let mut lines = status_out.lines();
    while let Some(line) = lines.next() {
        if line.contains(&marker) {
            for follow in lines.by_ref() {
                if follow.contains(needle) {
                    return follow.to_string();
                }
                if follow.starts_with("Base Dataset:") {
                    break;
                }
            }
            break;
        }
    }
    panic!("missing {needle} for Base Dataset {table} in:\n{status_out}");
}

fn sync_health_line_for_table(status_out: &str, table: &str) -> String {
    base_followup_line(status_out, table, "Sync Health:")
}

fn cutover_line_for_table(status_out: &str, table: &str) -> String {
    base_followup_line(status_out, table, "Cutover:")
}

fn delivery_health_line_for_pipeline(status_out: &str, pipeline: &str) -> String {
    let needle = format!("Pipeline={pipeline}");
    for line in status_out.lines() {
        if line.contains("Delivery Health:") && line.contains(&needle) {
            return line.to_string();
        }
    }
    panic!("missing Delivery Health for Pipeline {pipeline} in:\n{status_out}");
}

#[tokio::test]
async fn runtime_add_pipeline_initial_loads_only_new_table_without_restart() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");

    // 1) Boot Deployment with one Direct Pipeline; Initial Load CUSTOMERS + Delivery.
    let first = write_config(
        &dir,
        "deployment-customers.yaml",
        &deployment_with_pipelines(customers_only_pipelines(), &mongo_database),
    );
    let first_apply = migrate_and_apply(&url, &first);
    assert!(
        first_apply.contains("Initial Load complete: Base Dataset CUSTOMERS"),
        "first apply must Initial Load CUSTOMERS, got:\n{first_apply}"
    );
    assert!(
        first_apply.contains("Delivery complete: Pipeline customers"),
        "first apply must Deliver customers, got:\n{first_apply}"
    );

    // 2) Move existing Base onto the incremental path (operator Sync).
    run_sync(&url);
    let before = status(&url);
    assert!(
        before.contains("Pipeline: customers"),
        "expected customers Pipeline before runtime add, got:\n{before}"
    );
    assert!(
        !before.contains("Pipeline: orders"),
        "orders Pipeline must not exist yet, got:\n{before}"
    );
    assert!(
        before.contains("Base Dataset: CUSTOMERS"),
        "expected CUSTOMERS Base before runtime add, got:\n{before}"
    );
    assert!(
        !before.contains("Base Dataset: ORDERS"),
        "ORDERS Base must not exist yet, got:\n{before}"
    );

    let customers_sync_before = sync_health_line_for_table(&before, "CUSTOMERS");
    let customers_cutover_before = cutover_line_for_table(&before, "CUSTOMERS");
    let customers_delivery_before = delivery_health_line_for_pipeline(&before, "customers");
    assert!(
        customers_sync_before.contains("appliedChanges=")
            && !customers_sync_before.contains("appliedChanges=0"),
        "CUSTOMERS must be on incremental path with applied changes, got:\n{customers_sync_before}"
    );
    assert!(
        customers_delivery_before.contains("status=delivered"),
        "customers Delivery must be delivered before runtime add, got:\n{customers_delivery_before}"
    );

    // 3) Keep Deployment process running across the Pipeline add (ADR-0007).
    let mut run = start_run(&url);
    // Give `run` a moment to finish migrate + announce readiness.
    std::thread::sleep(std::time::Duration::from_millis(300));
    assert!(
        process_alive(&run),
        "Deployment run process must stay up before runtime Pipeline add"
    );

    // 4) Runtime add: apply a new Pipeline that needs a new Source table.
    let second = write_config(
        &dir,
        "deployment-customers-orders.yaml",
        &deployment_with_pipelines(customers_and_orders_pipelines(), &mongo_database),
    );
    let second_apply = apply(&url, &second);

    assert!(
        process_alive(&run),
        "runtime Pipeline add must not require full Deployment restart; run process died"
    );
    let _ = run.kill();
    let _ = run.wait();

    // New table only: ORDERS Initial Load; CUSTOMERS must not reload.
    assert!(
        second_apply.contains("Runtime Pipeline add: orders (source=ORDERS)"),
        "expected operator-visible runtime Pipeline add, got:\n{second_apply}"
    );
    assert!(
        second_apply.contains("Initial Load complete: Base Dataset ORDERS"),
        "new table must receive table-level Initial Load, got:\n{second_apply}"
    );
    assert!(
        !second_apply.contains("Initial Load complete: Base Dataset CUSTOMERS"),
        "pre-existing Base must not be reloaded on runtime add, got:\n{second_apply}"
    );

    // Existing Pipeline keeps running: do not re-Deliver it on runtime add.
    assert!(
        !second_apply.contains("Delivery complete: Pipeline customers"),
        "runtime add must not re-Deliver pre-existing Pipelines, got:\n{second_apply}"
    );
    assert!(
        second_apply.contains("Delivery complete: Pipeline orders"),
        "newly added Pipeline must start Delivery, got:\n{second_apply}"
    );

    let after = status(&url);
    assert!(
        after.contains("Pipeline: customers") && after.contains("Pipeline: orders"),
        "both Pipelines must be present after runtime add, got:\n{after}"
    );
    assert!(
        after.contains("Base Dataset: ORDERS")
            && (after.contains("initial_load_complete")
                || after.to_lowercase().contains("initial load complete")),
        "ORDERS Base must show Initial Load complete, got:\n{after}"
    );

    let customers_sync_after = sync_health_line_for_table(&after, "CUSTOMERS");
    let customers_cutover_after = cutover_line_for_table(&after, "CUSTOMERS");
    let customers_delivery_after = delivery_health_line_for_pipeline(&after, "customers");
    assert_eq!(
        customers_sync_before, customers_sync_after,
        "pre-existing CUSTOMERS Base must stay on incremental path (Sync Health unchanged)"
    );
    assert_eq!(
        customers_cutover_before, customers_cutover_after,
        "pre-existing CUSTOMERS Base cutover/checkpoint must be preserved (not reloaded)"
    );
    assert_eq!(
        customers_delivery_before, customers_delivery_after,
        "pre-existing customers Pipeline Delivery progress must be preserved (not restarted)"
    );

    // ORDERS rows present; CUSTOMERS fixture rows still present (not wiped).
    let orders = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", "ORDERS"])
        .output()
        .expect("base ORDERS");
    assert!(
        orders.status.success(),
        "ORDERS Base inspect failed: {}",
        String::from_utf8_lossy(&orders.stderr)
    );
    let orders_out = String::from_utf8_lossy(&orders.stdout);
    assert!(
        orders_out.contains("ORDER_ID") && orders_out.contains("42.50"),
        "expected ORDERS Initial Load rows, got:\n{orders_out}"
    );

    let customers = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", "CUSTOMERS"])
        .output()
        .expect("base CUSTOMERS");
    assert!(customers.status.success());
    let customers_out = String::from_utf8_lossy(&customers.stdout);
    assert!(
        customers_out.contains("Alicia") || customers_out.contains("Alice"),
        "pre-existing CUSTOMERS Base rows must remain after runtime add, got:\n{customers_out}"
    );
}
