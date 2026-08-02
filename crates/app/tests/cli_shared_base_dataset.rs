//! Operator-visible seam: shared Base Dataset across Pipelines.
//!
//! Agreed seam (issue #15 / PRD / ADR-0019 / ADR-0007): two Pipelines that
//! reference the same Source table share one Base Dataset (single Sync), not
//! per-Pipeline copies. Status/store show a single Base; both Pipelines Deliver
//! from that shared Base. Verified via CLI config/status/target — not private
//! module internals.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
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

fn one_customers_pipeline() -> &'static str {
    r#"    - name: customers
      mode: direct
      source:
        table: CUSTOMERS
      target:
        collection: customers
"#
}

fn two_pipelines_same_customers_table() -> &'static str {
    r#"    - name: customers
      mode: direct
      source:
        table: CUSTOMERS
      target:
        collection: customers
    - name: customers_mirror
      mode: direct
      source:
        table: CUSTOMERS
      target:
        collection: customers_mirror
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

fn count_base_dataset_lines(status_out: &str, table: &str) -> usize {
    let marker = format!("Base Dataset: {table}");
    status_out
        .lines()
        .filter(|line| line.contains(&marker))
        .count()
}

fn inspect_target(url: &str, collection: &str) -> String {
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
        "target inspect failed for {collection}: stdout={} stderr={}",
        String::from_utf8_lossy(&target.stdout),
        String::from_utf8_lossy(&target.stderr)
    );
    String::from_utf8_lossy(&target.stdout).into_owned()
}

fn assert_customers_delivered(collection_out: &str, collection: &str) {
    assert!(
        collection_out.contains("\"_id\": 1") || collection_out.contains("\"_id\":1"),
        "{collection} must Deliver Output Identity _id=1 from shared Base, got:\n{collection_out}"
    );
    assert!(
        collection_out.contains("Alice") && collection_out.contains("alice@example.com"),
        "{collection} must Deliver Managed fields from shared CUSTOMERS Base, got:\n{collection_out}"
    );
}

/// Match `Delivery complete: Pipeline <name>` without treating `customers` as a
/// prefix of `customers_mirror`.
fn delivery_complete_for(stdout: &str, pipeline: &str) -> bool {
    let needle = format!("Delivery complete: Pipeline {pipeline} ");
    stdout.contains(&needle)
        || stdout.lines().any(|line| {
            line.trim_end() == format!("Delivery complete: Pipeline {pipeline}")
        })
}

/// Match `Pipeline: <name> (` in status without prefix collisions.
fn status_has_pipeline(status_out: &str, pipeline: &str) -> bool {
    status_out.contains(&format!("Pipeline: {pipeline} ("))
}

/// Both Pipelines applied together: one Initial Load, one Base, both Deliver.
#[tokio::test]
async fn two_pipelines_same_table_share_one_base_and_both_deliver() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment-shared-base.yaml",
        &deployment_with_pipelines(two_pipelines_same_customers_table(), &mongo_database),
    );

    let apply_out = migrate_and_apply(&url, &config);

    let il_count = apply_out
        .matches("Initial Load complete: Base Dataset CUSTOMERS")
        .count();
    assert_eq!(
        il_count, 1,
        "same Source table must Initial Load once (shared Base), got:\n{apply_out}"
    );
    assert!(
        delivery_complete_for(&apply_out, "customers"),
        "first Pipeline must Deliver from shared Base, got:\n{apply_out}"
    );
    assert!(
        delivery_complete_for(&apply_out, "customers_mirror"),
        "second Pipeline must Deliver from shared Base, got:\n{apply_out}"
    );

    let status_out = status(&url);
    assert!(
        status_has_pipeline(&status_out, "customers")
            && status_has_pipeline(&status_out, "customers_mirror"),
        "status must list both Pipelines, got:\n{status_out}"
    );
    assert_eq!(
        count_base_dataset_lines(&status_out, "CUSTOMERS"),
        1,
        "status/store must show a single Base Dataset for CUSTOMERS, got:\n{status_out}"
    );
    assert!(
        !status_out.contains("Base Dataset: ORDERS"),
        "no other Base Datasets expected, got:\n{status_out}"
    );

    let customers = inspect_target(&url, "customers");
    let mirror = inspect_target(&url, "customers_mirror");
    assert_customers_delivered(&customers, "customers");
    assert_customers_delivered(&mirror, "customers_mirror");
}

/// Runtime-add second Pipeline on an already-loaded table: reuse Base, no reload.
#[tokio::test]
async fn runtime_add_second_pipeline_reuses_existing_base_for_same_table() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");

    let first = write_config(
        &dir,
        "deployment-one.yaml",
        &deployment_with_pipelines(one_customers_pipeline(), &mongo_database),
    );
    let first_apply = migrate_and_apply(&url, &first);
    assert!(
        first_apply.contains("Initial Load complete: Base Dataset CUSTOMERS"),
        "first apply must Initial Load CUSTOMERS, got:\n{first_apply}"
    );
    assert!(
        delivery_complete_for(&first_apply, "customers"),
        "first apply must Deliver customers, got:\n{first_apply}"
    );

    let before = status(&url);
    assert_eq!(
        count_base_dataset_lines(&before, "CUSTOMERS"),
        1,
        "exactly one CUSTOMERS Base before runtime add, got:\n{before}"
    );
    assert!(
        !status_has_pipeline(&before, "customers_mirror"),
        "mirror Pipeline must not exist yet, got:\n{before}"
    );

    let second = write_config(
        &dir,
        "deployment-two.yaml",
        &deployment_with_pipelines(two_pipelines_same_customers_table(), &mongo_database),
    );
    let second_apply = apply(&url, &second);

    assert!(
        second_apply.contains("Runtime Pipeline add: customers_mirror (source=CUSTOMERS)"),
        "expected runtime add of second Pipeline on shared table, got:\n{second_apply}"
    );
    assert!(
        !second_apply.contains("Initial Load complete: Base Dataset CUSTOMERS"),
        "second Pipeline must reuse existing Base (no Initial Load), got:\n{second_apply}"
    );
    assert!(
        !delivery_complete_for(&second_apply, "customers"),
        "unchanged already-delivered Pipeline must not re-Deliver, got:\n{second_apply}"
    );
    assert!(
        delivery_complete_for(&second_apply, "customers_mirror"),
        "new Pipeline must Deliver from the shared Base, got:\n{second_apply}"
    );

    let after = status(&url);
    assert!(
        status_has_pipeline(&after, "customers")
            && status_has_pipeline(&after, "customers_mirror"),
        "both Pipelines must be present after runtime add, got:\n{after}"
    );
    assert_eq!(
        count_base_dataset_lines(&after, "CUSTOMERS"),
        1,
        "status/store must still show a single Base Dataset for CUSTOMERS, got:\n{after}"
    );

    let customers = inspect_target(&url, "customers");
    let mirror = inspect_target(&url, "customers_mirror");
    assert_customers_delivered(&customers, "customers");
    assert_customers_delivered(&mirror, "customers_mirror");
}
