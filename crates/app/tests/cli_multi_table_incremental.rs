//! Operator-visible seam: multi-table Direct + Transform settle (issue #96).
//!
//! Agreed seam: CLI apply/sync + Base/Derived/Target outcomes.
//! Lab Scenario `concurrent-source-workload` exercises the same multi-table shape
//! (customers Direct + orders groupBy/sum) under parallel Source sessions on real
//! engines. This non-ignored contract/stub twin covers the gateable correctness slice:
//! both tables receive Incremental Capture changes in one sync cycle and settle to
//! correct Managed Target / Derived outcomes. True OS-level parallel sqlplus stays Lab.
//!
//! Also strengthens Lab `transform-pipeline` / groupBy-under-contention twin evidence
//! beyond single-table Affect Analysis (`cli_groupby_sum_affect`).

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

/// Same multi-table Pipeline shape as Lab `transform-pipeline` / `concurrent-source-workload`.
fn multi_table_deployment(mongo_database: &str) -> String {
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
    - name: order-totals
      mode: transform
      source:
        table: ORDERS
      target:
        collection: order_totals
      outputIdentity: [CUSTOMER_ID]
      transform:
        - groupBy:
            keys: [CUSTOMER_ID]
            aggregates:
              - op: sum
                field: AMOUNT
                as: TOTAL_AMOUNT
"#,
        host = mongo_host(),
        port = mongo_port(),
    )
}

fn migrate_and_apply(url: &str, config: &Path, doubles: &common::NamedScenarioDoubles) -> String {
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
    String::from_utf8_lossy(&apply.stdout).into_owned()
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

fn inspect_base(url: &str, table: &str) -> String {
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

fn inspect_derived(url: &str, pipeline: &str) -> String {
    let derived = Command::new(bin())
        .args([
            "derived",
            "--platform-store-url",
            url,
            "--pipeline",
            pipeline,
        ])
        .output()
        .expect("run derived");
    assert!(
        derived.status.success(),
        "derived inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&derived.stdout),
        String::from_utf8_lossy(&derived.stderr)
    );
    String::from_utf8_lossy(&derived.stdout).into_owned()
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
        "target inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&target.stdout),
        String::from_utf8_lossy(&target.stderr)
    );
    String::from_utf8_lossy(&target.stdout).into_owned()
}

#[tokio::test]
async fn multi_table_customers_and_orders_incremental_settle_to_correct_outcomes() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &multi_table_deployment(&mongo_database),
    );

    let apply_out = migrate_and_apply(&url, &config, &doubles);
    assert!(
        apply_out.contains("Initial Load complete: Base Dataset CUSTOMERS"),
        "multi-table apply must Initial Load CUSTOMERS, got:\n{apply_out}"
    );
    assert!(
        apply_out.contains("Initial Load complete: Base Dataset ORDERS"),
        "multi-table apply must Initial Load ORDERS, got:\n{apply_out}"
    );

    // Initial Derived: customer 1 = 52.50, customer 2 = 5.00 (stub ORDERS fixture).
    let derived_initial = inspect_derived(&url, "order-totals");
    assert!(
        derived_initial.contains("52.50") && derived_initial.contains("5.00"),
        "Initial groupBy/sum Derived must materialize both customer totals, got:\n{derived_initial}"
    );

    let sync_out = run_sync(&url, &doubles);
    assert!(
        sync_out.to_ascii_lowercase().contains("incremental")
            || sync_out.to_ascii_lowercase().contains("sync"),
        "sync should report Incremental Capture for both Bases, got:\n{sync_out}"
    );

    // CUSTOMERS stub batch: Alice→Alicia, insert Carol, delete Bob.
    let customers_base = inspect_base(&url, "CUSTOMERS");
    assert!(
        customers_base.contains("Alicia") && customers_base.contains("Carol"),
        "CUSTOMERS Base must reflect stub incremental I/U/D, got:\n{customers_base}"
    );
    assert!(
        !customers_base.contains("Bob"),
        "CUSTOMERS Base must drop deleted Bob, got:\n{customers_base}"
    );

    let customers_target = inspect_target(&url, "customers");
    assert!(
        customers_target.contains("Alicia") && customers_target.contains("Carol"),
        "customers Target must Deliver Managed Alicia+Carol, got:\n{customers_target}"
    );
    assert!(
        !customers_target.contains("Bob"),
        "customers Target must delete Bob document, got:\n{customers_target}"
    );

    // ORDERS stub batch: ADDRESS-only, AMOUNT 42.50→50.00 (customer 1 → 60.00),
    // then CUSTOMER_ID group-key move order 200: customer 2 → 3 (sum 5.00).
    let derived_after = inspect_derived(&url, "order-totals");
    assert!(
        derived_after.contains("60.00"),
        "order-totals Derived must settle to customer 1 sum 60.00 after multi-table sync, got:\n{derived_after}"
    );
    assert!(
        derived_after.contains("\"CUSTOMER_ID\": 3") && derived_after.contains("5.00"),
        "group-key move must settle customer 3 sum 5.00, got:\n{derived_after}"
    );
    assert!(
        !derived_after.contains("\"CUSTOMER_ID\": 2"),
        "old identity customer 2 must be gone after group-key move, got:\n{derived_after}"
    );
    assert!(
        !derived_after.contains("52.50"),
        "stale customer 1 sum 52.50 must be gone after settle, got:\n{derived_after}"
    );

    let totals_target = inspect_target(&url, "order_totals");
    assert!(
        totals_target.contains("60.00") || totals_target.contains("60"),
        "order_totals Target must Deliver settled TOTAL_AMOUNT for customer 1, got:\n{totals_target}"
    );
    assert!(
        totals_target.contains("\"_id\": 3"),
        "order_totals Target must upsert new Output Identity 3, got:\n{totals_target}"
    );
    assert!(
        !totals_target.contains("\"_id\": 2"),
        "order_totals Target must delete old Output Identity 2, got:\n{totals_target}"
    );
}
