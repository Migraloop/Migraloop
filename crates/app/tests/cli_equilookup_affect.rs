//! Operator-visible seam: Rich Transform equiLookup multi-Base (issue #127).
//!
//! Agreed seams: CLI config apply → Initial Load of primary + equiLookup.from Bases;
//! Derived join array + Mongo Delivery by Output Identity; Affect Analysis on both
//! Base sides; `$lookup` / free-form scripts fail apply clearly.

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use migraloop_capture::{LogMinerContent, LogMinerOperation};
use serde_json::{json, Value};
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

fn deployment_shell(mongo_database: &str, pipeline_yaml: &str) -> String {
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
{pipeline_yaml}
"#,
        host = mongo_host(),
        port = mongo_port(),
    )
}

fn equi_lookup_pipeline() -> &'static str {
    r#"
    - name: customers-with-orders
      mode: transform
      source:
        table: CUSTOMERS
      target:
        collection: customers_with_orders
      outputIdentity: [ID]
      transform:
        - project:
            fields: [ID, NAME]
        - equiLookup:
            from: ORDERS
            localField: ID
            foreignField: CUSTOMER_ID
            as: orders
"#
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
    let mut out = String::from_utf8_lossy(&apply.stdout).into_owned();
    out.push_str(&String::from_utf8_lossy(&apply.stderr));
    out
}

fn apply_expect_failure(url: &str, config: &Path, doubles: &common::NamedScenarioDoubles) -> String {
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
        !apply.status.success(),
        "apply should fail, but succeeded: stdout={}",
        String::from_utf8_lossy(&apply.stdout)
    );
    let mut combined = String::from_utf8_lossy(&apply.stderr).into_owned();
    combined.push_str(&String::from_utf8_lossy(&apply.stdout));
    combined
}

fn run_sync_fail_after(url: &str, after: u32, doubles: &common::NamedScenarioDoubles) -> String {
    let mut cmd = Command::new(bin());
    cmd.env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .env("MIGRALOOP_SYNC_FAIL_AFTER_CHANGES", after.to_string());
    doubles.apply_env(&mut cmd);
    let out = cmd
        .args(["sync", "--platform-store-url", url])
        .output()
        .expect("run sync with fail-after");
    let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    text
}

fn derived_stdout(url: &str, pipeline: &str) -> String {
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

fn json_num(n: i64) -> Value {
    json!(n)
}

fn json_str(s: &str) -> Value {
    json!(s)
}

fn row(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

#[tokio::test]
async fn equilookup_materializes_multi_base_derived_and_delivers() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_shell(&mongo_database, equi_lookup_pipeline()),
    );

    let apply_out = migrate_and_apply(&url, &config, &doubles);
    assert!(
        apply_out.contains("Initial Load complete: Base Dataset CUSTOMERS")
            && apply_out.contains("Initial Load complete: Base Dataset ORDERS"),
        "equiLookup must Initial Load both Bases, got:\n{apply_out}"
    );

    let customers_base = base_stdout(&url, "CUSTOMERS");
    let orders_base = base_stdout(&url, "ORDERS");
    assert!(
        customers_base.contains("Alice") && orders_base.contains("42.50"),
        "both Bases must be materialized, customers=\n{customers_base}\norders=\n{orders_base}"
    );

    let derived = derived_stdout(&url, "customers-with-orders");
    assert!(
        derived.contains("Alice") && derived.contains("orders") && derived.contains("42.50"),
        "Derived must embed matching ORDERS under orders, got:\n{derived}"
    );
    assert!(
        !derived.contains("alice@example.com") && !derived.contains("\"EMAIL\""),
        "project must drop EMAIL from Derived, got:\n{derived}"
    );

    let target = target_stdout(&url, "customers_with_orders");
    assert!(
        (target.contains("\"_id\": 1") || target.contains("\"_id\":1"))
            && target.contains("orders")
            && target.contains("42.50"),
        "Mongo Delivery must upsert by Output Identity with orders array, got:\n{target}"
    );
}

#[tokio::test]
async fn equilookup_affect_analysis_updates_on_either_base_side() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    // EMAIL-only primary update (unused after project) then stock ORDERS incremental.
    let extra = vec![LogMinerContent {
        scn: 1040,
        operation: LogMinerOperation::Update,
        seg_owner: "APP".to_string(),
        table_name: "CUSTOMERS".to_string(),
        identity: row(&[("ID", json_num(1))]),
        after_image: Some(row(&[
            ("ID", json_num(1)),
            ("NAME", json_str("Alice")),
            ("EMAIL", json_str("only-email@example.com")),
            ("ACTIVE", json_num(1)),
            ("BIO", json_str("blob-bytes-alice")),
        ])),
    }];
    let doubles = common::NamedScenarioDoubles::install_with_extra_logminer(dir.path(), &extra);
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_shell(&mongo_database, equi_lookup_pipeline()),
    );

    migrate_and_apply(&url, &config, &doubles);

    // CUSTOMERS table is processed first (BTreeSet). fail_after=1 → EMAIL-only skip.
    let unused_out = run_sync_fail_after(&url, 1, &doubles);
    assert!(
        unused_out.to_ascii_lowercase().contains("skip")
            || unused_out.to_ascii_lowercase().contains("unused"),
        "EMAIL-only primary update must Affect-Analysis skip, got:\n{unused_out}"
    );
    let derived_after_skip = derived_stdout(&url, "customers-with-orders");
    assert!(
        derived_after_skip.contains("Alice") && !derived_after_skip.contains("only-email"),
        "unused EMAIL must not alter Derived, got:\n{derived_after_skip}"
    );

    // Next CUSTOMERS change: Alice→Alicia (NAME used) — recompute.
    let name_out = run_sync_fail_after(&url, 1, &doubles);
    let name_lower = name_out.to_ascii_lowercase();
    assert!(
        name_lower.contains("affect")
            && (name_lower.contains("recomput")
                || name_lower.contains("affected")
                || name_lower.contains("delivery complete")),
        "NAME update must recompute primary side, got:\n{name_out}"
    );
    let derived_name = derived_stdout(&url, "customers-with-orders");
    assert!(
        derived_name.contains("Alicia"),
        "primary NAME change must update Derived, got:\n{derived_name}"
    );

    // Drain remaining CUSTOMERS changes (Carol insert, Bob delete) so ORDERS sync starts.
    let _ = run_sync_fail_after(&url, 2, &doubles);

    // ORDERS ADDRESS-only (SCN 510): foreign embed → recompute customer 1.
    let foreign_out = run_sync_fail_after(&url, 1, &doubles);
    let foreign_lower = foreign_out.to_ascii_lowercase();
    assert!(
        foreign_lower.contains("affect")
            && (foreign_lower.contains("recomput")
                || foreign_lower.contains("affected")
                || foreign_lower.contains("delivery complete")),
        "foreign ORDERS change must Affect Analysis recompute, got:\n{foreign_out}"
    );
    let derived_foreign = derived_stdout(&url, "customers-with-orders");
    assert!(
        derived_foreign.contains("Main Ave") || derived_foreign.contains("1 Main Ave"),
        "foreign ADDRESS change must appear in embedded orders, got:\n{derived_foreign}"
    );

    // ORDERS AMOUNT 42.50→50.00 (SCN 520).
    let amount_out = run_sync_fail_after(&url, 1, &doubles);
    assert!(
        amount_out.to_ascii_lowercase().contains("affect")
            || amount_out.to_ascii_lowercase().contains("delivery"),
        "foreign AMOUNT change must maintain Derived, got:\n{amount_out}"
    );
    let derived_amount = derived_stdout(&url, "customers-with-orders");
    assert!(
        derived_amount.contains("50.00") || derived_amount.contains("50"),
        "foreign AMOUNT must update embedded orders, got:\n{derived_amount}"
    );
    assert!(
        !derived_amount.contains("42.50"),
        "stale order amount must not remain, got:\n{derived_amount}"
    );

    let target = target_stdout(&url, "customers_with_orders");
    assert!(
        target.contains("Alicia") && (target.contains("50.00") || target.contains("50")),
        "Mongo Delivery must reflect both Base sides, got:\n{target}"
    );
}

#[tokio::test]
async fn equilookup_rejects_dollar_lookup_and_scripts_on_apply() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());

    let lookup_pipeline = r#"
    - name: bad-lookup
      mode: transform
      source:
        table: CUSTOMERS
      target:
        collection: bad_lookup
      outputIdentity: [ID]
      transform:
        - $lookup:
            from: ORDERS
            localField: ID
            foreignField: CUSTOMER_ID
            as: orders
"#;
    let lookup_config = write_config(
        &dir,
        "lookup.yaml",
        &deployment_shell(&mongo_database, lookup_pipeline),
    );
    let err = apply_expect_failure(&url, &lookup_config, &doubles);
    let lower = err.to_ascii_lowercase();
    assert!(
        lower.contains("$lookup") && lower.contains("equilookup"),
        "expected clear $lookup → equiLookup guidance, got:\n{err}"
    );

    let url2 = ephemeral_database_url().await;
    let mongo2 = unique_mongo_database();
    let dir2 = TempDir::new().expect("tempdir");
    let doubles2 = common::NamedScenarioDoubles::install(dir2.path());
    let script_pipeline = r#"
    - name: scripted
      mode: transform
      source:
        table: CUSTOMERS
      target:
        collection: scripted
      outputIdentity: [ID]
      transform:
        - equiLookup:
            from: ORDERS
            localField: ID
            foreignField: CUSTOMER_ID
            as: orders
        - script: "return true"
"#;
    let script_config = write_config(
        &dir2,
        "script.yaml",
        &deployment_shell(&mongo2, script_pipeline),
    );
    let err = apply_expect_failure(&url2, &script_config, &doubles2);
    let lower = err.to_ascii_lowercase();
    assert!(
        lower.contains("script") || lower.contains("free-form"),
        "expected clear free-form script failure, got:\n{err}"
    );
}
