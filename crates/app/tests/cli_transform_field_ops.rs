//! Operator-visible seam: Rich Transform addFields / rename / remove (issue #125).
//!
//! Agreed seams: CLI config apply → Derived Dataset + Mongo Delivery by Output Identity;
//! Affect Analysis skips unused removed fields; free-form scripts / unsupported ops still fail.

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
        .env("MONGO_PASSWORD", "mongo-secret-value");
    doubles.apply_env(&mut cmd);
    cmd.args(["sync", "--platform-store-url", url]);
    common::SyncCliOptions::fail_after(after).append_to(&mut cmd);
    let out = cmd.output().expect("run sync with fail-after");
    let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    text
}


fn delivery_applied_changes(status_out: &str, pipeline: &str) -> Option<i32> {
    for line in status_out.lines() {
        if line.contains("Delivery Health:") && line.contains(&format!("Pipeline={pipeline}")) {
            if let Some(idx) = line.find("appliedChanges=") {
                let rest = &line[idx + "appliedChanges=".len()..];
                let digits: String = rest
                    .chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect();
                return digits.parse().ok();
            }
        }
    }
    None
}

fn status_stdout(url: &str) -> String {
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
async fn transform_field_ops_materialize_derived_and_deliver_to_mongo() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let pipeline = r#"
    - name: shaped-customers
      mode: transform
      source:
        table: CUSTOMERS
      target:
        collection: shaped_customers
      outputIdentity: [ID]
      transform:
        - $project:
            ID: 1
            NAME: 1
            EMAIL: 1
            ACTIVE: 1
        - $unset: EMAIL
        - $rename:
            NAME: customerName
        - $addFields:
            source: oracle
            displayName: "$customerName"
        - $match:
            ACTIVE: 1
"#;
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_shell(&mongo_database, pipeline),
    );

    migrate_and_apply(&url, &config, &doubles);

    let derived = derived_stdout(&url, "shaped-customers");
    assert!(
        derived.contains("Alice") && derived.contains("Carol"),
        "Derived must include ACTIVE=1 rows after field ops, got:\n{derived}"
    );
    assert!(
        !derived.contains("Bob"),
        "Bob ACTIVE=0 must be filtered out, got:\n{derived}"
    );
    assert!(
        derived.contains("customerName") && derived.contains("displayName"),
        "rename + addFields copy must appear in Derived, got:\n{derived}"
    );
    assert!(
        derived.contains("oracle"),
        "addFields literal source=oracle must appear, got:\n{derived}"
    );
    assert!(
        !derived.contains("alice@example.com") && !derived.contains("\"EMAIL\""),
        "remove must drop EMAIL from Derived, got:\n{derived}"
    );

    let target = target_stdout(&url, "shaped_customers");
    assert!(
        (target.contains("\"_id\": 1") || target.contains("\"_id\":1"))
            && (target.contains("\"_id\": 3") || target.contains("\"_id\":3")),
        "Output Identity Delivery expected for Alice/Carol, got:\n{target}"
    );
    assert!(
        target.contains("customerName") && target.contains("displayName") && target.contains("oracle"),
        "Mongo Delivery must reflect rename/addFields Managed fields, got:\n{target}"
    );
    assert!(
        !target.contains("alice@example.com"),
        "EMAIL must not be Delivered after remove, got:\n{target}"
    );
}

#[tokio::test]
async fn transform_remove_affect_analysis_skips_unused_address_only_update() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    // Extra ADDRESS-only then AMOUNT updates (ORDERS fixture already has these at 510/520).
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let pipeline = r#"
    - name: order-shaped
      mode: transform
      source:
        table: ORDERS
      target:
        collection: order_shaped
      outputIdentity: [ORDER_ID]
      transform:
        - $unset: ADDRESS
        - $rename:
            AMOUNT: orderAmount
        - $addFields:
            currency: USD
"#;
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_shell(&mongo_database, pipeline),
    );

    migrate_and_apply(&url, &config, &doubles);

    let derived_initial = derived_stdout(&url, "order-shaped");
    assert!(
        derived_initial.contains("orderAmount") && derived_initial.contains("USD"),
        "Initial Derived must show rename/addFields, got:\n{derived_initial}"
    );
    assert!(
        !derived_initial.contains("ADDRESS") && !derived_initial.contains("Main St"),
        "remove must drop ADDRESS from Derived, got:\n{derived_initial}"
    );

    let status_initial = status_stdout(&url);
    let applied_initial = delivery_applied_changes(&status_initial, "order-shaped")
        .expect("Delivery Health appliedChanges after Initial Load");

    // SCN 510: ADDRESS-only update — unused after remove.
    let unused_out = run_sync_fail_after(&url, 1, &doubles);
    assert!(
        unused_out.to_ascii_lowercase().contains("affect")
            && (unused_out.to_ascii_lowercase().contains("skip")
                || unused_out.to_ascii_lowercase().contains("unused")),
        "ADDRESS-only update must Affect-Analysis skip after remove, got:\n{unused_out}"
    );
    let applied_after_unused = delivery_applied_changes(&status_stdout(&url), "order-shaped")
        .expect("Delivery Health after unused change");
    assert_eq!(
        applied_after_unused, applied_initial,
        "unused ADDRESS must not Deliver; before={applied_initial} after={applied_after_unused}"
    );

    // SCN 520: AMOUNT update — used via rename → orderAmount.
    let used_out = run_sync_fail_after(&url, 1, &doubles);
    let used_lower = used_out.to_ascii_lowercase();
    assert!(
        used_lower.contains("affect")
            && (used_lower.contains("recomput")
                || used_lower.contains("affected")
                || used_lower.contains("delivery complete")),
        "AMOUNT update must recompute/Deliver after rename, got:\n{used_out}"
    );
    let derived_after = derived_stdout(&url, "order-shaped");
    assert!(
        derived_after.contains("50.00") || derived_after.contains("50"),
        "used AMOUNT change must update orderAmount in Derived, got:\n{derived_after}"
    );
    // Output Identity merge: renamed field value change must replace, not duplicate.
    let order_100_hits = derived_after.matches("\"ORDER_ID\": 100").count()
        + derived_after.matches("\"ORDER_ID\":100").count();
    assert_eq!(
        order_100_hits, 1,
        "Derived must keep one row per Output Identity after rename-path update, got:\n{derived_after}"
    );
    assert!(
        !derived_after.contains("42.50"),
        "stale pre-update orderAmount must not remain in Derived, got:\n{derived_after}"
    );
}

#[tokio::test]
async fn transform_field_ops_script_and_unsupported_still_fail_apply() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());

    let script_pipeline = r#"
    - name: scripted
      mode: transform
      source:
        table: CUSTOMERS
      target:
        collection: scripted
      outputIdentity: [ID]
      transform:
        - addFields:
            fields:
              - as: ok
                value: 1
        - script: "return true"
"#;
    let script_config = write_config(
        &dir,
        "script.yaml",
        &deployment_shell(&mongo_database, script_pipeline),
    );
    let err = apply_expect_failure(&url, &script_config, &doubles);
    let lower = err.to_ascii_lowercase();
    assert!(
        lower.contains("script") || lower.contains("free-form"),
        "expected clear free-form script failure, got:\n{err}"
    );

    let url2 = ephemeral_database_url().await;
    let mongo2 = unique_mongo_database();
    let dir2 = TempDir::new().expect("tempdir");
    let doubles2 = common::NamedScenarioDoubles::install(dir2.path());
    let bad_pipeline = r#"
    - name: faceted
      mode: transform
      source:
        table: CUSTOMERS
      target:
        collection: faceted
      outputIdentity: [ID]
      transform:
        - rename:
            fields:
              - from: NAME
                to: customerName
        - facet:
            stages: []
"#;
    let bad_config = write_config(
        &dir2,
        "facet.yaml",
        &deployment_shell(&mongo2, bad_pipeline),
    );
    let err = apply_expect_failure(&url2, &bad_config, &doubles2);
    let lower = err.to_ascii_lowercase();
    assert!(
        lower.contains("unsupported") && lower.contains("facet"),
        "expected clear unsupported facet failure, got:\n{err}"
    );
}

#[tokio::test]
async fn transform_email_only_update_skips_when_removed() {
    // Custom Incremental: EMAIL-only update must skip when EMAIL is removed.
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let extra = vec![LogMinerContent {
        scn: 1040,
        operation: LogMinerOperation::Update,
        seg_owner: "APP".to_string(),
        table_name: "CUSTOMERS".to_string(),
        identity: row(&[("ID", json_num(1))]),
        after_image: Some(row(&[
            ("ID", json_num(1)),
            ("NAME", json_str("Alice")),
            ("EMAIL", json_str("only-email-changed@example.com")),
            ("ACTIVE", json_num(1)),
            ("BIO", json_str("blob-bytes-alice")),
        ])),
            rs_id: String::new(),
            ssn: 0,
        }];
    let doubles = common::NamedScenarioDoubles::install_with_extra_logminer(dir.path(), &extra);
    let pipeline = r#"
    - name: no-email
      mode: transform
      source:
        table: CUSTOMERS
      target:
        collection: no_email
      outputIdentity: [ID]
      transform:
        - $project:
            ID: 1
            NAME: 1
            EMAIL: 1
            ACTIVE: 1
        - $unset: EMAIL
"#;
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_shell(&mongo_database, pipeline),
    );

    migrate_and_apply(&url, &config, &doubles);
    let applied_initial = delivery_applied_changes(&status_stdout(&url), "no-email")
        .expect("appliedChanges after Initial Load");

    // fail_after=1 consumes the EMAIL-only update at SCN 1040 before Alice→Alicia.
    let unused_out = run_sync_fail_after(&url, 1, &doubles);
    assert!(
        unused_out.to_ascii_lowercase().contains("skip")
            || unused_out.to_ascii_lowercase().contains("unused"),
        "EMAIL-only update must skip when removed, got:\n{unused_out}"
    );
    let applied_after = delivery_applied_changes(&status_stdout(&url), "no-email")
        .expect("appliedChanges after EMAIL-only");
    assert_eq!(
        applied_after, applied_initial,
        "removed EMAIL change must not Deliver"
    );
    let derived = derived_stdout(&url, "no-email");
    assert!(
        derived.contains("Alice") && !derived.contains("only-email-changed"),
        "Derived NAME must stay Alice and EMAIL absent, got:\n{derived}"
    );
}
