//! Operator-visible seam: Rich Transform unwind (issue #129).
//!
//! Agreed seams: CLI config apply → equiLookup+unwind Derived rows + Mongo
//! Delivery by unwound Output Identity; Affect Analysis on primary/foreign Bases
//! (insert/update/delete of identities); unsupported `$unwind` options / scripts
//! fail apply clearly.

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

fn unwind_pipeline() -> &'static str {
    r#"
    - name: orders-unwound
      mode: transform
      source:
        table: CUSTOMERS
      target:
        collection: orders_unwound
      outputIdentity: [ORDER_ID]
      transform:
        - $project:
            ID: 1
            NAME: 1
        - $lookup:
            from: ORDERS
            localField: ID
            foreignField: CUSTOMER_ID
            as: orders
        - $unwind: "$orders"
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
        .env("MONGO_PASSWORD", "mongo-secret-value");
    doubles.apply_env(&mut cmd);
    cmd.args(["sync", "--platform-store-url", url]);
    common::SyncCliOptions::fail_after(after).append_to(&mut cmd);
    let out = cmd.output().expect("run sync with fail-after");
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
async fn unwind_materializes_unwound_identities_and_delivers() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_shell(&mongo_database, unwind_pipeline()),
    );

    let apply_out = migrate_and_apply(&url, &config, &doubles);
    assert!(
        apply_out.contains("Initial Load complete: Base Dataset CUSTOMERS")
            && apply_out.contains("Initial Load complete: Base Dataset ORDERS"),
        "equiLookup+unwind must Initial Load both Bases, got:\n{apply_out}"
    );
    assert!(
        apply_out.contains("Derived Dataset materialized")
            && apply_out.to_ascii_lowercase().contains("3 rows"),
        "unwind must materialize one Derived row per order, got:\n{apply_out}"
    );

    let derived = derived_stdout(&url, "orders-unwound");
    assert!(
        derived.contains("ORDER_ID")
            && derived.contains("Alice")
            && derived.contains("42.50")
            && derived.contains("10.00")
            && derived.contains("Bob")
            && derived.contains("5.00"),
        "Derived must flatten unwound order fields, got:\n{derived}"
    );
    assert!(
        !derived.contains("\"orders\"") && !derived.contains("alice@example.com"),
        "unwind must remove orders array and project must drop EMAIL, got:\n{derived}"
    );

    let target = target_stdout(&url, "orders_unwound");
    assert!(
        target.contains("Alice")
            && target.contains("42.50")
            && target.contains("ORDER_ID")
            && (target.contains("100") && target.contains("101") && target.contains("200")),
        "Mongo Delivery must upsert by unwound ORDER_ID Output Identity, got:\n{target}"
    );
}

#[tokio::test]
async fn unwind_affect_analysis_updates_and_deletes_identities() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    // EMAIL-only primary update (unused after project) then stock Incremental.
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
            rs_id: String::new(),
            ssn: 0,
        }];
    let doubles = common::NamedScenarioDoubles::install_with_extra_logminer(dir.path(), &extra);
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_shell(&mongo_database, unwind_pipeline()),
    );

    migrate_and_apply(&url, &config, &doubles);

    // CUSTOMERS table first. fail_after=1 → EMAIL-only skip.
    let unused_out = run_sync_fail_after(&url, 1, &doubles);
    assert!(
        unused_out.to_ascii_lowercase().contains("skip")
            || unused_out.to_ascii_lowercase().contains("unused"),
        "EMAIL-only primary update must Affect-Analysis skip, got:\n{unused_out}"
    );
    let derived_after_skip = derived_stdout(&url, "orders-unwound");
    assert!(
        derived_after_skip.contains("Alice") && !derived_after_skip.contains("only-email"),
        "unused EMAIL must not alter Derived, got:\n{derived_after_skip}"
    );

    // Alice→Alicia (NAME used) — recompute unwound identities 100 and 101.
    let name_out = run_sync_fail_after(&url, 1, &doubles);
    let name_lower = name_out.to_ascii_lowercase();
    assert!(
        name_lower.contains("affect")
            && (name_lower.contains("recomput")
                || name_lower.contains("affected")
                || name_lower.contains("delivery complete")),
        "NAME update must recompute unwound identities, got:\n{name_out}"
    );
    let derived_name = derived_stdout(&url, "orders-unwound");
    assert!(
        derived_name.contains("Alicia")
            && derived_name.contains("100")
            && derived_name.contains("101"),
        "primary NAME change must update unwound Derived rows, got:\n{derived_name}"
    );

    // Carol insert + Bob delete: Bob's order 200 Output Identity must be deleted.
    let bob_out = run_sync_fail_after(&url, 2, &doubles);
    let derived_bob = derived_stdout(&url, "orders-unwound");
    assert!(
        !derived_bob.contains("Bob"),
        "deleting Bob must remove unwound order identities, got:\n{derived_bob}\nsync:\n{bob_out}"
    );
    let target_bob = target_stdout(&url, "orders_unwound");
    assert!(
        !(target_bob.contains("\"_id\": \"200\"")
            || target_bob.contains("\"_id\":\"200\"")
            || target_bob.contains("\"_id\": 200")
            || target_bob.contains("\"_id\":200")),
        "order 200 Output Identity must be deleted after Bob delete, got:\n{target_bob}"
    );

    // ORDERS ADDRESS update (SCN 510): flattened field → recompute ORDER_ID 100.
    let address_out = run_sync_fail_after(&url, 1, &doubles);
    let address_lower = address_out.to_ascii_lowercase();
    assert!(
        address_lower.contains("affect")
            && (address_lower.contains("recomput")
                || address_lower.contains("affected")
                || address_lower.contains("delivery complete")),
        "foreign ADDRESS change must Affect Analysis recompute after unwind flatten, got:\n{address_out}"
    );
    let derived_address = derived_stdout(&url, "orders-unwound");
    assert!(
        derived_address.contains("Main Ave") || derived_address.contains("1 Main Ave"),
        "foreign ADDRESS must appear on unwound Derived row, got:\n{derived_address}"
    );

    // ORDERS AMOUNT 42.50→50.00 (SCN 520).
    let amount_out = run_sync_fail_after(&url, 1, &doubles);
    assert!(
        amount_out.to_ascii_lowercase().contains("affect")
            || amount_out.to_ascii_lowercase().contains("delivery"),
        "foreign AMOUNT change must maintain Derived, got:\n{amount_out}"
    );
    let derived_amount = derived_stdout(&url, "orders-unwound");
    assert!(
        derived_amount.contains("50.00") || derived_amount.contains("50"),
        "foreign AMOUNT must update unwound row, got:\n{derived_amount}"
    );
    assert!(
        !derived_amount.contains("42.50"),
        "stale order amount must not remain, got:\n{derived_amount}"
    );

    // ORDERS CUSTOMER_ID move 200: 2→3 (Bob gone, Carol present) → Delivery upsert for Carol.
    let move_out = run_sync_fail_after(&url, 1, &doubles);
    let derived_move = derived_stdout(&url, "orders-unwound");
    assert!(
        derived_move.contains("Carol")
            && (derived_move.contains("200") || derived_move.contains("\"ORDER_ID\": 200")),
        "order 200 must reappear under Carol after CUSTOMER_ID move, got:\n{derived_move}\nsync:\n{move_out}"
    );

    let target = target_stdout(&url, "orders_unwound");
    assert!(
        target.contains("Alicia")
            && (target.contains("50.00") || target.contains("50"))
            && target.contains("Carol"),
        "Mongo Delivery must reflect unwound identity insert/update/delete, got:\n{target}"
    );
}

#[tokio::test]
async fn unwind_rejects_unsupported_forms_and_scripts_on_apply() {
    let url2 = ephemeral_database_url().await;
    let mongo2 = unique_mongo_database();
    let dir2 = TempDir::new().expect("tempdir");
    let doubles2 = common::NamedScenarioDoubles::install(dir2.path());
    // Issue #232: Aggregation `$unwind` path is accepted; preserveNullAndEmptyArrays is not.
    let unsupported_pipeline = r#"
    - name: bad-preserve
      mode: transform
      source:
        table: CUSTOMERS
      target:
        collection: bad_preserve
      outputIdentity: [ORDER_ID]
      transform:
        - $lookup:
            from: ORDERS
            localField: ID
            foreignField: CUSTOMER_ID
            as: orders
        - $unwind:
            path: orders
            preserveNullAndEmptyArrays: true
"#;
    let unsupported_config = write_config(
        &dir2,
        "preserve.yaml",
        &deployment_shell(&mongo2, unsupported_pipeline),
    );
    let err = apply_expect_failure(&url2, &unsupported_config, &doubles2);
    let lower = err.to_ascii_lowercase();
    assert!(
        lower.contains("preservenullandemptyarrays") || lower.contains("preserve"),
        "expected clear unsupported unwind form failure, got:\n{err}"
    );

    let url3 = ephemeral_database_url().await;
    let mongo3 = unique_mongo_database();
    let dir3 = TempDir::new().expect("tempdir");
    let doubles3 = common::NamedScenarioDoubles::install(dir3.path());
    let script_pipeline = r#"
    - name: scripted
      mode: transform
      source:
        table: CUSTOMERS
      target:
        collection: scripted
      outputIdentity: [ORDER_ID]
      transform:
        - equiLookup:
            from: ORDERS
            localField: ID
            foreignField: CUSTOMER_ID
            as: orders
        - unwind:
            path: orders
        - script: "return true"
"#;
    let script_config = write_config(
        &dir3,
        "script.yaml",
        &deployment_shell(&mongo3, script_pipeline),
    );
    let err = apply_expect_failure(&url3, &script_config, &doubles3);
    let lower = err.to_ascii_lowercase();
    assert!(
        lower.contains("script") || lower.contains("free-form"),
        "expected clear free-form script failure, got:\n{err}"
    );
}
