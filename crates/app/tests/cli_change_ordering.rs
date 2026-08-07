//! Operator-visible seam: Change Ordering / confluence (ADR-0029 / issue #225).
//!
//! Agreed seam: CLI apply/sync + Base / Derived / Target Managed outcomes.
//! Lab Scenario `change-ordering` proves the same contract on live Oracle LogMiner.
//! This non-ignored contract/stub twin covers:
//! - same source key A→B→C capture order (final Managed is C, not B)
//! - distinct keys interleaved while both finals remain correct
//! - min aggregate after deleting the current extreme → per-identity Base recompute
//! Normal path must not invoke Source Alignment Check / Drift Check.

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use migraloop_capture::{
    ContractSourceCatalog, LogMinerContent, LogMinerOperation, CONTRACT_SOURCE_CATALOG_ENV,
    INJECT_LOGMINER_CONTENTS_ENV,
};
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

fn row(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

fn json_str(value: &str) -> Value {
    Value::String(value.to_string())
}

fn json_num(value: i64) -> Value {
    json!(value)
}

fn customer_update(scn: u64, id: i64, name: &str, email: &str) -> LogMinerContent {
    LogMinerContent {
        scn,
        operation: LogMinerOperation::Update,
        seg_owner: "APP".to_string(),
        table_name: "CUSTOMERS".to_string(),
        identity: row(&[("ID", json_num(id))]),
        after_image: Some(row(&[
            ("ID", json_num(id)),
            ("NAME", json_str(name)),
            ("EMAIL", json_str(email)),
            ("ACTIVE", json_num(1)),
            ("BIO", json_str("blob-bytes")),
        ])),
        rs_id: String::new(),
        ssn: 0,
    }
}

/// Capture stream for ADR-0029: interleaved keys + same-key A→B→C + min extreme delete.
fn change_ordering_logminer_contents() -> Vec<LogMinerContent> {
    vec![
        // Same key ID=1: Alice(A) → NameB → NameC. Wrong order leaves NameB.
        customer_update(1050, 1, "NameB", "b@example.com"),
        // Distinct key ID=2 interleaved between ID=1 updates.
        customer_update(1051, 2, "Key2Mid", "k2mid@example.com"),
        customer_update(1052, 1, "NameC", "c@example.com"),
        customer_update(1053, 2, "Key2Final", "k2final@example.com"),
        // Delete current min extreme for customer 1 (ORDER_ID 101 / AMOUNT 10.00).
        LogMinerContent {
            scn: 540,
            operation: LogMinerOperation::Delete,
            seg_owner: "APP".to_string(),
            table_name: "ORDERS".to_string(),
            identity: row(&[("ORDER_ID", json_num(101))]),
            after_image: None,
            rs_id: String::new(),
            ssn: 0,
        },
    ]
}

struct ChangeOrderingDoubles {
    catalog_path: PathBuf,
    logminer_path: PathBuf,
}

impl ChangeOrderingDoubles {
    fn install(dir: &Path) -> Self {
        let catalog_path = dir.join(format!(
            "change_ordering_catalog_{}.json",
            common::unique_suffix()
        ));
        let logminer_path = dir.join(format!(
            "change_ordering_logminer_{}.json",
            common::unique_suffix()
        ));

        let catalog = ContractSourceCatalog::with_default_fixtures();
        fs::write(
            &catalog_path,
            serde_json::to_string_pretty(&catalog.to_file()).expect("serialize catalog"),
        )
        .expect("write catalog");

        let inject = json!({ "contents": change_ordering_logminer_contents() });
        fs::write(
            &logminer_path,
            serde_json::to_string_pretty(&inject).expect("serialize logminer"),
        )
        .expect("write logminer inject");

        Self {
            catalog_path,
            logminer_path,
        }
    }

    fn apply_env<'a>(&self, cmd: &'a mut Command) -> &'a mut Command {
        cmd.env(CONTRACT_SOURCE_CATALOG_ENV, &self.catalog_path)
            .env(INJECT_LOGMINER_CONTENTS_ENV, &self.logminer_path)
    }
}

fn deployment(mongo_database: &str) -> String {
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
    - name: order-stats
      mode: transform
      source:
        table: ORDERS
      target:
        collection: order_stats
      outputIdentity: [CUSTOMER_ID]
      transform:
        - $group:
            _id: "$CUSTOMER_ID"
            ORDER_COUNT:
              $count: "$ORDER_ID"
            MIN_AMOUNT:
              $min: "$AMOUNT"
            MAX_AMOUNT:
              $max: "$AMOUNT"
            TOTAL_AMOUNT:
              $sum: "$AMOUNT"
"#,
        host = mongo_host(),
        port = mongo_port(),
    )
}

fn migrate_and_apply(url: &str, config: &Path, doubles: &ChangeOrderingDoubles) -> String {
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

fn run_sync(url: &str, doubles: &ChangeOrderingDoubles) -> String {
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
    format!(
        "{}{}",
        String::from_utf8_lossy(&sync.stdout),
        String::from_utf8_lossy(&sync.stderr)
    )
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

fn inspect_derived(url: &str) -> String {
    let derived = Command::new(bin())
        .args([
            "derived",
            "--platform-store-url",
            url,
            "--pipeline",
            "order-stats",
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
async fn change_ordering_same_key_cross_key_and_min_recompute_on_normal_incremental_path() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = ChangeOrderingDoubles::install(dir.path());
    let config = write_config(&dir, "deployment.yaml", &deployment(&mongo_database));

    let apply_out = migrate_and_apply(&url, &config, &doubles);
    assert!(
        apply_out.contains("Initial Load complete: Base Dataset CUSTOMERS")
            && apply_out.contains("Initial Load complete: Base Dataset ORDERS"),
        "apply must Initial Load both Bases, got:\n{apply_out}"
    );

    let derived_initial = inspect_derived(&url);
    assert!(
        derived_initial.contains("10.00")
            && derived_initial.contains("42.50")
            && derived_initial.contains("5.00"),
        "Initial min/max Derived must include extremes 10/42.50 and key2=5, got:\n{derived_initial}"
    );

    let sync_out = run_sync(&url, &doubles);
    let sync_lower = sync_out.to_ascii_lowercase();
    assert!(
        sync_lower.contains("incremental") || sync_lower.contains("sync"),
        "sync should report Incremental Capture, got:\n{sync_out}"
    );
    assert!(
        !sync_lower.contains("alignment") && !sync_lower.contains("drift"),
        "normal Incremental path must not rely on Source Alignment Check / Drift Check, got:\n{sync_out}"
    );

    // Same-key A→B→C: final Managed is NameC (not intermediate NameB / seed Alice).
    let customers_base = inspect_base(&url, "CUSTOMERS");
    assert!(
        customers_base.contains("NameC"),
        "same-key capture order must leave NameC on Base, got:\n{customers_base}"
    );
    assert!(
        !customers_base.contains("NameB") && !customers_base.contains("Alice"),
        "stale same-key values NameB/Alice must be gone, got:\n{customers_base}"
    );
    // Cross-key interleave: ID=2 finals independently to Key2Final.
    assert!(
        customers_base.contains("Key2Final"),
        "cross-key interleave must leave Key2Final on Base, got:\n{customers_base}"
    );
    assert!(
        !customers_base.contains("Key2Mid") && !customers_base.contains("Bob"),
        "stale key-2 values must be gone, got:\n{customers_base}"
    );

    let customers_target = inspect_target(&url, "customers");
    assert!(
        customers_target.contains("NameC") && customers_target.contains("Key2Final"),
        "Target Managed must Deliver same-key and cross-key finals, got:\n{customers_target}"
    );
    assert!(
        !customers_target.contains("NameB") && !customers_target.contains("Key2Mid"),
        "Target must not retain intermediate same-key / cross-key values, got:\n{customers_target}"
    );

    // Min extreme delete: customer 1 left with 42.50 only → min/max/sum 42.50, count 1.
    // Naive running-min without Base recompute would incorrectly keep 10.00.
    let derived_after = inspect_derived(&url);
    assert!(
        derived_after.contains("42.50")
            && derived_after.contains("\"ORDER_COUNT\": 1")
            && derived_after.contains("5.00"),
        "min extreme delete must Base-recompute customer 1 to 42.50/count=1; key2 stays 5, got:\n{derived_after}"
    );
    assert!(
        !derived_after.contains("10.00"),
        "stale min 10.00 must be gone after Base recompute, got:\n{derived_after}"
    );

    let stats_target = inspect_target(&url, "order_stats");
    assert!(
        stats_target.contains("42.50") || stats_target.contains("42.5"),
        "order_stats Target must Deliver recomputed min/max/sum 42.50, got:\n{stats_target}"
    );
    assert!(
        !stats_target.contains("10.00") && !stats_target.contains("\"10\""),
        "order_stats Target must not retain stale min 10, got:\n{stats_target}"
    );
}
