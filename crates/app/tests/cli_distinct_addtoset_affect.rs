//! Operator-visible seam: distinct / addToSet + value-level Affect Analysis (#128).
//!
//! Agreed seam: CLI config/status + Derived Dataset + Target documents.
//! Maintenance State enables skipping useless Derived updates when a duplicate
//! distinct key / addToSet member is already counted. Simple groupBy aggregations
//! must not invent Maintenance State (covered in transform unit tests).

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

fn json_num(value: i64) -> Value {
    Value::Number(value.into())
}

fn json_str(value: &str) -> Value {
    Value::String(value.to_string())
}

/// ORDERS Incremental stream ordered for distinct/addToSet value-level coverage.
fn distinct_addtoset_logminer_contents() -> Vec<LogMinerContent> {
    vec![
        // 1) ADDRESS-only — unused by distinct CUSTOMER_ID and by addToSet AMOUNT.
        LogMinerContent {
            scn: 510,
            operation: LogMinerOperation::Update,
            seg_owner: "APP".to_string(),
            table_name: "ORDERS".to_string(),
            identity: row(&[("ORDER_ID", json_num(100))]),
            after_image: Some(row(&[
                ("ORDER_ID", json_num(100)),
                ("CUSTOMER_ID", json_num(1)),
                ("AMOUNT", json_str("42.50")),
                ("ADDRESS", json_str("1 Main Ave")),
            ])),
            rs_id: String::new(),
            ssn: 0,
        },
        // 2) Duplicate CUSTOMER_ID insert — distinct value-level skip; addToSet may
        //    skip if AMOUNT already in set (42.50 is already present for customer 1).
        LogMinerContent {
            scn: 515,
            operation: LogMinerOperation::Insert,
            seg_owner: "APP".to_string(),
            table_name: "ORDERS".to_string(),
            identity: row(&[("ORDER_ID", json_num(102))]),
            after_image: Some(row(&[
                ("ORDER_ID", json_num(102)),
                ("CUSTOMER_ID", json_num(1)),
                ("AMOUNT", json_str("42.50")),
                ("ADDRESS", json_str("1 Main Ave")),
            ])),
            rs_id: String::new(),
            ssn: 0,
        },
        // 3) New AMOUNT for customer 1 — addToSet recompute; distinct still skip
        //    (CUSTOMER_ID already counted).
        LogMinerContent {
            scn: 520,
            operation: LogMinerOperation::Insert,
            seg_owner: "APP".to_string(),
            table_name: "ORDERS".to_string(),
            identity: row(&[("ORDER_ID", json_num(103))]),
            after_image: Some(row(&[
                ("ORDER_ID", json_num(103)),
                ("CUSTOMER_ID", json_num(1)),
                ("AMOUNT", json_str("7.00")),
                ("ADDRESS", json_str("1 Main Ave")),
            ])),
            rs_id: String::new(),
            ssn: 0,
        },
        // 4) Last order for customer 2 moves to customer 3 — distinct removes 2 / adds 3.
        LogMinerContent {
            scn: 530,
            operation: LogMinerOperation::Update,
            seg_owner: "APP".to_string(),
            table_name: "ORDERS".to_string(),
            identity: row(&[("ORDER_ID", json_num(200))]),
            after_image: Some(row(&[
                ("ORDER_ID", json_num(200)),
                ("CUSTOMER_ID", json_num(3)),
                ("AMOUNT", json_str("5.00")),
                ("ADDRESS", json_str("2 Side Rd")),
            ])),
            rs_id: String::new(),
            ssn: 0,
        },
        // 5) Delete last remaining order for customer 3 — both Pipelines drop identity 3.
        LogMinerContent {
            scn: 540,
            operation: LogMinerOperation::Delete,
            seg_owner: "APP".to_string(),
            table_name: "ORDERS".to_string(),
            identity: row(&[("ORDER_ID", json_num(200))]),
            after_image: None,
            rs_id: String::new(),
            ssn: 0,
        },
    ]
}

struct Doubles {
    catalog_path: PathBuf,
    logminer_path: PathBuf,
}

impl Doubles {
    fn install(dir: &Path) -> Self {
        let catalog_path = dir.join(format!(
            "distinct_addtoset_catalog_{}.json",
            common::unique_suffix()
        ));
        let logminer_path = dir.join(format!(
            "distinct_addtoset_logminer_{}.json",
            common::unique_suffix()
        ));

        let catalog = ContractSourceCatalog::with_default_fixtures();
        fs::write(
            &catalog_path,
            serde_json::to_string_pretty(&catalog.to_file()).expect("serialize catalog"),
        )
        .expect("write catalog");

        let inject = json!({ "contents": distinct_addtoset_logminer_contents() });
        fs::write(
            &logminer_path,
            serde_json::to_string_pretty(&inject).expect("serialize logminer"),
        )
        .expect("write logminer");

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
    - name: distinct-customers
      mode: transform
      source:
        table: ORDERS
      target:
        collection: distinct_customers
      outputIdentity: [CUSTOMER_ID]
      transform:
        - $group:
            _id: "$CUSTOMER_ID"
    - name: amounts-by-customer
      mode: transform
      source:
        table: ORDERS
      target:
        collection: amounts_by_customer
      outputIdentity: [CUSTOMER_ID]
      transform:
        - $group:
            _id: "$CUSTOMER_ID"
            AMOUNTS:
              $addToSet: "$AMOUNT"
"#,
        host = mongo_host(),
        port = mongo_port(),
    )
}

fn migrate_and_apply(url: &str, config: &Path, doubles: &Doubles) -> String {
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

fn run_sync_fail_after(url: &str, after: u32, doubles: &Doubles) -> std::process::Output {
    let mut cmd = Command::new(bin());
    cmd.env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value");
    doubles.apply_env(&mut cmd);
    cmd.args(["sync", "--platform-store-url", url]);
    common::SyncCliOptions::fail_after(after).append_to(&mut cmd);
    let out = cmd.output().expect("run sync with fail-after");
    out
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

async fn maintenance_state_row_count(url: &str, pipeline: &str) -> i64 {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect test store");
    let count: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*)::bigint
        FROM maintenance_states
        WHERE pipeline_name = $1
        "#,
    )
    .bind(pipeline)
    .fetch_one(&pool)
    .await
    .expect("count maintenance_states");
    count.0
}

#[tokio::test]
async fn distinct_addtoset_value_level_affect_skips_duplicates_and_keeps_delivery_correct() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = Doubles::install(dir.path());
    let config = write_config(&dir, "deployment.yaml", &deployment(&mongo_database));

    migrate_and_apply(&url, &config, &doubles);

    // Initial Load: distinct customers {1,2}; addToSet customer 1 has 10.00+42.50.
    let distinct_initial = derived_stdout(&url, "distinct-customers");
    assert!(
        distinct_initial.contains("\"CUSTOMER_ID\": 1")
            && distinct_initial.contains("\"CUSTOMER_ID\": 2"),
        "distinct Derived must list customers 1 and 2, got:\n{distinct_initial}"
    );
    assert!(
        !distinct_initial.contains("ADDRESS") && !distinct_initial.contains("AMOUNT"),
        "distinct output is only CUSTOMER_ID, got:\n{distinct_initial}"
    );

    let add_initial = derived_stdout(&url, "amounts-by-customer");
    assert!(
        add_initial.contains("42.50") && add_initial.contains("10.00") && add_initial.contains("5.00"),
        "addToSet Derived must materialize AMOUNT sets, got:\n{add_initial}"
    );

    assert_eq!(
        maintenance_state_row_count(&url, "distinct-customers").await,
        1,
        "distinct Pipeline must persist Maintenance State"
    );
    assert_eq!(
        maintenance_state_row_count(&url, "amounts-by-customer").await,
        1,
        "addToSet Pipeline must persist Maintenance State"
    );

    let status_initial = status_stdout(&url);
    let distinct_applied_initial =
        delivery_applied_changes(&status_initial, "distinct-customers").expect("distinct applied");
    let add_applied_initial =
        delivery_applied_changes(&status_initial, "amounts-by-customer").expect("addToSet applied");
    assert!(
        distinct_applied_initial >= 2 && add_applied_initial >= 2,
        "Initial Delivery should cover both Output Identities: distinct={distinct_applied_initial} add={add_applied_initial}"
    );

    // Change 1: ADDRESS unused → both Pipelines skip unused.
    let sync_unused = run_sync_fail_after(&url, 1, &doubles);
    let unused_out = format!(
        "{}{}",
        String::from_utf8_lossy(&sync_unused.stdout),
        String::from_utf8_lossy(&sync_unused.stderr)
    );
    assert!(
        unused_out.to_ascii_lowercase().contains("affect")
            && unused_out.to_ascii_lowercase().contains("unused"),
        "unused ADDRESS must surface Affect Analysis unused skip, got:\n{unused_out}"
    );
    let status_after_unused = status_stdout(&url);
    assert_eq!(
        delivery_applied_changes(&status_after_unused, "distinct-customers"),
        Some(distinct_applied_initial)
    );
    assert_eq!(
        delivery_applied_changes(&status_after_unused, "amounts-by-customer"),
        Some(add_applied_initial)
    );

    // Change 2: duplicate CUSTOMER_ID=1 + AMOUNT=42.50 → value-level skip for both.
    let sync_dup = run_sync_fail_after(&url, 1, &doubles);
    let dup_out = format!(
        "{}{}",
        String::from_utf8_lossy(&sync_dup.stdout),
        String::from_utf8_lossy(&sync_dup.stderr)
    );
    assert!(
        dup_out.to_ascii_lowercase().contains("value-level")
            || dup_out.to_ascii_lowercase().contains("no derived change"),
        "duplicate key/member must surface value-level Affect skip, got:\n{dup_out}"
    );
    let status_after_dup = status_stdout(&url);
    assert_eq!(
        delivery_applied_changes(&status_after_dup, "distinct-customers"),
        Some(distinct_applied_initial),
        "duplicate distinct key must not Deliver"
    );
    assert_eq!(
        delivery_applied_changes(&status_after_dup, "amounts-by-customer"),
        Some(add_applied_initial),
        "duplicate addToSet member must not Deliver"
    );

    // Change 3: new AMOUNT 7.00 for customer 1 — distinct skip; addToSet recompute.
    let sync_new_amount = run_sync_fail_after(&url, 1, &doubles);
    let new_out = format!(
        "{}{}",
        String::from_utf8_lossy(&sync_new_amount.stdout),
        String::from_utf8_lossy(&sync_new_amount.stderr)
    );
    assert!(
        new_out.contains("distinct-customers")
            && (new_out.to_ascii_lowercase().contains("value-level")
                || new_out.to_ascii_lowercase().contains("no derived change")),
        "new AMOUNT with existing CUSTOMER_ID must value-level skip distinct, got:\n{new_out}"
    );
    assert!(
        new_out.contains("amounts-by-customer")
            && new_out.to_ascii_lowercase().contains("affect"),
        "new AMOUNT must Affect Analysis recompute addToSet, got:\n{new_out}"
    );

    let add_after_new = derived_stdout(&url, "amounts-by-customer");
    assert!(
        add_after_new.contains("7.00")
            && add_after_new.contains("42.50")
            && add_after_new.contains("10.00"),
        "addToSet must include new AMOUNT 7.00, got:\n{add_after_new}"
    );
    let status_after_new = status_stdout(&url);
    assert_eq!(
        delivery_applied_changes(&status_after_new, "distinct-customers"),
        Some(distinct_applied_initial),
        "distinct Delivery must stay unchanged for new AMOUNT"
    );
    let add_applied_after_new =
        delivery_applied_changes(&status_after_new, "amounts-by-customer").expect("add applied");
    assert_eq!(
        add_applied_after_new,
        add_applied_initial + 1,
        "addToSet Delivery must update only the affected identity"
    );

    // Change 4: CUSTOMER_ID 2→3 on last order — distinct removes 2, adds 3.
    let sync_move = run_sync_fail_after(&url, 1, &doubles);
    let move_out = format!(
        "{}{}",
        String::from_utf8_lossy(&sync_move.stdout),
        String::from_utf8_lossy(&sync_move.stderr)
    );
    assert!(
        move_out.contains("distinct-customers")
            && move_out.to_ascii_lowercase().contains("affect"),
        "group-key move must Affect Analysis distinct, got:\n{move_out}"
    );

    let distinct_after_move = derived_stdout(&url, "distinct-customers");
    assert!(
        distinct_after_move.contains("\"CUSTOMER_ID\": 1")
            && distinct_after_move.contains("\"CUSTOMER_ID\": 3"),
        "distinct must show customers 1 and 3 after key move, got:\n{distinct_after_move}"
    );
    assert!(
        !distinct_after_move.contains("\"CUSTOMER_ID\": 2"),
        "customer 2 must be removed from distinct Derived, got:\n{distinct_after_move}"
    );

    let add_after_move = derived_stdout(&url, "amounts-by-customer");
    assert!(
        add_after_move.contains("\"CUSTOMER_ID\": 3") && add_after_move.contains("5.00"),
        "addToSet must move 5.00 to customer 3, got:\n{add_after_move}"
    );
    assert!(
        !add_after_move.contains("\"CUSTOMER_ID\": 2"),
        "customer 2 addToSet identity must be gone, got:\n{add_after_move}"
    );

    // Change 5: delete last order for customer 3 — both Pipelines remove that identity.
    let sync_delete = run_sync_fail_after(&url, 1, &doubles);
    let delete_out = format!(
        "{}{}",
        String::from_utf8_lossy(&sync_delete.stdout),
        String::from_utf8_lossy(&sync_delete.stderr)
    );
    assert!(
        delete_out.to_ascii_lowercase().contains("affect"),
        "last-row delete must Affect Analysis recompute, got:\n{delete_out}"
    );
    let distinct_after_delete = derived_stdout(&url, "distinct-customers");
    assert!(
        distinct_after_delete.contains("\"CUSTOMER_ID\": 1")
            && !distinct_after_delete.contains("\"CUSTOMER_ID\": 3")
            && !distinct_after_delete.contains("\"CUSTOMER_ID\": 2"),
        "delete must remove customer 3 from distinct, got:\n{distinct_after_delete}"
    );
    let add_after_delete = derived_stdout(&url, "amounts-by-customer");
    assert!(
        !add_after_delete.contains("\"CUSTOMER_ID\": 3")
            && add_after_delete.contains("\"CUSTOMER_ID\": 1"),
        "delete must remove customer 3 from addToSet, got:\n{add_after_delete}"
    );
}
