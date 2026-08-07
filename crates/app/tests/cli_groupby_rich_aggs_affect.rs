//! Operator-visible seam: groupBy count/min/max/avg + Affect Analysis (issue #126).
//!
//! Agreed seam: CLI config/status + Derived Dataset + Target documents.
//! Unused Base fields must not recompute Derived; used-field changes update only
//! affected Output Identities. No Maintenance State for these simple aggregations.

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

fn order_stats_deployment(mongo_database: &str) -> String {
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
            AVG_AMOUNT:
              $avg: "$AMOUNT"
            TOTAL_AMOUNT:
              $sum: "$AMOUNT"
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

fn run_sync_fail_after(url: &str, after: u32, doubles: &common::NamedScenarioDoubles) -> std::process::Output {
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

fn derived_stdout(url: &str) -> String {
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

fn target_stdout(url: &str) -> String {
    let target = Command::new(bin())
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "target",
            "--platform-store-url",
            url,
            "--collection",
            "order_stats",
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

#[tokio::test]
async fn groupby_rich_aggs_affect_analysis_skips_unused_and_updates_affected_identities() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &order_stats_deployment(&mongo_database),
    );

    migrate_and_apply(&url, &config, &doubles);

    // Initial Load: customer 1 = count 2, min 10, max 42.50, avg 26.25, sum 52.50;
    // customer 2 = count 1 / all 5.00.
    let derived_initial = derived_stdout(&url);
    assert!(
        derived_initial.contains("\"ORDER_COUNT\": 2")
            && derived_initial.contains("10.00")
            && derived_initial.contains("42.50")
            && derived_initial.contains("26.25")
            && derived_initial.contains("52.50")
            && derived_initial.contains("5.00"),
        "groupBy count/min/max/avg/sum Derived must materialize, got:\n{derived_initial}"
    );
    assert!(
        !derived_initial.contains("Main St") && !derived_initial.contains("ADDRESS"),
        "ADDRESS is unused and must not appear in Derived, got:\n{derived_initial}"
    );

    let status_initial = status_stdout(&url);
    let applied_initial = delivery_applied_changes(&status_initial, "order-stats")
        .expect("Delivery Health appliedChanges after Initial Load");
    assert!(
        applied_initial >= 2,
        "Initial Delivery should cover both Output Identities, got appliedChanges={applied_initial}:\n{status_initial}"
    );

    // Change 1 (ORDERS SCN 510): ADDRESS-only update — unused by all aggregates.
    let sync_unused = run_sync_fail_after(&url, 1, &doubles);
    let unused_out = format!(
        "{}{}",
        String::from_utf8_lossy(&sync_unused.stdout),
        String::from_utf8_lossy(&sync_unused.stderr)
    );
    assert!(
        unused_out.to_ascii_lowercase().contains("affect")
            && (unused_out.to_ascii_lowercase().contains("skip")
                || unused_out.to_ascii_lowercase().contains("unused")),
        "unused-field change must surface Affect Analysis skip, got:\n{unused_out}"
    );

    let derived_after_unused = derived_stdout(&url);
    assert!(
        derived_after_unused.contains("52.50")
            && derived_after_unused.contains("26.25")
            && derived_after_unused.contains("5.00"),
        "unused ADDRESS change must not recompute Derived aggregates, got:\n{derived_after_unused}"
    );
    let status_after_unused = status_stdout(&url);
    let applied_after_unused = delivery_applied_changes(&status_after_unused, "order-stats")
        .expect("Delivery Health after unused change");
    assert_eq!(
        applied_after_unused, applied_initial,
        "unused-field change must not Deliver; before={applied_initial} after={applied_after_unused}:\n{status_after_unused}"
    );

    // Change 2 (ORDERS SCN 520): AMOUNT 42.50→50.00 → customer 1 max/avg/sum update.
    let sync_used = run_sync_fail_after(&url, 1, &doubles);
    let used_out = format!(
        "{}{}",
        String::from_utf8_lossy(&sync_used.stdout),
        String::from_utf8_lossy(&sync_used.stderr)
    );
    assert!(
        used_out.to_ascii_lowercase().contains("affect") || used_out.contains("order-stats"),
        "used-field sync should mention Affect Analysis / Pipeline, got:\n{used_out}"
    );

    let derived_after_used = derived_stdout(&url);
    assert!(
        derived_after_used.contains("\"ORDER_COUNT\": 2")
            && derived_after_used.contains("10.00")
            && derived_after_used.contains("50.00")
            && derived_after_used.contains("30.00")
            && derived_after_used.contains("60.00"),
        "AMOUNT change must update customer 1 to count=2 min=10 max=50 avg=30 sum=60, got:\n{derived_after_used}"
    );
    assert!(
        derived_after_used.contains("5.00"),
        "customer 2 aggregates must remain 5.00, got:\n{derived_after_used}"
    );
    assert!(
        !derived_after_used.contains("52.50") && !derived_after_used.contains("26.25"),
        "stale customer 1 sum/avg must be gone, got:\n{derived_after_used}"
    );

    let target_out = target_stdout(&url);
    assert!(
        target_out.contains("60.00") || target_out.contains("60"),
        "Delivery must upsert affected identity TOTAL_AMOUNT=60, got:\n{target_out}"
    );
    assert!(
        target_out.contains("30.00") || target_out.contains("\"AVG_AMOUNT\": 30"),
        "Delivery must upsert AVG_AMOUNT=30, got:\n{target_out}"
    );

    let status_after_used = status_stdout(&url);
    let applied_after_used = delivery_applied_changes(&status_after_used, "order-stats")
        .expect("Delivery Health after used change");
    assert_eq!(
        applied_after_used,
        applied_initial + 1,
        "Delivery must update only the affected Output Identity (+1), before={applied_initial} after={applied_after_used}:\n{status_after_used}"
    );
}
