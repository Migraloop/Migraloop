//! Operator-visible seam: idempotent / duplicate-safe re-Delivery (issue #96 / Lab `idempotent-redelivery`).
//!
//! Agreed seam: CLI apply/sync/target + Platform Store Delivery status orchestration.
//! After Incremental Capture settles Managed Target outcomes, reset Pipeline Delivery
//! status to pending and re-apply so the product path re-Delivers the same Output
//! Identities (at-least-once upsert). Document count stays stable; non-Managed Target
//! fields survive Managed-field re-Delivery.
//!
//! This is the non-ignored contract/stub CI twin of Lab Scenario `idempotent-redelivery`.
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

fn deployment_with_direct_delivery(table: &str, collection: &str, mongo_database: &str) -> String {
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
        table: {table}
      target:
        collection: {collection}
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

fn apply_again(url: &str, config: &Path, doubles: &common::NamedScenarioDoubles) -> String {
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
        .expect("run re-apply");

    assert!(
        apply.status.success(),
        "re-apply failed: stdout={} stderr={}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
    String::from_utf8_lossy(&apply.stdout).into_owned()
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

fn parse_target_document_count(inspect: &str) -> Option<u64> {
    for line in inspect.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("documents:") {
            return rest.trim().parse().ok();
        }
    }
    None
}

fn count_delivery_document_ops(product_out: &str) -> u64 {
    let mut total = 0u64;
    for line in product_out.lines() {
        let Some(start) = line.find('(') else {
            continue;
        };
        let rest = &line[start + 1..];
        let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
        if digits.is_empty() || !rest[digits.len()..].starts_with(" documents)") {
            continue;
        }
        if let Ok(n) = digits.parse::<u64>() {
            total += n;
        }
    }
    total
}

fn seed_mongo_operator_note(database: &str, collection: &str, identity: i64, note: &str) {
    let status = Command::new("python3")
        .args([
            "-c",
            &format!(
                r#"
from pymongo import MongoClient
c = MongoClient(
    "mongodb://deliver_user:mongo-secret-value@{host}:{port}/{database}?authSource=admin",
    serverSelectionTimeoutMS=5000,
)
c["{database}"]["{collection}"].update_one(
    {{"_id": {identity}}},
    {{"$set": {{"operatorNote": "{note}"}}}},
)
print("planted", {identity})
"#,
                host = mongo_host(),
                port = mongo_port(),
                database = database,
                collection = collection,
                identity = identity,
                note = note,
            ),
        ])
        .status()
        .expect("run pymongo plant");
    assert!(status.success(), "failed to plant non-Managed operatorNote");
}

#[tokio::test]
async fn duplicate_safe_redelivery_keeps_managed_outcomes_and_non_managed_fields() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_delivery("CUSTOMERS", "customers", &mongo_database),
    );

    let apply_out = migrate_and_apply(&url, &config, &doubles);
    assert!(
        apply_out.contains("Initial Load") || apply_out.to_ascii_lowercase().contains("initial"),
        "first apply must Initial Load, got:\n{apply_out}"
    );
    assert!(
        apply_out.to_ascii_lowercase().contains("delivery"),
        "first apply must Deliver, got:\n{apply_out}"
    );

    let sync_out = run_sync(&url, &doubles);
    assert!(
        sync_out.to_ascii_lowercase().contains("incremental")
            || sync_out.to_ascii_lowercase().contains("sync"),
        "sync should report Incremental Capture, got:\n{sync_out}"
    );

    let target_before = target_stdout(&url, "customers");
    let docs_before = parse_target_document_count(&target_before)
        .expect("target inspect must report documents: N before re-Delivery");
    assert_eq!(
        docs_before, 2,
        "after stub I/U/D Target must have Alicia+Carol only, got:\n{target_before}"
    );
    assert!(
        target_before.contains("Alicia") && target_before.contains("Carol"),
        "pre-redelivery Managed baseline missing Alicia/Carol, got:\n{target_before}"
    );
    assert!(
        !target_before.contains("Bob"),
        "pre-redelivery Managed baseline must not include deleted Bob, got:\n{target_before}"
    );

    const OPERATOR_NOTE: &str = "ci-keep-across-redelivery";
    seed_mongo_operator_note(&mongo_database, "customers", 1, OPERATOR_NOTE);

    // Test orchestration only (same idea as Lab Scenario): mark Delivery pending so the
    // next product `apply` re-Delivers current Base Output Identities.
    {
        let store = migraloop_platform_store::PlatformStore::open(&url)
            .await
            .expect("open Platform Store for re-Delivery exercise");
        store
            .record_delivery_progress(
                "oracle-to-mongo",
                "customers",
                Some("pending"),
                None,
                None,
            )
            .await
            .expect("reset Pipeline Delivery status for re-Delivery exercise");
    }

    let reapply_out = apply_again(&url, &config, &doubles);
    assert!(
        reapply_out.to_ascii_lowercase().contains("delivery"),
        "re-apply must perform duplicate-safe re-Delivery, got:\n{reapply_out}"
    );
    assert!(
        !reapply_out.contains("Initial Load")
            && !reapply_out.to_ascii_lowercase().contains("initial_load"),
        "re-apply must not reload existing Base, got:\n{reapply_out}"
    );
    let redelivery_ops = count_delivery_document_ops(&reapply_out);
    assert!(
        redelivery_ops >= 2,
        "re-apply must re-Deliver current Base Output Identities (expected ≥2 docs, got {redelivery_ops}):\n{reapply_out}"
    );

    let target_after = target_stdout(&url, "customers");
    let docs_after = parse_target_document_count(&target_after)
        .expect("target inspect must report documents: N after re-Delivery");
    assert_eq!(
        docs_after, docs_before,
        "document count must stay stable across duplicate-safe re-Delivery:\nbefore:\n{target_before}\nafter:\n{target_after}"
    );
    assert!(
        target_after.contains("Alicia") && target_after.contains("Carol"),
        "Managed outcomes must remain Alicia+Carol after re-Delivery, got:\n{target_after}"
    );
    assert!(
        !target_after.contains("Bob"),
        "deleted identity must stay absent after re-Delivery, got:\n{target_after}"
    );
    assert!(
        target_after.contains(OPERATOR_NOTE),
        "non-Managed operatorNote must survive Managed-field re-Delivery, got:\n{target_after}"
    );
}
