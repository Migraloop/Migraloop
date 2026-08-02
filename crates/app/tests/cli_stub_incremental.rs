//! Operator-visible seam: LogMiner (contract) Incremental Capture updates Base then Mongo.
//!
//! Agreed seam (issue #8 / #13 / PRD): CLI config/status + resulting Base/Target outcomes.
//! Insert/update/delete-style LogMiner contract changes apply identity-keyed and
//! Managed-field scoped for a Direct Pipeline. Sync/Delivery progress is visible in status.
//! Cutover / restart resume are covered by dedicated seam tests.

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

fn migrate_and_apply(url: &str, config: &Path) {
    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", url])
        .output()
        .expect("run migrate");
    assert!(
        migrate.status.success(),
        "migrate failed: {}",
        String::from_utf8_lossy(&migrate.stderr)
    );

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
}

fn run_sync(url: &str) -> std::process::Output {
    Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args(["sync", "--platform-store-url", url])
        .output()
        .expect("run sync")
}

fn seed_mongo_document(database: &str, collection: &str, document_json: &str) {
    let status = Command::new("python3")
        .args([
            "-c",
            &format!(
                r#"
from pymongo import MongoClient
import json
c = MongoClient(
    "mongodb://deliver_user:mongo-secret-value@{host}:{port}/{database}?authSource=admin",
    serverSelectionTimeoutMS=5000,
)
doc = json.loads('''{document_json}''')
c["{database}"]["{collection}"].replace_one({{"_id": doc["_id"]}}, doc, upsert=True)
print("seeded", doc["_id"])
"#,
                host = mongo_host(),
                port = mongo_port(),
                database = database,
                collection = collection,
                document_json = document_json,
            ),
        ])
        .status()
        .expect("run pymongo seed");
    assert!(status.success(), "failed to seed Mongo document");
}

#[tokio::test]
async fn stub_incremental_insert_update_delete_update_base_then_mongo() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_delivery("CUSTOMERS", "customers", &mongo_database),
    );

    migrate_and_apply(&url, &config);

    // Baseline after Initial Load: Alice (1), Bob (2), and overlap Carol (3).
    let base_before = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", "CUSTOMERS"])
        .output()
        .expect("base before sync");
    assert!(base_before.status.success());
    let before = String::from_utf8_lossy(&base_before.stdout);
    assert!(
        before.contains("Alice") && before.contains("Bob") && before.contains("Carol"),
        "expected Initial Load rows before incremental, got:\n{before}"
    );

    let sync = run_sync(&url);
    assert!(
        sync.status.success(),
        "sync failed: stdout={} stderr={}",
        String::from_utf8_lossy(&sync.stdout),
        String::from_utf8_lossy(&sync.stderr)
    );
    let sync_out = String::from_utf8_lossy(&sync.stdout);
    assert!(
        sync_out.to_ascii_lowercase().contains("incremental")
            || sync_out.to_ascii_lowercase().contains("sync"),
        "expected Incremental Capture progress in sync output, got:\n{sync_out}"
    );

    // Stub batch: update Alice→Alicia, insert Carol (3), delete Bob (2).
    let base_after = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", "CUSTOMERS"])
        .output()
        .expect("base after sync");
    assert!(base_after.status.success());
    let base_out = String::from_utf8_lossy(&base_after.stdout);
    assert!(
        base_out.contains("Alicia") && base_out.contains("alicia@example.com"),
        "Base must apply stub update for Output Identity 1, got:\n{base_out}"
    );
    assert!(
        base_out.contains("Carol") && base_out.contains("carol@example.com"),
        "Base must apply stub insert for Output Identity 3, got:\n{base_out}"
    );
    assert!(
        !base_out.contains("Bob") && !base_out.contains("bob@example.com"),
        "Base must apply stub delete for Output Identity 2, got:\n{base_out}"
    );
    // Unsupported BIO stays listed as omitted metadata, but must not appear in row payloads.
    assert!(
        !base_out.contains("blob-bytes")
            && !base_out.contains("\"BIO\"")
            && base_out.contains("omittedUnsupported")
            && base_out.contains("BIO (BLOB)"),
        "unsupported columns must be omitted from Base rows with visibility, got:\n{base_out}"
    );

    let target = Command::new(bin())
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "target",
            "--platform-store-url",
            &url,
            "--collection",
            "customers",
        ])
        .output()
        .expect("target after sync");
    assert!(
        target.status.success(),
        "target inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&target.stdout),
        String::from_utf8_lossy(&target.stderr)
    );
    let target_out = String::from_utf8_lossy(&target.stdout);

    assert!(
        (target_out.contains("\"_id\": 1") || target_out.contains("\"_id\":1"))
            && target_out.contains("Alicia"),
        "Mongo must upsert Managed fields for identity 1, got:\n{target_out}"
    );
    assert!(
        (target_out.contains("\"_id\": 3") || target_out.contains("\"_id\":3"))
            && target_out.contains("Carol"),
        "Mongo must insert identity + Managed fields for identity 3, got:\n{target_out}"
    );
    assert!(
        !(target_out.contains("\"_id\": 2") || target_out.contains("\"_id\":2"))
            && !target_out.contains("Bob"),
        "Mongo must delete entire document for disappeared identity 2, got:\n{target_out}"
    );
}

#[tokio::test]
async fn incremental_delivery_preserves_non_managed_fields_and_status_shows_progress() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_delivery("CUSTOMERS", "customers", &mongo_database),
    );

    // Non-Managed field on identity 1 must survive Managed-field upsert after incremental update.
    seed_mongo_document(
        &mongo_database,
        "customers",
        r#"{"_id": 1, "NAME": "Stale", "EMAIL": "stale@example.com", "ACTIVE": 0, "EXTRA": "keep-me"}"#,
    );

    migrate_and_apply(&url, &config);
    let sync = run_sync(&url);
    assert!(
        sync.status.success(),
        "sync failed: stdout={} stderr={}",
        String::from_utf8_lossy(&sync.stdout),
        String::from_utf8_lossy(&sync.stderr)
    );

    let target = Command::new(bin())
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "target",
            "--platform-store-url",
            &url,
            "--collection",
            "customers",
        ])
        .output()
        .expect("target after sync");
    assert!(target.status.success());
    let target_out = String::from_utf8_lossy(&target.stdout);
    assert!(
        target_out.contains("Alicia"),
        "Managed fields must update from stub incremental change, got:\n{target_out}"
    );
    assert!(
        target_out.contains("keep-me") || target_out.contains("EXTRA"),
        "non-Managed Target field EXTRA must not be cleared on incremental update, got:\n{target_out}"
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("status after sync");
    assert!(
        status.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    let status_out = String::from_utf8_lossy(&status.stdout);
    let lower = status_out.to_ascii_lowercase();
    assert!(
        lower.contains("sync")
            && (lower.contains("health")
                || lower.contains("incremental")
                || lower.contains("ok")
                || lower.contains("applied")),
        "expected Sync progress/health in status, got:\n{status_out}"
    );
    assert!(
        status_out.contains("Delivery Health")
            && status_out.contains("appliedChanges=")
            && (status_out.contains("delivered") || status_out.contains("ok")),
        "expected Delivery Health progress after incremental Delivery, got:\n{status_out}"
    );
    // Initial Load delivered 3 docs (Alice/Bob/Carol); stub batch Delivers 3 Output Identity
    // applies (2 upsert + 1 delete), including Carol overlap upsert.
    assert!(
        status_out.contains("Delivery Health: ok")
            && status_out.contains("appliedChanges=6"),
        "expected Delivery Health appliedChanges=6 after Initial Load + incremental, got:\n{status_out}"
    );
}
