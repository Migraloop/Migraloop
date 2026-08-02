//! Operator-visible seam: Direct Pipeline Delivery Base → MongoDB.
//!
//! Agreed seam (issue #7 / PRD): CLI config/status + resulting Target documents.
//! Output Identity from source PK; updates write Managed fields only; inserts are
//! identity + Managed fields.

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

#[tokio::test]
async fn after_initial_load_mongo_documents_exist_with_output_identity_from_source_pk() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_delivery("CUSTOMERS", "customers", &mongo_database),
    );

    migrate_and_apply(&url, &config);

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("run status");
    assert!(
        status.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    let status_out = String::from_utf8_lossy(&status.stdout);
    assert!(
        status_out.contains("Delivery")
            && (status_out.contains("delivered")
                || status_out.contains("complete")
                || status_out.contains("ok")),
        "expected Delivery progress in status, got:\n{status_out}"
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
        .expect("run target");
    assert!(
        target.status.success(),
        "target inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&target.stdout),
        String::from_utf8_lossy(&target.stderr)
    );
    let stdout = String::from_utf8_lossy(&target.stdout);

    // Output Identity defaults from source PK (CUSTOMERS.ID) → Mongo _id.
    assert!(
        stdout.contains("\"_id\": 1") || stdout.contains("\"_id\":1"),
        "expected Output Identity _id=1 from source PK, got:\n{stdout}"
    );
    assert!(
        stdout.contains("\"_id\": 2") || stdout.contains("\"_id\":2"),
        "expected Output Identity _id=2 from source PK, got:\n{stdout}"
    );
    assert!(
        stdout.contains("Alice") && stdout.contains("alice@example.com"),
        "expected Managed field values from Base row, got:\n{stdout}"
    );
}

fn seed_mongo_document(database: &str, collection: &str, document_json: &str) {
    // Operator/test Target fixture via Mongo wire protocol (pymongo). Delivery itself
    // never replaces whole documents — only `$set`s Managed fields.
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
async fn managed_field_upsert_preserves_non_managed_target_fields() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_delivery("CUSTOMERS", "customers", &mongo_database),
    );

    // Pre-seed identity 1 with stale Managed values plus a non-Managed field.
    seed_mongo_document(
        &mongo_database,
        "customers",
        r#"{"_id": 1, "NAME": "Stale", "EMAIL": "stale@example.com", "ACTIVE": 0, "EXTRA": "keep-me"}"#,
    );

    migrate_and_apply(&url, &config);

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
        .expect("run target");
    assert!(
        target.status.success(),
        "target inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&target.stdout),
        String::from_utf8_lossy(&target.stderr)
    );
    let stdout = String::from_utf8_lossy(&target.stdout);

    assert!(
        stdout.contains("\"_id\": 1") || stdout.contains("\"_id\":1"),
        "expected document _id=1, got:\n{stdout}"
    );
    assert!(
        stdout.contains("Alice") && stdout.contains("alice@example.com"),
        "Managed fields must upsert to Base values, got:\n{stdout}"
    );
    assert!(
        !stdout.contains("Stale") && !stdout.contains("stale@example.com"),
        "stale Managed values must be overwritten, got:\n{stdout}"
    );
    assert!(
        stdout.contains("keep-me") || stdout.contains("EXTRA"),
        "non-Managed Target field EXTRA must not be cleared, got:\n{stdout}"
    );
}

#[tokio::test]
async fn existing_base_without_target_can_later_deliver_with_output_identity() {
    // #6 → #7 path: Initial Load into Base first, then apply Target Binding for Delivery.
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");

    let base_only = write_config(
        &dir,
        "base-only.yaml",
        &format!(
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
"#,
            host = mongo_host(),
            port = mongo_port(),
        ),
    );
    migrate_and_apply(&url, &base_only);

    // Simulate a pre-Delivery Base (migration default / older slice) with empty PK metadata.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect Platform Store");
    sqlx::query(
        "UPDATE base_datasets SET primary_key_json = '[]' WHERE source_table = 'CUSTOMERS'",
    )
    .execute(&pool)
    .await
    .expect("clear primary key metadata");

    let with_delivery = write_config(
        &dir,
        "with-delivery.yaml",
        &deployment_with_direct_delivery("CUSTOMERS", "customers", &mongo_database),
    );
    let apply = Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "apply",
            "--platform-store-url",
            &url,
            "--file",
            with_delivery.to_str().unwrap(),
        ])
        .output()
        .expect("re-apply with Target Binding");
    assert!(
        apply.status.success(),
        "Delivery apply after existing Base failed: stdout={} stderr={}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
    let apply_out = String::from_utf8_lossy(&apply.stdout);
    assert!(
        apply_out.contains("Delivery complete"),
        "expected Delivery after Target Binding apply, got:\n{apply_out}"
    );
    assert!(
        !apply_out.contains("Initial Load complete"),
        "existing Base must not reload, got:\n{apply_out}"
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
        .expect("run target");
    assert!(target.status.success());
    let stdout = String::from_utf8_lossy(&target.stdout);
    assert!(
        (stdout.contains("\"_id\": 1") || stdout.contains("\"_id\":1"))
            && stdout.contains("Alice"),
        "Output Identity from source PK must work for pre-existing Base, got:\n{stdout}"
    );
}

#[tokio::test]
async fn new_identities_insert_identity_and_managed_fields_only() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_delivery("CUSTOMERS", "customers", &mongo_database),
    );

    migrate_and_apply(&url, &config);

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
        .expect("run target");
    assert!(target.status.success());
    let stdout = String::from_utf8_lossy(&target.stdout);

    // New identities from Initial Load must not invent unrelated Target fields.
    for forbidden in ["EXTRA", "invented", "unrelated", "BIO", "blob-bytes"] {
        assert!(
            !stdout.contains(forbidden),
            "insert must be identity + Managed fields only; found {forbidden} in:\n{stdout}"
        );
    }

    // Known Managed fields from Direct Pipeline Base columns must be present.
    for field in ["NAME", "EMAIL", "ACTIVE", "ID"] {
        assert!(
            stdout.contains(field),
            "expected Managed field {field} on insert, got:\n{stdout}"
        );
    }
    assert!(
        stdout.contains("\"_id\"") && stdout.contains("Bob"),
        "expected new identity documents from Base rows, got:\n{stdout}"
    );
}
