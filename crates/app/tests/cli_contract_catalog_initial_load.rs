//! Operator-seam proof (issue #40): contract Oracle discovery + Initial Load for a
//! table outside the hard-coded named fixture list (CUSTOMERS/ORDERS/EVENTS/ACCOUNTS).
//!
//! Named scenario fixtures remain for other tests; this case injects an arbitrary
//! schema via `MIGRALOOP_CONTRACT_SOURCE_CATALOG` and drives apply → Base → Delivery.

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

fn widgets_catalog_json() -> String {
    r#"{
  "tables": [
    {
      "table": "WIDGETS",
      "low_watermark": 9000,
      "primary_key": ["WID"],
      "columns": [
        {
          "name": "WID",
          "oracle_type": "NUMBER",
          "supported": true,
          "precision": 10,
          "scale": 0
        },
        {
          "name": "LABEL",
          "oracle_type": "VARCHAR2",
          "supported": true
        },
        {
          "name": "PHOTO",
          "oracle_type": "BLOB",
          "supported": false
        }
      ],
      "rows": [
        {
          "WID": 1,
          "LABEL": "alpha",
          "PHOTO": "blob-bytes-alpha"
        },
        {
          "WID": 2,
          "LABEL": "beta",
          "PHOTO": "blob-bytes-beta"
        }
      ]
    }
  ]
}
"#
    .to_string()
}

fn deployment_widgets(mongo_database: &str) -> String {
    format!(
        r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: contract-widgets
spec:
  source:
    kind: oracle
    host: contract
    port: 1521
    database: ORCL
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
    - name: widgets
      mode: direct
      source:
        table: WIDGETS
      target:
        collection: widgets
"#,
        host = mongo_host(),
        port = mongo_port(),
    )
}

fn migrate_and_apply(url: &str, config: &Path, catalog_path: &Path) {
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
        .env(
            "MIGRALOOP_CONTRACT_SOURCE_CATALOG",
            catalog_path.to_str().unwrap(),
        )
        // Injected WIDGETS must pass table supplemental-logging probe.
        .env("MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING", "all")
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
    let stdout = String::from_utf8_lossy(&apply.stdout);
    assert!(
        stdout.contains("Initial Load complete") && stdout.contains("WIDGETS"),
        "expected Initial Load for injected WIDGETS, got:\n{stdout}"
    );
}

#[tokio::test]
async fn contract_injected_table_discovers_initial_loads_base_and_delivers() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let catalog = write_config(&dir, "widgets-catalog.json", &widgets_catalog_json());
    let config = write_config(&dir, "deployment.yaml", &deployment_widgets(&mongo_database));

    migrate_and_apply(&url, &config, &catalog);

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
        status_out.contains("Pipeline: widgets") && status_out.to_lowercase().contains("direct"),
        "expected Direct Pipeline widgets in status, got:\n{status_out}"
    );
    assert!(
        status_out.contains("Base Dataset: WIDGETS")
            && status_out.contains("initial_load_complete"),
        "expected WIDGETS Base Dataset after Initial Load, got:\n{status_out}"
    );
    assert!(
        status_out.contains("PHOTO")
            && (status_out.contains("BLOB")
                || status_out.to_lowercase().contains("omitted")
                || status_out.to_lowercase().contains("unsupported")),
        "unsupported PHOTO/BLOB must be operator-visible, got:\n{status_out}"
    );

    let base = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", "WIDGETS"])
        .output()
        .expect("run base");
    assert!(
        base.status.success(),
        "base inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&base.stdout),
        String::from_utf8_lossy(&base.stderr)
    );
    let base_out = String::from_utf8_lossy(&base.stdout);
    for col in ["WID", "LABEL"] {
        assert!(
            base_out.contains(col),
            "Base must keep supported column {col}, got:\n{base_out}"
        );
    }
    assert!(
        base_out.contains("alpha") && base_out.contains("beta"),
        "expected injected row labels in Base, got:\n{base_out}"
    );
    assert!(
        !base_out.to_lowercase().contains("blob-bytes"),
        "unsupported PHOTO payload must be omitted from Base rows, got:\n{base_out}"
    );

    let target = Command::new(bin())
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "target",
            "--platform-store-url",
            &url,
            "--collection",
            "widgets",
        ])
        .output()
        .expect("run target");
    assert!(
        target.status.success(),
        "target inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&target.stdout),
        String::from_utf8_lossy(&target.stderr)
    );
    let target_out = String::from_utf8_lossy(&target.stdout);
    assert!(
        target_out.contains("\"_id\": 1") || target_out.contains("\"_id\":1"),
        "expected Output Identity from WID PK, got:\n{target_out}"
    );
    assert!(
        target_out.contains("alpha"),
        "expected Managed LABEL delivered to Mongo, got:\n{target_out}"
    );
    assert!(
        !target_out.to_lowercase().contains("blob-bytes") && !target_out.contains("PHOTO"),
        "unsupported PHOTO must not be delivered, got:\n{target_out}"
    );
}

#[tokio::test]
async fn contract_unknown_non_catalog_table_fails_clearly_without_stub_vocabulary() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.yaml",
        r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: missing-table
spec:
  source:
    kind: oracle
    host: contract
    port: 1521
    database: ORCL
    username: sync_user
    password:
      fromEnv: ORACLE_PASSWORD
  target:
    kind: mongodb
    host: mongo.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
  pipelines:
    - name: missing
      mode: direct
      source:
        table: NOT_IN_CATALOG
"#,
    );

    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", &url])
        .output()
        .expect("migrate");
    assert!(migrate.status.success());

    let apply = Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .env("MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING", "NOT_IN_CATALOG")
        .args([
            "apply",
            "--platform-store-url",
            &url,
            "--file",
            config.to_str().unwrap(),
        ])
        .output()
        .expect("apply");
    assert!(
        !apply.status.success(),
        "apply must fail for unknown Source table"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    )
    .to_ascii_lowercase();
    assert!(
        combined.contains("unknown source table") && combined.contains("not_in_catalog"),
        "expected clear missing-Source-table error, got:\n{combined}"
    );
    assert!(
        !combined.contains("unknown stub"),
        "must not use stub-catalog vocabulary as long-term UX, got:\n{combined}"
    );
}
