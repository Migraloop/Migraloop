//! Operator-seam proof (issue #120): product runtime does not ship a hard-coded
//! business-table stub catalog. `host: stub` / `contract` without an injected
//! catalog treats CUSTOMERS like any missing Source table.

mod common;

use std::fs;
use std::path::PathBuf;
use std::process::Command;

use tempfile::TempDir;

fn admin_url() -> String {
    std::env::var("MIGRALOOP_TEST_ADMIN_URL").unwrap_or_else(|_| {
        "postgres://migraloop:migraloop@127.0.0.1:5432/postgres".to_string()
    })
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

fn customers_deployment() -> String {
    r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: no-default-stub
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
    host: mongo.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
  pipelines:
    - name: customers
      mode: direct
      source:
        table: CUSTOMERS
"#
    .to_string()
}

#[tokio::test]
async fn apply_without_injected_catalog_fails_for_former_fixture_table() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(&dir, "deployment.yaml", &customers_deployment());

    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", &url])
        .output()
        .expect("migrate");
    assert!(
        migrate.status.success(),
        "migrate failed: {}",
        String::from_utf8_lossy(&migrate.stderr)
    );

    // Deliberately omit MIGRALOOP_CONTRACT_SOURCE_CATALOG — product path must
    // not auto-load CUSTOMERS/ORDERS/EVENTS/ACCOUNTS from an in-binary catalog.
    let apply = Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .env("MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING", "CUSTOMERS")
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
        "apply must fail when CUSTOMERS is not injected; got success:\n{}",
        String::from_utf8_lossy(&apply.stdout)
    );

    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    )
    .to_ascii_lowercase();
    assert!(
        combined.contains("unknown source table") && combined.contains("customers"),
        "expected clear missing-Source-table error, got:\n{combined}"
    );
    assert!(
        !combined.contains("unknown stub"),
        "must not imply a fixed stub-catalog model, got:\n{combined}"
    );
    for fixture_name in ["orders", "events", "accounts"] {
        assert!(
            !combined.contains(fixture_name),
            "error must not advertise other hard-coded fixture names ({fixture_name}), got:\n{combined}"
        );
    }
}

#[tokio::test]
async fn injected_named_scenario_doubles_still_work_on_operator_seam() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(&dir, "deployment.yaml", &customers_deployment());

    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", &url])
        .output()
        .expect("migrate");
    assert!(migrate.status.success());

    let mut apply = Command::new(bin());
    apply
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value");
    doubles.apply_env(&mut apply);
    let apply = apply
        .args([
            "apply",
            "--platform-store-url",
            &url,
            "--file",
            config.to_str().unwrap(),
        ])
        .output()
        .expect("apply with injected doubles");
    assert!(
        apply.status.success(),
        "injectable named-scenario doubles must still work: stdout={} stderr={}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );

    let base = Command::new(bin())
        .args([
            "base",
            "--platform-store-url",
            &url,
            "--table",
            "CUSTOMERS",
        ])
        .output()
        .expect("base");
    assert!(base.status.success());
    let stdout = String::from_utf8_lossy(&base.stdout);
    assert!(
        stdout.contains("Alice") && stdout.contains("ID"),
        "expected injected CUSTOMERS fixture rows in Base, got:\n{stdout}"
    );
}
