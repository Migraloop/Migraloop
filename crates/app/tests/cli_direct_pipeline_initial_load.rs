//! Operator-visible seam: Direct Pipeline apply → stub Source Initial Load → Base Dataset.
//!
//! Agreed seam (issue #6 / PRD): verify via CLI config/status and operator-facing Base checks,
//! not private module internals. Stub Source only; no Mongo Delivery.

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

fn deployment_with_direct_pipeline(table: &str) -> String {
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
        table: {table}
"#
    )
}

fn migrate_and_apply(url: &str, config: &Path, doubles: &common::NamedScenarioDoubles) {
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
}

#[tokio::test]
async fn apply_direct_pipeline_initial_loads_referenced_table_into_base() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_pipeline("CUSTOMERS"),
    );

    migrate_and_apply(&url, &config, &doubles);

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("run status");
    assert!(
        status.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    let stdout = String::from_utf8_lossy(&status.stdout);

    assert!(
        stdout.contains("Pipeline: customers") && stdout.to_lowercase().contains("direct"),
        "expected Direct Pipeline in status, got:\n{stdout}"
    );
    assert!(
        stdout.contains("Base Dataset: CUSTOMERS")
            && (stdout.contains("initial_load_complete")
                || stdout.contains("Initial Load complete")),
        "expected table-level Initial Load into Base Dataset, got:\n{stdout}"
    );
}

#[tokio::test]
async fn base_contains_full_supported_type_rows_not_projected_subset() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_pipeline("CUSTOMERS"),
    );

    migrate_and_apply(&url, &config, &doubles);

    let base = Command::new(bin())
        .args([
            "base",
            "--platform-store-url",
            &url,
            "--table",
            "CUSTOMERS",
        ])
        .output()
        .expect("run base");
    assert!(
        base.status.success(),
        "base inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&base.stdout),
        String::from_utf8_lossy(&base.stderr)
    );
    let stdout = String::from_utf8_lossy(&base.stdout);

    // Stub CUSTOMERS has supported columns beyond a minimal identity projection.
    for col in ["ID", "NAME", "EMAIL", "ACTIVE"] {
        assert!(
            stdout.contains(col),
            "Base must keep full supported-type columns including {col}, got:\n{stdout}"
        );
    }
    assert!(
        stdout.contains("Alice") && stdout.contains("alice@example.com"),
        "expected fixture row values in Base, got:\n{stdout}"
    );

    // Row payloads must omit unsupported columns; status/metadata may still name them.
    let mut found_row = false;
    for chunk in stdout.split('{').skip(1) {
        let Some(end) = chunk.find('}') else {
            continue;
        };
        let row_json = format!("{{{}", &chunk[..=end]);
        if !row_json.contains("\"ID\"") {
            continue;
        }
        found_row = true;
        assert!(
            !row_json.contains("BIO") && !row_json.to_lowercase().contains("blob-bytes"),
            "unsupported BLOB column must not appear in Base row JSON, got:\n{row_json}"
        );
        assert!(
            row_json.contains("\"EMAIL\""),
            "supported EMAIL column must remain in Base row JSON, got:\n{row_json}"
        );
    }
    assert!(found_row, "expected at least one Base row JSON object, got:\n{stdout}");
    assert!(
        !stdout.to_lowercase().contains("blob-bytes"),
        "unsupported BIO payload must be omitted from Base rows, got:\n{stdout}"
    );
}

#[tokio::test]
async fn reapply_does_not_reload_existing_base_dataset() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_pipeline("CUSTOMERS"),
    );

    migrate_and_apply(&url, &config, &doubles);

    let mut reapply = Command::new(bin());
    reapply
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value");
    doubles.apply_env(&mut reapply);
    let reapply = reapply
        .args([
            "apply",
            "--platform-store-url",
            &url,
            "--file",
            config.to_str().unwrap(),
        ])
        .output()
        .expect("re-apply");

    assert!(
        reapply.status.success(),
        "re-apply failed: {}",
        String::from_utf8_lossy(&reapply.stderr)
    );
    let stdout = String::from_utf8_lossy(&reapply.stdout);
    assert!(
        !stdout.contains("Initial Load complete"),
        "existing Base must not be reloaded on re-apply, got:\n{stdout}"
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("status");
    let status_out = String::from_utf8_lossy(&status.stdout);
    assert!(
        status_out.contains("Base Dataset: CUSTOMERS")
            && status_out.contains("initial_load_complete"),
        "Base Dataset should remain after re-apply, got:\n{status_out}"
    );
}

#[tokio::test]
async fn unsupported_columns_are_omitted_and_visible_in_status() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_pipeline("CUSTOMERS"),
    );

    migrate_and_apply(&url, &config, &doubles);

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("run status");
    assert!(status.status.success());
    let stdout = String::from_utf8_lossy(&status.stdout);

    assert!(
        stdout.contains("BIO")
            && (stdout.contains("BLOB")
                || stdout.to_lowercase().contains("omitted")
                || stdout.to_lowercase().contains("unsupported")),
        "expected omitted unsupported column surfaced in status, got:\n{stdout}"
    );
}

#[tokio::test]
async fn unreferenced_stub_tables_are_not_captured() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_pipeline("CUSTOMERS"),
    );

    migrate_and_apply(&url, &config, &doubles);

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("run status");
    assert!(status.status.success());
    let stdout = String::from_utf8_lossy(&status.stdout);

    assert!(
        stdout.contains("Base Dataset: CUSTOMERS"),
        "expected CUSTOMERS Base Dataset, got:\n{stdout}"
    );
    assert!(
        !stdout.contains("Base Dataset: ORDERS"),
        "must not whole-schema capture unreferenced ORDERS, got:\n{stdout}"
    );

    let orders = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", "ORDERS"])
        .output()
        .expect("run base for ORDERS");
    assert!(
        !orders.status.success(),
        "inspecting unreferenced ORDERS Base should fail"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&orders.stdout),
        String::from_utf8_lossy(&orders.stderr)
    );
    assert!(
        combined.to_lowercase().contains("not found")
            || combined.to_lowercase().contains("no base"),
        "expected clear missing-Base error for ORDERS, got:\n{combined}"
    );
}
