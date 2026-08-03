//! Operator-visible seam: fail-fast Oracle Source Prerequisites (#12 / ADR-0021).
//!
//! Agreed seam (PRD Testing Decisions / issue #12): CLI apply/sync fail with a clear
//! pre-run error when Source Prerequisites are unmet; satisfied checks allow proceed.
//! The platform never auto-alters Source System settings to "fix" prerequisites.
//! Stub Source simulates Oracle probe state via env (same pattern as timezone stub).

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

fn deployment_config(mongo_database: &str) -> String {
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
"#,
        host = mongo_host(),
        port = mongo_port(),
    )
}

fn migrate(url: &str) {
    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", url])
        .output()
        .expect("run migrate");
    assert!(
        migrate.status.success(),
        "migrate failed: {}",
        String::from_utf8_lossy(&migrate.stderr)
    );
}

fn apply_with_env(
    url: &str,
    config: &Path,
    doubles: &common::NamedScenarioDoubles,
    extra_env: &[(&str, &str)],
) -> std::process::Output {
    let mut cmd = Command::new(bin());
    cmd.env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        // Default stub prereqs are satisfied unless a test overrides them.
        .env_remove("MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING")
        .env_remove("MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING")
        .env_remove("MIGRALOOP_STUB_REDO_RETENTION_HOURS");
    doubles.apply_env(&mut cmd);
    cmd.args([
        "apply",
        "--platform-store-url",
        url,
        "--file",
        config.to_str().unwrap(),
    ]);
    for (key, value) in extra_env {
        cmd.env(key, value);
    }
    cmd.output().expect("run apply")
}

fn sync_with_env(
    url: &str,
    doubles: &common::NamedScenarioDoubles,
    extra_env: &[(&str, &str)],
) -> std::process::Output {
    let mut cmd = Command::new(bin());
    cmd.env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .env_remove("MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING")
        .env_remove("MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING")
        .env_remove("MIGRALOOP_STUB_REDO_RETENTION_HOURS");
    doubles.apply_env(&mut cmd);
    cmd.args(["sync", "--platform-store-url", url]);
    for (key, value) in extra_env {
        cmd.env(key, value);
    }
    cmd.output().expect("run sync")
}

fn combined_output(output: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[tokio::test]
async fn apply_fails_fast_when_database_supplemental_logging_missing() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let mongo_db = unique_mongo_database();
    let config = write_config(&dir, "deployment.yaml", &deployment_config(&mongo_db));
    migrate(&url);

    let apply = apply_with_env(
        &url,
        &config,
        &doubles,
        &[("MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING", "off")],
    );
    let text = combined_output(&apply);
    assert!(
        !apply.status.success(),
        "apply must fail-fast when supplemental logging is off, got:\n{text}"
    );
    assert!(
        text.to_lowercase().contains("prerequisite")
            || text.to_lowercase().contains("prerequisites"),
        "error must mention Source Prerequisites, got:\n{text}"
    );
    assert!(
        text.to_lowercase().contains("supplemental logging"),
        "error must name the missing prerequisite, got:\n{text}"
    );
    assert!(
        !text.contains("Initial Load complete"),
        "capture must not proceed past the pre-run check, got:\n{text}"
    );
    assert!(
        text.to_lowercase().contains("does not automatically alter"),
        "error must state the platform will not auto-alter Source settings, got:\n{text}"
    );
}

#[tokio::test]
async fn apply_fails_fast_when_table_supplemental_logging_missing() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let mongo_db = unique_mongo_database();
    let config = write_config(&dir, "deployment.yaml", &deployment_config(&mongo_db));
    migrate(&url);

    // Database-level OK; Pipeline-referenced CUSTOMERS missing PK/ALL supplemental logging.
    let apply = apply_with_env(
        &url,
        &config,
        &doubles,
        &[("MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING", "ORDERS")],
    );
    let text = combined_output(&apply);
    assert!(
        !apply.status.success(),
        "apply must fail when table supplemental logging is missing, got:\n{text}"
    );
    assert!(
        text.contains("CUSTOMERS"),
        "error must name the affected table, got:\n{text}"
    );
    assert!(
        text.to_lowercase().contains("supplemental logging"),
        "error must mention supplemental logging, got:\n{text}"
    );
    assert!(
        !text.contains("Initial Load complete"),
        "must fail before Initial Load, got:\n{text}"
    );
}

#[tokio::test]
async fn apply_fails_fast_when_redo_retention_insufficient() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let mongo_db = unique_mongo_database();
    let config = write_config(&dir, "deployment.yaml", &deployment_config(&mongo_db));
    migrate(&url);

    let apply = apply_with_env(
        &url,
        &config,
        &doubles,
        &[("MIGRALOOP_STUB_REDO_RETENTION_HOURS", "1")],
    );
    let text = combined_output(&apply);
    assert!(
        !apply.status.success(),
        "apply must fail when redo retention is insufficient, got:\n{text}"
    );
    assert!(
        text.to_lowercase().contains("redo") || text.to_lowercase().contains("retention"),
        "error must mention redo retention, got:\n{text}"
    );
    assert!(
        !text.contains("Initial Load complete"),
        "must fail before Initial Load, got:\n{text}"
    );
}

#[tokio::test]
async fn apply_proceeds_when_source_prerequisites_satisfied() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let mongo_db = unique_mongo_database();
    let config = write_config(&dir, "deployment.yaml", &deployment_config(&mongo_db));
    migrate(&url);

    let apply = apply_with_env(
        &url,
        &config,
        &doubles,
        &[
            ("MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING", "on"),
            ("MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING", "all"),
            ("MIGRALOOP_STUB_REDO_RETENTION_HOURS", "72"),
        ],
    );
    let text = combined_output(&apply);
    assert!(
        apply.status.success(),
        "satisfied prerequisites must allow apply to proceed, got:\n{text}"
    );
    assert!(
        text.contains("Initial Load complete"),
        "Initial Load should run after prerequisites pass, got:\n{text}"
    );
    assert!(
        text.contains("Deployment applied: oracle-to-mongo"),
        "Deployment apply should complete, got:\n{text}"
    );
}

#[tokio::test]
async fn sync_fails_fast_when_prerequisites_become_unmet() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let mongo_db = unique_mongo_database();
    let config = write_config(&dir, "deployment.yaml", &deployment_config(&mongo_db));
    migrate(&url);

    let apply = apply_with_env(&url, &config, &doubles, &[]);
    assert!(
        apply.status.success(),
        "baseline apply with default satisfied prereqs failed: {}",
        combined_output(&apply)
    );

    let sync = sync_with_env(
        &url,
        &doubles,
        &[("MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING", "off")],
    );
    let text = combined_output(&sync);
    assert!(
        !sync.status.success(),
        "sync must fail-fast when prerequisites become unmet, got:\n{text}"
    );
    assert!(
        text.to_lowercase().contains("prerequisite")
            || text.to_lowercase().contains("prerequisites"),
        "sync error must mention Source Prerequisites, got:\n{text}"
    );
    assert!(
        !text.contains("Incremental Capture: resuming"),
        "Incremental Capture must not proceed past the pre-run check, got:\n{text}"
    );
}

#[tokio::test]
async fn unmet_prerequisites_are_not_auto_fixed_by_platform() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let mongo_db = unique_mongo_database();
    let config = write_config(&dir, "deployment.yaml", &deployment_config(&mongo_db));
    migrate(&url);

    let first = apply_with_env(
        &url,
        &config,
        &doubles,
        &[("MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING", "off")],
    );
    let first_text = combined_output(&first);
    assert!(!first.status.success(), "first apply should fail:\n{first_text}");
    assert!(
        first_text.to_lowercase().contains("does not alter")
            || first_text.to_lowercase().contains("will not alter")
            || first_text.to_lowercase().contains("does not automatically")
            || first_text.to_lowercase().contains("will not automatically"),
        "error should state the platform does not auto-alter Source settings, got:\n{first_text}"
    );

    // Same unmet stub state: platform must not have "fixed" the Source between attempts.
    let second = apply_with_env(
        &url,
        &config,
        &doubles,
        &[("MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING", "off")],
    );
    let second_text = combined_output(&second);
    assert!(
        !second.status.success(),
        "re-apply must still fail; platform must not auto-mutate Source settings, got:\n{second_text}"
    );
    assert!(
        second_text.to_lowercase().contains("supplemental logging"),
        "second failure must still report the unmet prerequisite, got:\n{second_text}"
    );
}
