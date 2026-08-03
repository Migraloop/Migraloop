//! Operator-visible seam: backward-compatible upgrades + store migrations
//! (issue #29 / ADR-0014).
//!
//! Agreed seam (PRD Testing Decisions / issue #3): CLI Deployment runtime —
//! `migrate` / `apply` / `status` / `base` — not private module internals.
//!
//! Seams under test:
//! 1. Newer app migrates Platform Store from a prior schema automatically
//!    (`migraloop migrate`) without wiping existing Deployment / Base data.
//! 2. Older SemVer-compatible config (`apiVersion: migraloop.dev/v1.0.0`)
//!    still applies after upgrade.
//! 3. Upgrade smoke does not require rebuild-from-scratch (no Initial Load
//!    when Base already exists; row data remains).
//!
//! This is the non-ignored contract/stub CI twin of Lab Scenario
//! `backward-compatible-upgrades`. It must not run Lab Fixture / live Oracle.

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

/// Older SemVer-compatible config shape (prior accepted apiVersion form).
fn older_compatible_deployment(mongo_database: &str) -> String {
    format!(
        r#"
apiVersion: migraloop.dev/v1.0.0
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

fn current_deployment(mongo_database: &str) -> String {
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

/// Seed Deployment + Pipeline + Base rows at the prior-release schema cut.
async fn seed_prior_release_deployment_data(database_url: &str) {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
        .expect("connect prior-schema store");

    sqlx::query(
        r#"
        INSERT INTO deployments (
            name,
            source_kind, source_host, source_port, source_database, source_username,
            source_password_ref_kind, source_password_ref_value,
            target_kind, target_host, target_port, target_database, target_username,
            target_password_ref_kind, target_password_ref_value
        ) VALUES (
            'oracle-to-mongo',
            'oracle', 'stub', 1521, 'STUB', 'sync_user',
            'env', 'ORACLE_PASSWORD',
            'mongodb', '127.0.0.1', 27017, 'prior_upgrade_db', 'deliver_user',
            'env', 'MONGO_PASSWORD'
        )
        "#,
    )
    .execute(&pool)
    .await
    .expect("seed prior deployment");

    sqlx::query(
        r#"
        INSERT INTO pipelines (
            deployment_name, name, mode, source_table, source_schema,
            target_collection, delivery_status
        ) VALUES (
            'oracle-to-mongo', 'customers', 'direct', 'CUSTOMERS', '',
            'customers', 'delivered'
        )
        "#,
    )
    .execute(&pool)
    .await
    .expect("seed prior pipeline");

    sqlx::query(
        r#"
        INSERT INTO base_datasets (
            deployment_name, source_table, source_schema, status,
            columns_json, omitted_columns_json, row_count, primary_key_json
        ) VALUES (
            'oracle-to-mongo', 'CUSTOMERS', '', 'initial_load_complete',
            $1, '[]', 2, '["ID"]'
        )
        "#,
    )
    .bind(
        r#"[{"name":"ID","oracle_type":"NUMBER","precision":10,"scale":0},{"name":"NAME","oracle_type":"VARCHAR2"}]"#,
    )
    .execute(&pool)
    .await
    .expect("seed prior base_datasets");

    sqlx::query(
        r#"
        INSERT INTO base_rows (
            deployment_name, source_schema, source_table, row_ordinal, row_json
        ) VALUES
            ('oracle-to-mongo', '', 'CUSTOMERS', 0, '{"ID":1,"NAME":"Alice"}'),
            ('oracle-to-mongo', '', 'CUSTOMERS', 1, '{"ID":2,"NAME":"Bob"}')
        "#,
    )
    .execute(&pool)
    .await
    .expect("seed prior base_rows");
}

fn migrate_cli(url: &str) -> String {
    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", url])
        .output()
        .expect("run migrate");
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&migrate.stdout),
        String::from_utf8_lossy(&migrate.stderr)
    );
    assert!(
        migrate.status.success(),
        "migrate failed: {combined}"
    );
    combined
}

fn apply_cli(url: &str, config: &Path) -> String {
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
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
    assert!(
        apply.status.success(),
        "apply failed: {combined}"
    );
    combined
}

fn status_cli(url: &str) -> String {
    let status = Command::new(bin())
        .args(["status", "--platform-store-url", url])
        .output()
        .expect("run status");
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&status.stdout),
        String::from_utf8_lossy(&status.stderr)
    );
    assert!(
        status.status.success(),
        "status failed: {combined}"
    );
    combined
}

#[tokio::test]
async fn newer_app_migrates_prior_schema_without_wiping_deployment_data() {
    let url = ephemeral_database_url().await;

    migraloop_platform_store::migrate_through(
        &url,
        migraloop_platform_store::PRIOR_RELEASE_SCHEMA_VERSION,
    )
    .await
    .expect("seed prior-release schema");
    seed_prior_release_deployment_data(&url).await;

    let migrate_out = migrate_cli(&url);
    assert!(
        migrate_out.to_ascii_lowercase().contains("migration")
            || migrate_out.contains("Platform Store"),
        "migrate should report success, got:\n{migrate_out}"
    );

    let status = status_cli(&url);
    let latest = migraloop_platform_store::latest_migration_version();
    assert!(
        status.contains("Platform Store: healthy"),
        "store must be healthy after upgrade migrate, got:\n{status}"
    );
    assert!(
        status.contains(&format!("Schema version: {latest}")),
        "schema must reach latest {latest} after upgrade migrate, got:\n{status}"
    );
    assert!(
        status.contains("Deployment: oracle-to-mongo"),
        "existing Deployment must survive upgrade migrate, got:\n{status}"
    );
    assert!(
        status.contains("Pipeline: customers") || status.contains("customers"),
        "existing Pipeline must survive upgrade migrate, got:\n{status}"
    );
    assert!(
        status.contains("Base Dataset: CUSTOMERS")
            && status.contains("rows=2")
            && status.contains("initial_load_complete"),
        "Base Dataset rows must survive upgrade migrate, got:\n{status}"
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
        .expect("run base");
    let base_out = format!(
        "{}{}",
        String::from_utf8_lossy(&base.stdout),
        String::from_utf8_lossy(&base.stderr)
    );
    assert!(base.status.success(), "base inspect failed: {base_out}");
    assert!(
        base_out.contains("Alice") && base_out.contains("Bob"),
        "Base row payloads must survive upgrade migrate, got:\n{base_out}"
    );
}

#[tokio::test]
async fn older_semver_compatible_config_still_applies_after_upgrade() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let mongo_database = unique_mongo_database();

    migraloop_platform_store::migrate_through(
        &url,
        migraloop_platform_store::PRIOR_RELEASE_SCHEMA_VERSION,
    )
    .await
    .expect("seed prior-release schema");
    seed_prior_release_deployment_data(&url).await;
    migrate_cli(&url);

    let older = write_config(
        &dir,
        "older-v1.0.0.yaml",
        &older_compatible_deployment(&mongo_database),
    );
    let apply_out = apply_cli(&url, &older);
    assert!(
        !apply_out.contains("Initial Load complete"),
        "older compatible config must not rebuild Base from scratch, got:\n{apply_out}"
    );

    let status = status_cli(&url);
    assert!(
        status.contains("Deployment: oracle-to-mongo"),
        "Deployment must remain after older config apply, got:\n{status}"
    );
    assert!(
        status.contains("Base Dataset: CUSTOMERS") && status.contains("rows=2"),
        "Base data must remain after older config apply, got:\n{status}"
    );
}

#[tokio::test]
async fn incompatible_config_major_is_rejected() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    migrate_cli(&url);

    let bad = write_config(
        &dir,
        "v2.yaml",
        r#"
apiVersion: migraloop.dev/v2
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
    host: 127.0.0.1
    port: 27017
    database: unused
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
"#,
    );

    let apply = Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "apply",
            "--platform-store-url",
            &url,
            "--file",
            bad.to_str().unwrap(),
        ])
        .output()
        .expect("run apply");
    assert!(
        !apply.status.success(),
        "incompatible major must fail apply"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
    assert!(
        combined.contains("unsupported apiVersion") && combined.contains("v2"),
        "expected clear incompatible-major error, got:\n{combined}"
    );
}

#[tokio::test]
async fn upgrade_smoke_does_not_require_rebuild_from_scratch() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let mongo_database = unique_mongo_database();

    // Establish a full current Deployment first (operator path), then re-apply
    // older SemVer form after a second migrate — upgrade smoke without wipe.
    let current = write_config(
        &dir,
        "current.yaml",
        &current_deployment(&mongo_database),
    );
    migrate_cli(&url);
    let first_apply = apply_cli(&url, &current);
    assert!(
        first_apply.contains("Initial Load complete"),
        "fresh apply should Initial Load, got:\n{first_apply}"
    );

    let before = status_cli(&url);
    assert!(
        before.contains("Base Dataset: CUSTOMERS"),
        "pre-upgrade Base missing:\n{before}"
    );

    // Operator upgrade loop: roll new binary → migrate → status → re-apply older config.
    migrate_cli(&url);
    let older = write_config(
        &dir,
        "older.yaml",
        &older_compatible_deployment(&mongo_database),
    );
    let reapply = apply_cli(&url, &older);
    assert!(
        !reapply.contains("Initial Load complete"),
        "upgrade smoke must not rebuild Base from scratch, got:\n{reapply}"
    );

    let after = status_cli(&url);
    assert!(
        after.contains("Platform Store: healthy")
            && after.contains("Deployment: oracle-to-mongo")
            && after.contains("Base Dataset: CUSTOMERS"),
        "Deployment data must remain after upgrade smoke, got:\n{after}"
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
        .expect("run base");
    let base_out = String::from_utf8_lossy(&base.stdout);
    assert!(base.status.success(), "base failed: {}", base_out);
    assert!(
        base_out.contains("Alice") || base_out.contains("ID"),
        "Base rows must still be inspectable after upgrade smoke, got:\n{base_out}"
    );
}
