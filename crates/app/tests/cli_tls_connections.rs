//! Operator-visible seam: TLS for Source, Target, and Platform Store (#123).
//!
//! Agreed seams:
//! - Config load / apply / status (Deployment YAML → Platform Store → `status`)
//! - Connect-string / URI construction (unit-tested in capture/delivery)
//! - Fail-clear when TLS is requested but cannot be established (apply/sync/store)
//! - Cleartext remains allowed when TLS is not requested

mod common;

use std::fs;
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

fn write_config(dir: &TempDir, name: &str, contents: &str) -> std::path::PathBuf {
    let path = dir.path().join(name);
    fs::write(&path, contents).expect("write config");
    path
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

#[tokio::test]
async fn apply_persists_tls_settings_visible_in_status_without_secret_material() {
    let url = ephemeral_database_url().await;
    migrate(&url);
    let dir = TempDir::new().expect("tempdir");
    let ca = dir.path().join("mongo-ca.pem");
    fs::write(&ca, "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n")
        .expect("write ca");
    let wallet = dir.path().join("oracle-wallet");
    fs::create_dir(&wallet).expect("wallet dir");

    let config = write_config(
        &dir,
        "deployment.yaml",
        &format!(
            r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: tls-demo
spec:
  source:
    kind: oracle
    host: oracle.example.com
    port: 2484
    database: ORCLPDB1
    username: sync_user
    password:
      fromEnv: ORACLE_PASSWORD
    tls:
      enabled: true
      walletLocation: {}
  target:
    kind: mongodb
    host: mongo.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
    tls:
      enabled: true
      caFile: {}
"#,
            wallet.display(),
            ca.display()
        ),
    );

    let apply = Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "apply",
            "--platform-store-url",
            &url,
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
        stdout.contains("tls=enabled")
            && stdout.contains(&format!("walletLocation={}", wallet.display()))
            && stdout.contains(&format!("caFile={}", ca.display())),
        "status should surface non-secret TLS settings, got:\n{stdout}"
    );
    assert!(
        !stdout.contains("oracle-secret-value")
            && !stdout.contains("mongo-secret-value")
            && !stdout.contains("BEGIN CERTIFICATE"),
        "status must not leak secrets or certificate PEM bodies:\n{stdout}"
    );
}

#[tokio::test]
async fn apply_without_tls_block_keeps_cleartext_and_shows_tls_disabled() {
    let url = ephemeral_database_url().await;
    migrate(&url);
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.yaml",
        r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: cleartext-demo
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
    database: appdb
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
            config.to_str().unwrap(),
        ])
        .output()
        .expect("run apply");
    assert!(
        apply.status.success(),
        "cleartext apply must still succeed: stdout={} stderr={}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("run status");
    let stdout = String::from_utf8_lossy(&status.stdout);
    assert!(
        stdout.contains("tls=disabled"),
        "status should show tls=disabled when unset, got:\n{stdout}"
    );
}

#[tokio::test]
async fn apply_rejects_missing_tls_ca_file_clearly() {
    let url = ephemeral_database_url().await;
    migrate(&url);
    let dir = TempDir::new().expect("tempdir");
    let missing = dir.path().join("missing-ca.pem");
    let config = write_config(
        &dir,
        "deployment.yaml",
        &format!(
            r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: bad-tls
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
    tls:
      enabled: true
      caFile: {}
"#,
            missing.display()
        ),
    );

    let apply = Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "apply",
            "--platform-store-url",
            &url,
            "--file",
            config.to_str().unwrap(),
        ])
        .output()
        .expect("run apply");
    assert!(!apply.status.success(), "missing TLS caFile must fail apply");
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
    let lower = combined.to_ascii_lowercase();
    assert!(
        lower.contains("tls") && (lower.contains("cafile") || lower.contains("ca file")),
        "expected clear TLS caFile error, got:\n{combined}"
    );
    assert!(
        !lower.contains("falling back") && !lower.contains("fallback"),
        "must not silently fall back, got:\n{combined}"
    );
}

#[tokio::test]
async fn apply_rejects_ca_file_on_oracle_source() {
    let url = ephemeral_database_url().await;
    migrate(&url);
    let dir = TempDir::new().expect("tempdir");
    let ca = dir.path().join("oracle-ca.pem");
    fs::write(&ca, "-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----\n")
        .expect("write ca");
    let config = write_config(
        &dir,
        "deployment.yaml",
        &format!(
            r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: bad-source-ca
spec:
  source:
    kind: oracle
    host: stub
    port: 1521
    database: STUB
    username: sync_user
    password:
      fromEnv: ORACLE_PASSWORD
    tls:
      enabled: true
      caFile: {}
  target:
    kind: mongodb
    host: mongo.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
"#,
            ca.display()
        ),
    );

    let apply = Command::new(bin())
        .env("ORACLE_PASSWORD", "x")
        .env("MONGO_PASSWORD", "y")
        .args([
            "apply",
            "--platform-store-url",
            &url,
            "--file",
            config.to_str().unwrap(),
        ])
        .output()
        .expect("run apply");
    assert!(!apply.status.success(), "Oracle caFile must be rejected");
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
    let lower = combined.to_ascii_lowercase();
    assert!(
        lower.contains("cafile") && lower.contains("walletlocation"),
        "expected Oracle caFile → walletLocation guidance, got:\n{combined}"
    );
}

#[tokio::test]
async fn apply_rejects_missing_source_wallet_location_when_tls_enabled() {
    let url = ephemeral_database_url().await;
    migrate(&url);
    let dir = TempDir::new().expect("tempdir");
    let missing = dir.path().join("missing-wallet");
    let config = write_config(
        &dir,
        "deployment.yaml",
        &format!(
            r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: bad-source-wallet
spec:
  source:
    kind: oracle
    host: stub
    port: 1521
    database: STUB
    username: sync_user
    password:
      fromEnv: ORACLE_PASSWORD
    tls:
      enabled: true
      walletLocation: {}
  target:
    kind: mongodb
    host: mongo.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
"#,
            missing.display()
        ),
    );

    let apply = Command::new(bin())
        .env("ORACLE_PASSWORD", "x")
        .env("MONGO_PASSWORD", "y")
        .args([
            "apply",
            "--platform-store-url",
            &url,
            "--file",
            config.to_str().unwrap(),
        ])
        .output()
        .expect("run apply");
    assert!(
        !apply.status.success(),
        "missing Oracle walletLocation must fail apply"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
    let lower = combined.to_ascii_lowercase();
    assert!(
        lower.contains("tls") && lower.contains("walletlocation"),
        "expected clear Source TLS walletLocation error, got:\n{combined}"
    );
}

#[tokio::test]
async fn apply_rejects_wallet_location_on_mongodb_target() {
    let url = ephemeral_database_url().await;
    migrate(&url);
    let dir = TempDir::new().expect("tempdir");
    let wallet = dir.path().join("wallet");
    fs::create_dir(&wallet).expect("wallet");
    let config = write_config(
        &dir,
        "deployment.yaml",
        &format!(
            r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: bad-wallet
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
    tls:
      enabled: true
      walletLocation: {}
"#,
            wallet.display()
        ),
    );

    let apply = Command::new(bin())
        .env("ORACLE_PASSWORD", "x")
        .env("MONGO_PASSWORD", "y")
        .args([
            "apply",
            "--platform-store-url",
            &url,
            "--file",
            config.to_str().unwrap(),
        ])
        .output()
        .expect("run apply");
    assert!(!apply.status.success(), "Mongo walletLocation must be rejected");
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
    assert!(
        combined.to_ascii_lowercase().contains("walletlocation")
            && combined.to_ascii_lowercase().contains("oracle"),
        "expected walletLocation/Oracle guidance, got:\n{combined}"
    );
}

#[tokio::test]
async fn apply_with_tls_enabled_against_cleartext_mongo_fails_clearly() {
    let url = ephemeral_database_url().await;
    migrate(&url);
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let ca = dir.path().join("dummy-ca.pem");
    // Minimal PEM-shaped file so path validation passes; TLS handshake still fails.
    fs::write(
        &ca,
        "-----BEGIN CERTIFICATE-----\nMIIBkTCB+wIJANotARealCert\n-----END CERTIFICATE-----\n",
    )
    .expect("write ca");

    let mongo_db = format!("tls_fail_{}", common::unique_suffix());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &format!(
            r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: tls-mongo-fail
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
    database: {mongo_db}
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
    tls:
      enabled: true
      caFile: {ca}
  pipelines:
    - name: customers
      mode: direct
      source:
        table: CUSTOMERS
      target:
        collection: customers
"#,
            mongo_db = mongo_db,
            ca = ca.display()
        ),
    );

    // apply with pipelines opens Target Delivery; TLS against cleartext Mongo must fail clearly.
    let apply = Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .env(
            migraloop_capture::CONTRACT_SOURCE_CATALOG_ENV,
            doubles.catalog_path.as_os_str(),
        )
        .env(
            migraloop_capture::INJECT_LOGMINER_CONTENTS_ENV,
            doubles.logminer_path.as_os_str(),
        )
        .args([
            "apply",
            "--platform-store-url",
            &url,
            "--file",
            config.to_str().unwrap(),
        ])
        .output()
        .expect("run apply");
    assert!(
        !apply.status.success(),
        "TLS against cleartext Mongo must fail at apply/run"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
    let lower = combined.to_ascii_lowercase();
    assert!(
        lower.contains("tls")
            && lower.contains("target")
            && lower.contains("no silent cleartext fallback"),
        "expected clear TLS Target connect failure, got:\n{combined}"
    );
}

#[tokio::test]
async fn platform_store_sslmode_require_against_cleartext_fails_clearly() {
    // Ephemeral DB URL is cleartext Postgres; sslmode=require must not silently fall back.
    let url = ephemeral_database_url().await;
    let require_url = if url.contains('?') {
        format!("{url}&sslmode=require")
    } else {
        format!("{url}?sslmode=require")
    };

    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", &require_url])
        .output()
        .expect("run migrate");
    assert!(
        !migrate.status.success(),
        "sslmode=require against cleartext store must fail"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&migrate.stdout),
        String::from_utf8_lossy(&migrate.stderr)
    );
    let lower = combined.to_ascii_lowercase();
    assert!(
        (lower.contains("tls") || lower.contains("ssl"))
            && lower.contains("no cleartext fallback"),
        "expected required-TLS Platform Store error, got:\n{combined}"
    );
}
