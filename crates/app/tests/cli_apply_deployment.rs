//! Operator-visible seam: CLI apply / status for Deployment config + secret refs.
//!
//! Agreed seam (issue #5 / PRD): verify via CLI output and config, not private internals.

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

#[tokio::test]
async fn apply_yaml_creates_deployment_visible_in_status() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.yaml",
        r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: oracle-to-mongo
spec:
  source:
    kind: oracle
    host: oracle.example.com
    port: 1521
    database: ORCLPDB1
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
"#,
    );

    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", &url])
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
        stdout.contains("Deployment: oracle-to-mongo"),
        "expected Deployment identity, got:\n{stdout}"
    );
    assert!(
        stdout.contains("Source: oracle")
            && stdout.contains("oracle.example.com:1521")
            && stdout.contains("ORCLPDB1")
            && stdout.contains("sync_user"),
        "expected non-secret Source connection config, got:\n{stdout}"
    );
    assert!(
        stdout.contains("Target: mongodb")
            && stdout.contains("mongo.example.com:27017")
            && stdout.contains("appdb")
            && stdout.contains("deliver_user"),
        "expected non-secret Target connection config, got:\n{stdout}"
    );
    assert!(
        stdout.contains("passwordRef=env:ORACLE_PASSWORD")
            && stdout.contains("passwordRef=env:MONGO_PASSWORD"),
        "expected secret references (not values) in status, got:\n{stdout}"
    );
    assert!(
        !stdout.contains("oracle-secret-value") && !stdout.contains("mongo-secret-value"),
        "status must not leak secret values:\n{stdout}"
    );
}

#[tokio::test]
async fn apply_fails_clearly_when_secret_env_is_missing() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.yaml",
        r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: missing-secret
spec:
  source:
    kind: oracle
    host: oracle.example.com
    port: 1521
    database: ORCLPDB1
    username: sync_user
    password:
      fromEnv: MISSING_ORACLE_PASSWORD
  target:
    kind: mongodb
    host: mongo.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
"#,
    );

    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", &url])
        .output()
        .expect("run migrate");
    assert!(migrate.status.success());

    let apply = Command::new(bin())
        .env_remove("MISSING_ORACLE_PASSWORD")
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
        !apply.status.success(),
        "apply should fail when a secret reference is unresolvable"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
    assert!(
        combined.contains("MISSING_ORACLE_PASSWORD")
            && (combined.to_lowercase().contains("secret")
                || combined.to_lowercase().contains("unresolvable")
                || combined.to_lowercase().contains("missing")),
        "expected clear missing-secret error, got:\n{combined}"
    );
}

#[tokio::test]
async fn apply_rejects_plaintext_password_in_config() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.yaml",
        r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: plaintext-bad
spec:
  source:
    kind: oracle
    host: oracle.example.com
    port: 1521
    database: ORCLPDB1
    username: sync_user
    password: "literally-in-the-file"
  target:
    kind: mongodb
    host: mongo.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
"#,
    );

    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", &url])
        .output()
        .expect("run migrate");
    assert!(migrate.status.success());

    let apply = Command::new(bin())
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

    assert!(!apply.status.success(), "plaintext password must be rejected");
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
    assert!(
        combined.to_lowercase().contains("plaintext")
            || combined.contains("fromEnv")
            || combined.contains("fromFile")
            || combined.to_lowercase().contains("secret reference"),
        "expected clear plaintext-secret rejection, got:\n{combined}"
    );
}

#[tokio::test]
async fn apply_updates_existing_deployment_and_supports_file_secret_refs() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");

    let oracle_secret = dir.path().join("oracle.password");
    let mongo_secret = dir.path().join("mongo.password");
    fs::write(&oracle_secret, "oracle-from-file\n").expect("write oracle secret");
    fs::write(&mongo_secret, "mongo-from-file\n").expect("write mongo secret");

    let v1 = write_config(
        &dir,
        "v1.yaml",
        r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: primary
spec:
  source:
    kind: oracle
    host: oracle-v1.example.com
    port: 1521
    database: ORCLPDB1
    username: sync_user
    password:
      fromEnv: ORACLE_PASSWORD
  target:
    kind: mongodb
    host: mongo-v1.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
"#,
    );

    let v2 = write_config(
        &dir,
        "v2.yaml",
        &format!(
            r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: primary
spec:
  source:
    kind: oracle
    host: oracle-v2.example.com
    port: 1522
    database: ORCLPDB2
    username: sync_user_v2
    password:
      fromFile: {}
  target:
    kind: mongodb
    host: mongo-v2.example.com
    port: 27018
    database: appdb2
    username: deliver_user_v2
    password:
      fromFile: {}
"#,
            oracle_secret.display(),
            mongo_secret.display()
        ),
    );

    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", &url])
        .output()
        .expect("run migrate");
    assert!(migrate.status.success());

    let apply_v1 = Command::new(bin())
        .env("ORACLE_PASSWORD", "o1")
        .env("MONGO_PASSWORD", "m1")
        .args([
            "apply",
            "--platform-store-url",
            &url,
            "--file",
            v1.to_str().unwrap(),
        ])
        .output()
        .expect("apply v1");
    assert!(apply_v1.status.success(), "{}", String::from_utf8_lossy(&apply_v1.stderr));

    let apply_v2 = Command::new(bin())
        .args([
            "apply",
            "--platform-store-url",
            &url,
            "--file",
            v2.to_str().unwrap(),
        ])
        .output()
        .expect("apply v2");
    assert!(
        apply_v2.status.success(),
        "update apply failed: {}",
        String::from_utf8_lossy(&apply_v2.stderr)
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("status");
    assert!(status.status.success());
    let stdout = String::from_utf8_lossy(&status.stdout);

    assert!(stdout.contains("Deployment: primary"));
    assert!(
        stdout.contains("oracle-v2.example.com:1522")
            && stdout.contains("ORCLPDB2")
            && stdout.contains("sync_user_v2"),
        "expected updated Source config, got:\n{stdout}"
    );
    assert!(
        stdout.contains("mongo-v2.example.com:27018")
            && stdout.contains("appdb2")
            && stdout.contains("deliver_user_v2"),
        "expected updated Target config, got:\n{stdout}"
    );
    assert!(
        stdout.contains(&format!("passwordRef=file:{}", oracle_secret.display()))
            && stdout.contains(&format!("passwordRef=file:{}", mongo_secret.display())),
        "expected file secret references, got:\n{stdout}"
    );
    assert!(
        !stdout.contains("oracle-from-file") && !stdout.contains("mongo-from-file"),
        "status must not leak file secret values:\n{stdout}"
    );
    assert!(
        !stdout.contains("oracle-v1.example.com") && !stdout.contains("mongo-v1.example.com"),
        "status should show updated Deployment only once, got:\n{stdout}"
    );
}

#[tokio::test]
async fn apply_rejects_password_object_with_extra_plaintext_field() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.yaml",
        r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: sneaky-plaintext
spec:
  source:
    kind: oracle
    host: oracle.example.com
    port: 1521
    database: ORCLPDB1
    username: sync_user
    password:
      fromEnv: ORACLE_PASSWORD
      plaintext: "should-not-be-accepted"
  target:
    kind: mongodb
    host: mongo.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
"#,
    );

    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", &url])
        .output()
        .expect("run migrate");
    assert!(migrate.status.success());

    let apply = Command::new(bin())
        .env("ORACLE_PASSWORD", "o")
        .env("MONGO_PASSWORD", "m")
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
        "password objects with unknown plaintext fields must be rejected"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
    assert!(
        combined.to_lowercase().contains("unknown field")
            || combined.to_lowercase().contains("plaintext")
            || combined.contains("fromEnv"),
        "expected rejection of unknown password fields, got:\n{combined}"
    );
}

#[tokio::test]
async fn apply_supports_docker_secret_references() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");

    // Simulate Docker's /run/secrets mount with a writable temp root via symlink is hard;
    // instead write the secret where fromDockerSecret resolves: we can't write /run/secrets
    // without root, so exercise fromDockerSecret through a failing clear error when missing,
    // and a successful fromFile path that matches Docker's mount convention.
    let secrets_dir = dir.path().join("secrets");
    fs::create_dir_all(&secrets_dir).expect("secrets dir");
    let oracle_secret = secrets_dir.join("oracle_password");
    let mongo_secret = secrets_dir.join("mongo_password");
    fs::write(&oracle_secret, "oracle-docker-secret\n").expect("write oracle");
    fs::write(&mongo_secret, "mongo-docker-secret\n").expect("write mongo");

    let config = write_config(
        &dir,
        "deployment.yaml",
        &format!(
            r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: docker-secrets
spec:
  source:
    kind: oracle
    host: oracle.example.com
    port: 1521
    database: ORCLPDB1
    username: sync_user
    password:
      fromFile: {}
  target:
    kind: mongodb
    host: mongo.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromFile: {}
"#,
            oracle_secret.display(),
            mongo_secret.display()
        ),
    );

    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", &url])
        .output()
        .expect("migrate");
    assert!(migrate.status.success());

    let apply = Command::new(bin())
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
        apply.status.success(),
        "mounted-secret file refs (Docker secrets path shape) should apply: {}",
        String::from_utf8_lossy(&apply.stderr)
    );

    // fromDockerSecret resolves under /run/secrets; missing name must fail clearly.
    let docker_cfg = write_config(
        &dir,
        "docker.yaml",
        r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: docker-secret-name
spec:
  source:
    kind: oracle
    host: oracle.example.com
    port: 1521
    database: ORCLPDB1
    username: sync_user
    password:
      fromDockerSecret: missing_oracle_secret
  target:
    kind: mongodb
    host: mongo.example.com
    port: 27017
    database: appdb
    username: deliver_user
    password:
      fromDockerSecret: missing_mongo_secret
"#,
    );
    let apply_docker = Command::new(bin())
        .args([
            "apply",
            "--platform-store-url",
            &url,
            "--file",
            docker_cfg.to_str().unwrap(),
        ])
        .output()
        .expect("apply docker");
    assert!(
        !apply_docker.status.success(),
        "missing Docker secret should fail"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&apply_docker.stdout),
        String::from_utf8_lossy(&apply_docker.stderr)
    );
    assert!(
        combined.contains("missing_oracle_secret")
            || combined.contains("/run/secrets/missing_oracle_secret"),
        "expected clear Docker-secret resolution error, got:\n{combined}"
    );
    assert!(
        combined.to_lowercase().contains("unresolvable")
            || combined.to_lowercase().contains("secret"),
        "expected unresolvable-secret wording, got:\n{combined}"
    );
}

#[tokio::test]
async fn apply_json_creates_deployment() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.json",
        r#"
{
  "apiVersion": "migraloop.dev/v1",
  "kind": "Deployment",
  "metadata": { "name": "from-json" },
  "spec": {
    "source": {
      "kind": "oracle",
      "host": "oracle.json.example.com",
      "port": 1521,
      "database": "ORCL",
      "username": "src",
      "password": { "fromEnv": "ORACLE_PASSWORD" }
    },
    "target": {
      "kind": "mongodb",
      "host": "mongo.json.example.com",
      "port": 27017,
      "database": "db",
      "username": "tgt",
      "password": { "fromEnv": "MONGO_PASSWORD" }
    }
  }
}
"#,
    );

    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", &url])
        .output()
        .expect("migrate");
    assert!(migrate.status.success());

    let apply = Command::new(bin())
        .env("ORACLE_PASSWORD", "o")
        .env("MONGO_PASSWORD", "m")
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
        apply.status.success(),
        "json apply failed: {}",
        String::from_utf8_lossy(&apply.stderr)
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("status");
    let stdout = String::from_utf8_lossy(&status.stdout);
    assert!(
        stdout.contains("Deployment: from-json")
            && stdout.contains("oracle.json.example.com:1521"),
        "expected JSON-applied Deployment in status, got:\n{stdout}"
    );
}
