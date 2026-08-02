//! Operator-visible seam: Transform Pipeline MVP (issue #16 / PRD).
//!
//! Agreed seam: CLI config/status + Derived Dataset + Target documents.
//! Declarative project/filter only; Output Identity required; scripts rejected.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

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
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
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
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    format!("appdb_{suffix}")
}

fn deployment_shell(mongo_database: &str, pipeline_yaml: &str) -> String {
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
{pipeline_yaml}
"#,
        host = mongo_host(),
        port = mongo_port(),
    )
}

fn apply_expect_failure(url: &str, config: &Path) -> String {
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
        !apply.status.success(),
        "apply should fail, but succeeded: stdout={}",
        String::from_utf8_lossy(&apply.stdout)
    );
    let mut combined = String::from_utf8_lossy(&apply.stderr).into_owned();
    combined.push_str(&String::from_utf8_lossy(&apply.stdout));
    combined
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
async fn transform_pipeline_missing_output_identity_fails_apply() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let pipeline = r#"
    - name: active-customers
      mode: transform
      source:
        table: CUSTOMERS
      target:
        collection: active_customers
      transform:
        - project:
            fields: [ID, NAME, ACTIVE]
        - filter:
            field: ACTIVE
            eq: 1
"#;
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_shell(&mongo_database, pipeline),
    );

    let err = apply_expect_failure(&url, &config);
    assert!(
        err.to_ascii_lowercase().contains("output identity")
            || err.contains("outputIdentity"),
        "expected clear Output Identity apply failure, got:\n{err}"
    );
}

#[tokio::test]
async fn transform_pipeline_script_transform_fails_apply_clearly() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let pipeline = r#"
    - name: scripted
      mode: transform
      source:
        table: CUSTOMERS
      target:
        collection: scripted
      outputIdentity: [ID]
      transform:
        - script: "return doc.ACTIVE == 1"
"#;
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_shell(&mongo_database, pipeline),
    );

    let err = apply_expect_failure(&url, &config);
    let lower = err.to_ascii_lowercase();
    assert!(
        lower.contains("script")
            || lower.contains("unsupported")
            || lower.contains("free-form"),
        "expected clear unsupported/script transform failure, got:\n{err}"
    );
}

#[tokio::test]
async fn transform_pipeline_malformed_project_fails_as_invalid_not_unsupported() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let pipeline = r#"
    - name: bad-project
      mode: transform
      source:
        table: CUSTOMERS
      target:
        collection: bad_project
      outputIdentity: [ID]
      transform:
        - project:
            fields: []
"#;
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_shell(&mongo_database, pipeline),
    );

    let err = apply_expect_failure(&url, &config);
    let lower = err.to_ascii_lowercase();
    assert!(
        lower.contains("invalid") || lower.contains("project.fields"),
        "expected invalid project shape error, got:\n{err}"
    );
    assert!(
        !lower.contains("unsupported"),
        "malformed project must not be reported as unsupported, got:\n{err}"
    );
}

#[tokio::test]
async fn transform_pipeline_filter_matching_no_rows_still_materializes_empty_derived() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    // ACTIVE==99 matches nothing in stub CUSTOMERS.
    let pipeline = r#"
    - name: nobody
      mode: transform
      source:
        table: CUSTOMERS
      target:
        collection: nobody
      outputIdentity: [ID]
      transform:
        - project:
            fields: [ID, NAME, ACTIVE]
        - filter:
            field: ACTIVE
            eq: 99
"#;
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_shell(&mongo_database, pipeline),
    );

    migrate_and_apply(&url, &config);

    let derived = Command::new(bin())
        .args([
            "derived",
            "--platform-store-url",
            &url,
            "--pipeline",
            "nobody",
        ])
        .output()
        .expect("run derived");
    assert!(
        derived.status.success(),
        "derived inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&derived.stdout),
        String::from_utf8_lossy(&derived.stderr)
    );
    let derived_out = String::from_utf8_lossy(&derived.stdout);
    assert!(
        derived_out.contains("rows=0") || derived_out.contains("rows = 0"),
        "empty filter result must materialize Derived with 0 rows, got:\n{derived_out}"
    );
}

#[tokio::test]
async fn transform_pipeline_unsupported_operator_fails_apply_clearly() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let pipeline = r#"
    - name: faceted
      mode: transform
      source:
        table: CUSTOMERS
      target:
        collection: faceted
      outputIdentity: [ID]
      transform:
        - facet:
            stages: []
"#;
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_shell(&mongo_database, pipeline),
    );

    let err = apply_expect_failure(&url, &config);
    let lower = err.to_ascii_lowercase();
    assert!(
        lower.contains("unsupported") && lower.contains("facet"),
        "expected clear unsupported operator failure naming facet, got:\n{err}"
    );
}

#[tokio::test]
async fn transform_pipeline_project_filter_materializes_derived_and_delivers_to_mongo() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    // project keeps ID/NAME/ACTIVE; filter keeps ACTIVE==1 → Alice + Carol (not Bob).
    let pipeline = r#"
    - name: active-customers
      mode: transform
      source:
        table: CUSTOMERS
      target:
        collection: active_customers
      outputIdentity: [ID]
      transform:
        - project:
            fields: [ID, NAME, ACTIVE]
        - filter:
            field: ACTIVE
            eq: 1
"#;
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_shell(&mongo_database, pipeline),
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
        status_out.contains("active-customers") && status_out.contains("transform"),
        "expected Transform Pipeline in status, got:\n{status_out}"
    );
    assert!(
        status_out.contains("Derived Dataset")
            && (status_out.contains("active-customers") || status_out.contains("rows=")),
        "expected Derived Dataset materialization in status, got:\n{status_out}"
    );
    assert!(
        status_out.contains("Delivery")
            && (status_out.contains("delivered")
                || status_out.contains("complete")
                || status_out.contains("ok")),
        "expected Delivery progress in status, got:\n{status_out}"
    );

    let derived = Command::new(bin())
        .args([
            "derived",
            "--platform-store-url",
            &url,
            "--pipeline",
            "active-customers",
        ])
        .output()
        .expect("run derived");
    assert!(
        derived.status.success(),
        "derived inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&derived.stdout),
        String::from_utf8_lossy(&derived.stderr)
    );
    let derived_out = String::from_utf8_lossy(&derived.stdout);
    assert!(
        derived_out.contains("Alice") && derived_out.contains("Carol"),
        "Derived must include filtered ACTIVE=1 rows Alice/Carol, got:\n{derived_out}"
    );
    assert!(
        !derived_out.contains("Bob"),
        "Derived must filter out ACTIVE=0 Bob, got:\n{derived_out}"
    );
    assert!(
        !derived_out.contains("alice@example.com") && !derived_out.contains("EMAIL"),
        "Derived project must omit EMAIL, got:\n{derived_out}"
    );

    let target = Command::new(bin())
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "target",
            "--platform-store-url",
            &url,
            "--collection",
            "active_customers",
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
        "expected Output Identity _id=1 Delivered, got:\n{target_out}"
    );
    assert!(
        target_out.contains("\"_id\": 3") || target_out.contains("\"_id\":3"),
        "expected Output Identity _id=3 Delivered, got:\n{target_out}"
    );
    assert!(
        target_out.contains("Alice") && target_out.contains("Carol"),
        "expected Managed Derived fields Delivered, got:\n{target_out}"
    );
    assert!(
        !target_out.contains("Bob"),
        "Bob must not be Delivered after filter, got:\n{target_out}"
    );
    assert!(
        !target_out.contains("alice@example.com"),
        "EMAIL must not be Delivered after project, got:\n{target_out}"
    );
}
