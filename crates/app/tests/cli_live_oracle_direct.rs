//! Gated live-Oracle seam: Direct Pipeline Initial Load + LogMiner → MongoDB.
//!
//! Ignored by default (CI uses contract/stub). Enable with a prepared live Source:
//!
//! ```bash
//! export LD_LIBRARY_PATH=/path/to/instantclient
//! export MIGRALOOP_LIVE_ORACLE_HOST=...
//! export MIGRALOOP_LIVE_ORACLE_PORT=1521
//! export MIGRALOOP_LIVE_ORACLE_SERVICE=FREEPDB1
//! export MIGRALOOP_LIVE_ORACLE_USER=SYNC_USER
//! export ORACLE_PASSWORD=...
//! cargo test -p migraloop-app --test cli_live_oracle_direct -- --ignored --nocapture
//! ```
//!
//! Agreed seam (issue #58): CLI apply / sync / inspect — not mocked capture.

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use oracle::{Connection, Connector};
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

fn live_env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

fn require_live_oracle() -> (String, u16, String, String, String) {
    let host = live_env("MIGRALOOP_LIVE_ORACLE_HOST")
        .expect("MIGRALOOP_LIVE_ORACLE_HOST required for ignored live test");
    let port = live_env("MIGRALOOP_LIVE_ORACLE_PORT")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1521);
    let service = live_env("MIGRALOOP_LIVE_ORACLE_SERVICE").unwrap_or_else(|| "FREEPDB1".into());
    let user = live_env("MIGRALOOP_LIVE_ORACLE_USER").unwrap_or_else(|| "SYNC_USER".into());
    let password = live_env("ORACLE_PASSWORD").expect("ORACLE_PASSWORD required");
    (host, port, service, user, password)
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

fn connect_live(
    host: &str,
    port: u16,
    service: &str,
    user: &str,
    password: &str,
) -> Connection {
    let connect_string = format!("//{host}:{port}/{service}");
    Connector::new(user, password, &connect_string)
        .connect()
        .expect("connect to live Oracle for fixture setup")
}

fn prepare_lab_customers(conn: &Connection, table: &str) {
    let _ = conn.execute(&format!("DROP TABLE {table} PURGE"), &[]);
    conn.execute(
        &format!(
            "CREATE TABLE {table} (
                ID NUMBER(10) PRIMARY KEY,
                NAME VARCHAR2(100) NOT NULL,
                EMAIL VARCHAR2(200),
                ACTIVE NUMBER(1)
            )"
        ),
        &[],
    )
    .expect("create live table");
    // Table-level supplemental logging for LogMiner identity/images.
    conn.execute(
        &format!("ALTER TABLE {table} ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"),
        &[],
    )
    .expect("table supplemental logging");
    conn.execute(
        &format!(
            "INSERT INTO {table} (ID, NAME, EMAIL, ACTIVE) VALUES (1, 'Alice', 'alice@example.com', 1)"
        ),
        &[],
    )
    .expect("seed alice");
    conn.execute(
        &format!(
            "INSERT INTO {table} (ID, NAME, EMAIL, ACTIVE) VALUES (2, 'Bob', 'bob@example.com', 0)"
        ),
        &[],
    )
    .expect("seed bob");
    conn.commit().expect("commit seed");
}

fn mutate_lab_customers(conn: &Connection, table: &str) {
    conn.execute(
        &format!("UPDATE {table} SET NAME = 'Alicia', EMAIL = 'alicia@example.com' WHERE ID = 1"),
        &[],
    )
    .expect("update alice");
    conn.execute(
        &format!(
            "INSERT INTO {table} (ID, NAME, EMAIL, ACTIVE) VALUES (3, 'Carol', 'carol@example.com', 1)"
        ),
        &[],
    )
    .expect("insert carol");
    conn.execute(&format!("DELETE FROM {table} WHERE ID = 2"), &[])
        .expect("delete bob");
    conn.commit().expect("commit mutations");
}

#[tokio::test]
#[ignore = "requires live Oracle + Instant Client; see handbook Source System"]
async fn live_oracle_direct_pipeline_initial_load_and_logminer_to_mongo() {
    let (host, port, service, user, password) = require_live_oracle();
    let table = format!(
        "LAB_CUST_{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_secs()
            % 1_000_000
    );

    let conn = connect_live(&host, port, &service, &user, &password);
    // Best-effort DB supplemental logging (may require privileges; Lab Fixture grants these).
    let _ = conn.execute("ALTER DATABASE ADD SUPPLEMENTAL LOG DATA", &[]);
    prepare_lab_customers(&conn, &table);

    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.yaml",
        &format!(
            r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: live-oracle-to-mongo
spec:
  source:
    kind: oracle
    host: {host}
    port: {port}
    database: {service}
    username: {user}
    password:
      fromEnv: ORACLE_PASSWORD
  target:
    kind: mongodb
    host: {mongo_host}
    port: {mongo_port}
    database: {mongo_database}
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
  pipelines:
    - name: lab-customers
      mode: direct
      source:
        table: {table}
      target:
        collection: lab_customers
"#,
            mongo_host = mongo_host(),
            mongo_port = mongo_port(),
        ),
    );

    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", &url])
        .output()
        .expect("migrate");
    assert!(
        migrate.status.success(),
        "migrate failed: {}",
        String::from_utf8_lossy(&migrate.stderr)
    );

    let apply = Command::new(bin())
        .env("ORACLE_PASSWORD", &password)
        .env("MONGO_PASSWORD", "mongo-secret-value")
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
        "live apply/Initial Load failed: stdout={} stderr={}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
    let apply_out = String::from_utf8_lossy(&apply.stdout);
    assert!(
        apply_out.contains("Initial Load") || apply_out.contains("initial_load"),
        "expected Initial Load on live path, got:\n{apply_out}"
    );

    let base_after_apply = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", &table])
        .output()
        .expect("base after apply");
    assert!(base_after_apply.status.success());
    let base_apply_out = String::from_utf8_lossy(&base_after_apply.stdout);
    assert!(
        base_apply_out.contains("Alice") && base_apply_out.contains("Bob"),
        "Initial Load must read live Oracle rows, got:\n{base_apply_out}"
    );

    mutate_lab_customers(&conn, &table);

    let sync = Command::new(bin())
        .env("ORACLE_PASSWORD", &password)
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args(["sync", "--platform-store-url", &url])
        .output()
        .expect("sync");
    assert!(
        sync.status.success(),
        "live sync failed: stdout={} stderr={}",
        String::from_utf8_lossy(&sync.stdout),
        String::from_utf8_lossy(&sync.stderr)
    );
    let sync_out = String::from_utf8_lossy(&sync.stdout);
    assert!(
        sync_out.to_ascii_lowercase().contains("logminer"),
        "expected LogMiner (OCI) on live path, got:\n{sync_out}"
    );

    let base_after = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", &table])
        .output()
        .expect("base after sync");
    assert!(base_after.status.success());
    let base_out = String::from_utf8_lossy(&base_after.stdout);
    assert!(
        base_out.contains("Alicia") && base_out.contains("Carol") && !base_out.contains("Bob"),
        "live LogMiner must reflect insert/update/delete on Base, got:\n{base_out}"
    );

    let target = Command::new(bin())
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "target",
            "--platform-store-url",
            &url,
            "--collection",
            "lab_customers",
        ])
        .output()
        .expect("target");
    assert!(target.status.success());
    let target_out = String::from_utf8_lossy(&target.stdout);
    assert!(
        target_out.contains("Alicia")
            && target_out.contains("Carol")
            && !target_out.contains("Bob"),
        "Mongo Managed fields must follow live Oracle changes, got:\n{target_out}"
    );

    let _ = conn.execute(&format!("DROP TABLE {table} PURGE"), &[]);
}
