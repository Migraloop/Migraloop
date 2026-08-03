//! Operator-visible seam: schema-driven types, timezone, and NUMBER precision (#11).
//!
//! Agreed seam (PRD Testing Decisions / issue #11): CLI config/apply/status/target
//! and resulting Target BSON types / apply-time failures.

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

fn apply(url: &str, config: &Path, doubles: &common::NamedScenarioDoubles) -> std::process::Output {
    let mut cmd = Command::new(bin());
    cmd
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .env_remove("MIGRALOOP_STUB_DB_TIMEZONE");
    doubles.apply_env(&mut cmd);
    cmd
        .args([
            "apply",
            "--platform-store-url",
            url,
            "--file",
            config.to_str().unwrap(),
        ])
        .output()
        .expect("run apply")

}

fn pymongo_field_type(database: &str, collection: &str, id: i64, field: &str) -> String {
    let output = Command::new("python3")
        .args([
            "-c",
            &format!(
                r#"
from pymongo import MongoClient
from bson.decimal128 import Decimal128
from bson.int64 import Int64
import datetime
c = MongoClient(
    "mongodb://deliver_user:mongo-secret-value@{host}:{port}/{database}?authSource=admin",
    serverSelectionTimeoutMS=5000,
)
doc = c["{database}"]["{collection}"].find_one({{"_id": {id}}})
assert doc is not None, "missing doc"
val = doc.get("{field}")
if isinstance(val, bool):
    print("bool")
elif isinstance(val, Int64):
    print("int64")
elif isinstance(val, int):
    print("int")
elif isinstance(val, float):
    print("float")
elif isinstance(val, Decimal128):
    print("decimal128")
elif isinstance(val, datetime.datetime):
    print("datetime")
elif isinstance(val, str):
    print("str")
else:
    print(type(val).__name__)
print(repr(val))
"#,
                host = mongo_host(),
                port = mongo_port(),
                database = database,
                collection = collection,
                id = id,
                field = field,
            ),
        ])
        .output()
        .expect("pymongo type probe");
    assert!(
        output.status.success(),
        "pymongo failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

#[tokio::test]
async fn safe_number_delivers_long_and_decimal128_not_ieee_double() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "orders.yaml",
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
    - name: orders
      mode: direct
      source:
        table: ORDERS
      target:
        collection: orders
"#,
            host = mongo_host(),
            port = mongo_port(),
        ),
    );

    migrate(&url);
    let apply_out = apply(&url, &config, &doubles);
    assert!(
        apply_out.status.success(),
        "apply failed: stdout={} stderr={}",
        String::from_utf8_lossy(&apply_out.stdout),
        String::from_utf8_lossy(&apply_out.stderr)
    );

    let id_type = pymongo_field_type(&mongo_database, "orders", 100, "ORDER_ID");
    assert!(
        id_type.contains("int64") || id_type.lines().next() == Some("int"),
        "ORDER_ID must be integer/Long, not float: {id_type}"
    );
    assert!(
        !id_type.contains("float"),
        "NUMBER must not default to IEEE double: {id_type}"
    );

    let amount_type = pymongo_field_type(&mongo_database, "orders", 100, "AMOUNT");
    assert!(
        amount_type.contains("decimal128"),
        "AMOUNT NUMBER(12,2) must be Decimal128, got: {amount_type}"
    );
    assert!(
        !amount_type.contains("float"),
        "AMOUNT must not be IEEE double: {amount_type}"
    );
}

#[tokio::test]
async fn unsafe_number_blocks_apply_until_string_or_omit() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());

    let blocked = write_config(
        &dir,
        "blocked.yaml",
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
    - name: accounts
      mode: direct
      source:
        table: ACCOUNTS
      target:
        collection: accounts
"#,
            host = mongo_host(),
            port = mongo_port(),
        ),
    );

    migrate(&url);
    let fail = apply(&url, &blocked, &doubles);
    assert!(
        !fail.status.success(),
        "unsafe NUMBER must block apply, got success: {}",
        String::from_utf8_lossy(&fail.stdout)
    );
    let err = format!(
        "{}{}",
        String::from_utf8_lossy(&fail.stdout),
        String::from_utf8_lossy(&fail.stderr)
    );
    assert!(
        err.contains("unsafe") && (err.contains("HUGE_AMOUNT") || err.contains("LEGACY_NUM")),
        "expected unsafe NUMBER apply failure, got: {err}"
    );

    let resolved = write_config(
        &dir,
        "resolved.yaml",
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
    - name: accounts
      mode: direct
      source:
        table: ACCOUNTS
      target:
        collection: accounts
      fields:
        HUGE_AMOUNT:
          as: string
        LEGACY_NUM:
          as: omit
"#,
            host = mongo_host(),
            port = mongo_port(),
        ),
    );

    let ok = apply(&url, &resolved, &doubles);
    assert!(
        ok.status.success(),
        "string/omit must allow apply: stdout={} stderr={}",
        String::from_utf8_lossy(&ok.stdout),
        String::from_utf8_lossy(&ok.stderr)
    );

    let huge = pymongo_field_type(&mongo_database, "accounts", 1, "HUGE_AMOUNT");
    assert!(
        huge.contains("str"),
        "HUGE_AMOUNT must be delivered as string: {huge}"
    );

    let probe_legacy = Command::new("python3")
        .args([
            "-c",
            &format!(
                r#"
from pymongo import MongoClient
c = MongoClient(
    "mongodb://deliver_user:mongo-secret-value@{host}:{port}/{database}?authSource=admin",
    serverSelectionTimeoutMS=5000,
)
doc = c["{database}"]["accounts"].find_one({{"_id": 1}})
assert "LEGACY_NUM" not in doc, doc
assert "BALANCE_CENTS" in doc
print("ok")
"#,
                host = mongo_host(),
                port = mongo_port(),
                database = mongo_database,
            ),
        ])
        .output()
        .expect("probe omit");
    assert!(
        probe_legacy.status.success(),
        "LEGACY_NUM must be omitted: {}",
        String::from_utf8_lossy(&probe_legacy.stderr)
    );

    let balance = pymongo_field_type(&mongo_database, "accounts", 1, "BALANCE_CENTS");
    assert!(
        !balance.contains("float"),
        "safe NUMBER must not be double: {balance}"
    );
}

#[tokio::test]
async fn naive_date_uses_configured_timezone_mongo_utc_datetime() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "events.yaml",
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
    timezone: America/New_York
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
    - name: events
      mode: direct
      source:
        table: EVENTS
      target:
        collection: events
"#,
            host = mongo_host(),
            port = mongo_port(),
        ),
    );

    migrate(&url);
    let out = apply(&url, &config, &doubles);
    assert!(
        out.status.success(),
        "apply failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("status");
    let status_out = String::from_utf8_lossy(&status.stdout);
    assert!(
        status_out.contains("timezone=America/New_York"),
        "status should surface source timezone, got:\n{status_out}"
    );

    let occurred = pymongo_field_type(&mongo_database, "events", 1, "OCCURRED_AT");
    assert!(
        occurred.contains("datetime"),
        "OCCURRED_AT must be Mongo UTC datetime: {occurred}"
    );
    // 2024-01-15 10:30 America/New_York (EST) → 15:30 UTC
    assert!(
        occurred.contains("15, 30") || occurred.contains("15:30"),
        "expected UTC 15:30 from America/New_York naive local, got: {occurred}"
    );

    let aware = pymongo_field_type(&mongo_database, "events", 1, "AWARE_AT");
    assert!(
        aware.contains("datetime"),
        "AWARE_AT must be Mongo UTC datetime: {aware}"
    );
    assert!(
        aware.contains("1, 30") || aware.contains("01:30"),
        "aware +09:00 must normalize to 01:30 UTC, got: {aware}"
    );
}

#[tokio::test]
async fn naive_date_uses_readable_db_timezone_over_configured() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "events-db-tz.yaml",
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
    timezone: America/New_York
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
    - name: events
      mode: direct
      source:
        table: EVENTS
      target:
        collection: events
"#,
            host = mongo_host(),
            port = mongo_port(),
        ),
    );

    migrate(&url);
    let mut out = Command::new(bin());
    out
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .env("MIGRALOOP_STUB_DB_TIMEZONE", "Asia/Tokyo");
    doubles.apply_env(&mut out);
    let out = out
        .args([
            "apply",
            "--platform-store-url",
            &url,
            "--file",
            config.to_str().unwrap(),
        ])
        .output()
        .expect("apply with db tz");

    assert!(
        out.status.success(),
        "apply failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let occurred = pymongo_field_type(&mongo_database, "events", 1, "OCCURRED_AT");
    // 10:30 Asia/Tokyo → 01:30 UTC (DB timezone preferred over configured)
    assert!(
        occurred.contains("1, 30") || occurred.contains("01:30"),
        "DB timezone Asia/Tokyo must win over configured America/New_York, got: {occurred}"
    );
}

#[tokio::test]
async fn unsupported_type_cannot_be_used_as_managed_input() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "blob-managed.yaml",
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
      target:
        collection: customers
      fields:
        BIO:
          as: string
"#,
            host = mongo_host(),
            port = mongo_port(),
        ),
    );

    migrate(&url);
    let out = apply(&url, &config, &doubles);
    assert!(
        !out.status.success(),
        "BLOB as Managed input must fail apply"
    );
    let err = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        err.contains("unsupported") && err.contains("BIO"),
        "expected unsupported Managed failure, got: {err}"
    );
}
