//! Operator-visible seam: Source Alignment Check for Base Datasets (issue #24).
//!
//! Agreed seams (PRD / issue #3):
//! - CLI `align` / `status` / `base` + resulting Base Dataset outcomes
//! - Detect Base≠Source for a controlled mismatch
//! - Repair Base from Source check reads (never write Source)
//! - Resource-gated via `--max-rows` (not a full slam by default)
//!
//! This is the non-ignored contract/stub CI twin of Lab Scenario
//! `source-alignment`. It must not run Lab Fixture / live Oracle.
//!
//! Controlled mismatch is injected by corrupting Platform Store Base rows after
//! Initial Load while the contract Source catalog stays unchanged — proving
//! repair reads Source and writes only Base.

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;
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

fn unique_mongo_database() -> String {
    let suffix = common::unique_suffix();
    format!("appdb_{suffix}")
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

fn deployment_with_direct_delivery(mongo_database: &str) -> String {
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

/// Corrupt a Base Dataset NAME for a given ID without touching Source.
async fn corrupt_base_customer_name(url: &str, customer_id: i64, new_name: &str) {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect Platform Store");

    let rows: Vec<(i32, String)> = sqlx::query_as(
        r#"
        SELECT row_ordinal, row_json
        FROM base_rows
        WHERE deployment_name = 'oracle-to-mongo' AND source_table = 'CUSTOMERS'
        ORDER BY row_ordinal
        "#,
    )
    .fetch_all(&pool)
    .await
    .expect("load base_rows");

    let mut found = false;
    for (ordinal, row_json) in rows {
        let mut value: Value = serde_json::from_str(&row_json).expect("parse row_json");
        let id_matches = match value.get("ID") {
            Some(Value::Number(n)) => n.as_i64() == Some(customer_id),
            Some(Value::String(s)) => s.parse::<i64>().ok() == Some(customer_id),
            _ => false,
        };
        if !id_matches {
            continue;
        }
        value["NAME"] = Value::String(new_name.to_string());
        sqlx::query(
            r#"
            UPDATE base_rows
            SET row_json = $1
            WHERE deployment_name = 'oracle-to-mongo'
              AND source_table = 'CUSTOMERS'
              AND row_ordinal = $2
            "#,
        )
        .bind(value.to_string())
        .bind(ordinal)
        .execute(&pool)
        .await
        .expect("corrupt base row");
        found = true;
        break;
    }
    assert!(found, "expected Base row for CUSTOMERS ID={customer_id}");
}

#[tokio::test]
async fn align_detects_mismatch_repairs_base_without_writing_source() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_delivery(&mongo_database),
    );

    migrate_and_apply(&url, &config);

    // Controlled Base≠Source mismatch: corrupt Base only; Source catalog stays Alice.
    corrupt_base_customer_name(&url, 1, "WRONG").await;

    let base_before = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", "CUSTOMERS"])
        .output()
        .expect("base before align");
    assert!(base_before.status.success());
    let before_out = String::from_utf8_lossy(&base_before.stdout);
    assert!(
        before_out.contains("WRONG") && !before_out.contains("Alice"),
        "Base must show controlled mismatch before align:\n{before_out}"
    );

    let align = Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "align",
            "--platform-store-url",
            &url,
            "--table",
            "CUSTOMERS",
        ])
        .output()
        .expect("run align");
    let align_out = format!(
        "{}{}",
        String::from_utf8_lossy(&align.stdout),
        String::from_utf8_lossy(&align.stderr)
    );
    assert!(
        align.status.success(),
        "align failed: {align_out}"
    );
    let align_lower = align_out.to_ascii_lowercase();
    assert!(
        align_lower.contains("source alignment")
            && (align_lower.contains("mismatched") || align_lower.contains("misaligned"))
            && align_lower.contains("repaired"),
        "align must report detection + repair, got:\n{align_out}"
    );
    assert!(
        !align_lower.contains("write source") && !align_lower.contains("updating source"),
        "align must never claim to write Source, got:\n{align_out}"
    );

    let base_after = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", "CUSTOMERS"])
        .output()
        .expect("base after align");
    assert!(base_after.status.success());
    let after_out = String::from_utf8_lossy(&base_after.stdout);
    assert!(
        after_out.contains("Alice") && !after_out.contains("WRONG"),
        "Base must be repaired from Source to Alice:\n{after_out}"
    );

    // Re-align: Source catalog still Alice (never written); no further mismatches.
    let align2 = Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .args([
            "align",
            "--platform-store-url",
            &url,
            "--table",
            "CUSTOMERS",
        ])
        .output()
        .expect("run align second time");
    let align2_out = format!(
        "{}{}",
        String::from_utf8_lossy(&align2.stdout),
        String::from_utf8_lossy(&align2.stderr)
    );
    assert!(align2.status.success(), "second align failed: {align2_out}");
    assert!(
        align2_out.to_ascii_lowercase().contains("mismatched=0")
            || align2_out.to_ascii_lowercase().contains("mismatchedrows=0"),
        "second align must see Source still Alice (no Source write), got:\n{align2_out}"
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("status after align");
    assert!(status.status.success());
    let status_out = String::from_utf8_lossy(&status.stdout);
    assert!(
        status_out.contains("Source Alignment:")
            && status_out.to_ascii_lowercase().contains("aligned"),
        "status must show Source Alignment after check, got:\n{status_out}"
    );
}

#[tokio::test]
async fn align_is_resource_gated_by_max_rows_not_full_slam() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_delivery(&mongo_database),
    );

    migrate_and_apply(&url, &config);

    // Corrupt Bob (ID=2). With max-rows=1 the check reads only the first Source
    // row (Alice/ID=1) and must not slam/repair the rest of the Base.
    corrupt_base_customer_name(&url, 2, "CORRUPT_BOB").await;

    let align_gated = Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .args([
            "align",
            "--platform-store-url",
            &url,
            "--table",
            "CUSTOMERS",
            "--max-rows",
            "1",
        ])
        .output()
        .expect("run gated align");
    let gated_out = format!(
        "{}{}",
        String::from_utf8_lossy(&align_gated.stdout),
        String::from_utf8_lossy(&align_gated.stderr)
    );
    assert!(
        align_gated.status.success(),
        "gated align failed: {gated_out}"
    );
    let gated_lower = gated_out.to_ascii_lowercase();
    assert!(
        gated_lower.contains("maxrows=1")
            && (gated_lower.contains("partial") || gated_lower.contains("truncated")),
        "resource-gated align must report maxRows=1 and partial/truncated, got:\n{gated_out}"
    );

    let base_gated = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", "CUSTOMERS"])
        .output()
        .expect("base after gated align");
    assert!(base_gated.status.success());
    let gated_base = String::from_utf8_lossy(&base_gated.stdout);
    assert!(
        gated_base.contains("CORRUPT_BOB"),
        "max-rows=1 must not full-slam repair Bob:\n{gated_base}"
    );

    let align_full = Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .args([
            "align",
            "--platform-store-url",
            &url,
            "--table",
            "CUSTOMERS",
            "--max-rows",
            "1000",
        ])
        .output()
        .expect("run full-budget align");
    let full_out = format!(
        "{}{}",
        String::from_utf8_lossy(&align_full.stdout),
        String::from_utf8_lossy(&align_full.stderr)
    );
    assert!(
        align_full.status.success(),
        "full-budget align failed: {full_out}"
    );

    let base_full = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", "CUSTOMERS"])
        .output()
        .expect("base after full align");
    assert!(base_full.status.success());
    let full_base = String::from_utf8_lossy(&base_full.stdout);
    assert!(
        full_base.contains("Bob") && !full_base.contains("CORRUPT_BOB"),
        "larger budget must repair Bob from Source:\n{full_base}"
    );
}
