//! Operator-visible seam: Drift Check with Managed-field auto-repair (issue #25).
//!
//! Agreed seams (PRD / issue #3):
//! - CLI `drift` / `status` / `target` + resulting Target document outcomes
//! - Manual Managed-field drift is detected
//! - Default auto-repair restores Managed fields
//! - Non-Managed Target fields are not overwritten by repair
//!
//! This is the non-ignored contract/stub CI twin of Lab Scenario `drift-check`.
//! It must not run Lab Fixture / live Oracle.
//!
//! Controlled drift is injected by mutating Target Managed fields after apply
//! (and after Source Alignment so Base is a trusted Drift baseline) while the
//! Platform expected dataset stays correct — proving repair writes Managed
//! fields only via Delivery upsert.

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

fn align_base(url: &str, doubles: &common::NamedScenarioDoubles) {
    let mut align = Command::new(bin());
    align
        .env("ORACLE_PASSWORD", "oracle-secret-value");
    doubles.apply_env(&mut align);
    let align = align
        .args([
            "align",
            "--platform-store-url",
            url,
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
        "align (Drift baseline) failed: {align_out}"
    );
    assert!(
        align_out.to_ascii_lowercase().contains("aligned")
            || align_out.to_ascii_lowercase().contains("source alignment"),
        "align must establish Drift baseline, got:\n{align_out}"
    );
}

/// Mutate Managed NAME and plant a non-Managed EXTRA field on Target identity 1.
fn drift_target_document(database: &str) {
    let status = Command::new("python3")
        .args([
            "-c",
            &format!(
                r#"
from pymongo import MongoClient
c = MongoClient(
    "mongodb://deliver_user:mongo-secret-value@{host}:{port}/{database}?authSource=admin",
    serverSelectionTimeoutMS=5000,
)
coll = c["{database}"]["customers"]
r = coll.update_one(
    {{"_id": 1}},
    {{"$set": {{"NAME": "DRIFTED", "EXTRA": "keep-me"}}}},
)
assert r.matched_count == 1, r.raw_result
print("drifted", r.matched_count)
"#,
                host = mongo_host(),
                port = mongo_port(),
                database = database,
            ),
        ])
        .status()
        .expect("run pymongo drift seed");
    assert!(status.success(), "failed to inject Target Managed-field drift");
}

#[tokio::test]
async fn drift_detects_managed_mismatch_repairs_without_touching_non_managed() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_delivery(&mongo_database),
    );

    migrate_and_apply(&url, &config, &doubles);
    // Domain: Drift baseline requires Source Alignment for Bases.
    align_base(&url, &doubles);

    drift_target_document(&mongo_database);

    let target_before = Command::new(bin())
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "target",
            "--platform-store-url",
            &url,
            "--collection",
            "customers",
        ])
        .output()
        .expect("target before drift");
    assert!(target_before.status.success());
    let before_out = String::from_utf8_lossy(&target_before.stdout);
    assert!(
        before_out.contains("DRIFTED") && !before_out.contains("Alice"),
        "Target must show controlled Managed drift before check:\n{before_out}"
    );
    assert!(
        before_out.contains("keep-me") || before_out.contains("EXTRA"),
        "non-Managed EXTRA must be present before repair:\n{before_out}"
    );

    let mut drift = Command::new(bin());
    drift
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value");
    doubles.apply_env(&mut drift);
    let drift = drift
        .args([
            "drift",
            "--platform-store-url",
            &url,
            "--pipeline",
            "customers",
        ])
        .output()
        .expect("run drift");

    let drift_out = format!(
        "{}{}",
        String::from_utf8_lossy(&drift.stdout),
        String::from_utf8_lossy(&drift.stderr)
    );
    assert!(
        drift.status.success(),
        "drift failed: {drift_out}"
    );
    let drift_lower = drift_out.to_ascii_lowercase();
    assert!(
        drift_lower.contains("drift")
            && (drift_lower.contains("mismatched") || drift_lower.contains("drifted"))
            && drift_lower.contains("repaired"),
        "drift must report detection + Managed auto-repair, got:\n{drift_out}"
    );

    let target_after = Command::new(bin())
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "target",
            "--platform-store-url",
            &url,
            "--collection",
            "customers",
        ])
        .output()
        .expect("target after drift");
    assert!(target_after.status.success());
    let after_out = String::from_utf8_lossy(&target_after.stdout);
    assert!(
        after_out.contains("Alice") && !after_out.contains("DRIFTED"),
        "Managed fields must be auto-repaired to platform expected values:\n{after_out}"
    );
    assert!(
        after_out.contains("keep-me") || after_out.contains("EXTRA"),
        "non-Managed Target field EXTRA must not be overwritten by repair:\n{after_out}"
    );

    // Re-check: no further Managed drift.
    let drift2 = Command::new(bin())
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "drift",
            "--platform-store-url",
            &url,
            "--pipeline",
            "customers",
        ])
        .output()
        .expect("run drift second time");
    let drift2_out = format!(
        "{}{}",
        String::from_utf8_lossy(&drift2.stdout),
        String::from_utf8_lossy(&drift2.stderr)
    );
    assert!(drift2.status.success(), "second drift failed: {drift2_out}");
    assert!(
        drift2_out.to_ascii_lowercase().contains("mismatched=0")
            || drift2_out.to_ascii_lowercase().contains("mismatchedrows=0"),
        "second drift must see Managed fields restored, got:\n{drift2_out}"
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("status after drift");
    assert!(status.status.success());
    let status_out = String::from_utf8_lossy(&status.stdout);
    assert!(
        status_out.contains("Drift:")
            && (status_out.to_ascii_lowercase().contains("ok")
                || status_out.to_ascii_lowercase().contains("aligned")),
        "status must show Drift Check result after check, got:\n{status_out}"
    );
}

#[tokio::test]
async fn drift_is_resource_gated_by_max_rows_not_full_slam() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_delivery(&mongo_database),
    );

    migrate_and_apply(&url, &config, &doubles);
    align_base(&url, &doubles);

    // Drift Bob (ID=2). With max-rows=1 the check only inspects the first
    // expected identity (Alice/ID=1) and must not slam/repair Bob.
    let status = Command::new("python3")
        .args([
            "-c",
            &format!(
                r#"
from pymongo import MongoClient
c = MongoClient(
    "mongodb://deliver_user:mongo-secret-value@{host}:{port}/{database}?authSource=admin",
    serverSelectionTimeoutMS=5000,
)
r = c["{database}"]["customers"].update_one(
    {{"_id": 2}},
    {{"$set": {{"NAME": "CORRUPT_BOB"}}}},
)
assert r.matched_count == 1, r.raw_result
print("drifted bob")
"#,
                host = mongo_host(),
                port = mongo_port(),
                database = mongo_database,
            ),
        ])
        .status()
        .expect("run pymongo bob drift");
    assert!(status.success(), "failed to drift Bob Managed field");

    let drift_gated = Command::new(bin())
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "drift",
            "--platform-store-url",
            &url,
            "--pipeline",
            "customers",
            "--max-rows",
            "1",
        ])
        .output()
        .expect("run gated drift");
    let gated_out = format!(
        "{}{}",
        String::from_utf8_lossy(&drift_gated.stdout),
        String::from_utf8_lossy(&drift_gated.stderr)
    );
    assert!(
        drift_gated.status.success(),
        "gated drift failed: {gated_out}"
    );
    let gated_lower = gated_out.to_ascii_lowercase();
    assert!(
        gated_lower.contains("maxrows=1")
            && (gated_lower.contains("partial") || gated_lower.contains("truncated")),
        "resource-gated drift must report maxRows=1 and partial/truncated, got:\n{gated_out}"
    );

    let target_gated = Command::new(bin())
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "target",
            "--platform-store-url",
            &url,
            "--collection",
            "customers",
        ])
        .output()
        .expect("target after gated drift");
    assert!(target_gated.status.success());
    let gated_target = String::from_utf8_lossy(&target_gated.stdout);
    assert!(
        gated_target.contains("CORRUPT_BOB"),
        "max-rows=1 must not full-slam repair Bob:\n{gated_target}"
    );

    let drift_full = Command::new(bin())
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "drift",
            "--platform-store-url",
            &url,
            "--pipeline",
            "customers",
            "--max-rows",
            "1000",
        ])
        .output()
        .expect("run full-budget drift");
    let full_out = format!(
        "{}{}",
        String::from_utf8_lossy(&drift_full.stdout),
        String::from_utf8_lossy(&drift_full.stderr)
    );
    assert!(
        drift_full.status.success(),
        "full-budget drift failed: {full_out}"
    );

    let target_full = Command::new(bin())
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "target",
            "--platform-store-url",
            &url,
            "--collection",
            "customers",
        ])
        .output()
        .expect("target after full drift");
    assert!(target_full.status.success());
    let full_target = String::from_utf8_lossy(&target_full.stdout);
    assert!(
        full_target.contains("Bob") && !full_target.contains("CORRUPT_BOB"),
        "larger budget must repair Bob Managed fields:\n{full_target}"
    );
}
