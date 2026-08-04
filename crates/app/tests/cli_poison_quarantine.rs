//! Operator-visible seam: poison change quarantine (issue #22 / ADR-0015).
//!
//! Agreed seam (PRD / issue #3): CLI `sync` / `status` / `base` / `target` +
//! resulting Base/Target outcomes. After bounded Delivery retries, a single
//! poison Output Identity is quarantined with an Operator-visible alert while
//! the Pipeline continues other changes. Status shows quarantined keys as
//! unhealthy / not aligned (never silent skip; never whole-Pipeline pause).
//!
//! This is the non-ignored contract/stub CI twin of Lab Scenario
//! `poison-quarantine`. It must not run Lab Fixture / live Oracle.
//!
//! Poison Delivery failures are injected via typed SyncOptions CLI flags
//! (`--sync-poison-identity` / `--sync-poison-max-attempts`; issue #180).

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

#[tokio::test]
async fn poison_identity_is_quarantined_pipeline_continues_status_unhealthy() {
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

    // Contract Incremental batch: update identity 1 (Alice→Alicia), insert 3 (Carol),
    // delete 2 (Bob). Poison only identity 1 Delivery so the Pipeline must quarantine
    // that key and still Deliver the other identities.
    let mut sync = Command::new(bin());
    sync
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value");
    doubles.apply_env(&mut sync);
    sync.args(["sync", "--platform-store-url", &url]);
    common::SyncCliOptions {
        poison_identities: vec!["1"],
        poison_max_attempts: Some(2),
        queue_capacity: None,
        delivery_delay_ms: None,
        fail_after_changes: None,
    }
    .append_to(&mut sync);
    let sync = sync.output().expect("run sync");

    assert!(
        sync.status.success(),
        "sync must succeed after quarantine (Pipeline continues): stdout={} stderr={}",
        String::from_utf8_lossy(&sync.stdout),
        String::from_utf8_lossy(&sync.stderr)
    );
    let sync_out = format!(
        "{}{}",
        String::from_utf8_lossy(&sync.stdout),
        String::from_utf8_lossy(&sync.stderr)
    );
    let sync_lower = sync_out.to_ascii_lowercase();
    assert!(
        sync_lower.contains("quarantine") && sync_lower.contains("alert"),
        "expected quarantine alert for poison identity, got:\n{sync_out}"
    );
    assert!(
        !sync_lower.contains("paused"),
        "poison quarantine must not pause the Pipeline, got:\n{sync_out}"
    );

    // Base still applies the poison change (platform advances; Delivery is what quarantines).
    let base = Command::new(bin())
        .args(["base", "--platform-store-url", &url, "--table", "CUSTOMERS"])
        .output()
        .expect("base after sync");
    assert!(base.status.success());
    let base_out = String::from_utf8_lossy(&base.stdout);
    assert!(
        base_out.contains("Alicia") && base_out.contains("Carol") && !base_out.contains("Bob"),
        "Base must apply all Incremental changes including the poison identity, got:\n{base_out}"
    );

    // Target: identity 1 stays at Initial Load (Alice) — not silently skipped as "ok";
    // Carol inserted; Bob deleted.
    let target = Command::new(bin())
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "target",
            "--platform-store-url",
            &url,
            "--collection",
            "customers",
        ])
        .output()
        .expect("target after sync");
    assert!(
        target.status.success(),
        "target inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&target.stdout),
        String::from_utf8_lossy(&target.stderr)
    );
    let target_out = String::from_utf8_lossy(&target.stdout);
    assert!(
        target_out.contains("Alice") && !target_out.contains("Alicia"),
        "poison identity 1 must not Deliver the failing update (stays Alice), got:\n{target_out}"
    );
    assert!(
        (target_out.contains("\"_id\": 3") || target_out.contains("\"_id\":3"))
            && target_out.contains("Carol"),
        "Pipeline must continue and Deliver non-poison identity 3, got:\n{target_out}"
    );
    assert!(
        !(target_out.contains("\"_id\": 2") || target_out.contains("\"_id\":2"))
            && !target_out.contains("Bob"),
        "Pipeline must continue and Deliver delete for identity 2, got:\n{target_out}"
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("status after quarantine");
    assert!(
        status.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    let status_out = String::from_utf8_lossy(&status.stdout);
    let status_lower = status_out.to_ascii_lowercase();
    assert!(
        status_out.contains("Delivery Health: unhealthy")
            && status_out.contains("customers")
            && status_lower.contains("quarantine"),
        "expected Delivery Health unhealthy with quarantine visibility, got:\n{status_out}"
    );
    assert!(
        status_out.contains("identity=1")
            || status_out.contains("identity=1 ")
            || status_lower.contains("identity=1"),
        "expected quarantined Output Identity 1 in status, got:\n{status_out}"
    );
    assert!(
        status_lower.contains("unhealthy") || status_lower.contains("not aligned"),
        "quarantined keys must be marked unhealthy/not aligned, got:\n{status_out}"
    );
}
