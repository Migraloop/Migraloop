//! Operator-visible seam: bounded backpressure with visible lag (issue #26 / ADR-0020).
//!
//! Agreed seam (PRD / issue #3): CLI `sync` / `status` / `target` + resulting
//! Base/Target outcomes. When Downstream (Target Delivery) is artificially slow,
//! stages use a bounded queue and slow capture/apply — lag is visible on Sync
//! Health / Delivery Health, the process does not retain an unbounded pending
//! change buffer, and the Pipeline is not paused merely for slowness.
//!
//! This is the non-ignored contract/stub CI twin of Lab Scenario
//! `bounded-backpressure`. It must not run Lab Fixture / live Oracle.
//!
//! Fault injection:
//! - `MIGRALOOP_INJECT_LOGMINER_CONTENTS` — extra contract LogMiner contents
//! - `MIGRALOOP_DELIVERY_DELAY_MS` — artificial Downstream Delivery slowness
//! - `MIGRALOOP_SYNC_QUEUE_CAPACITY` — bound on in-flight Incremental window
//! - `MIGRALOOP_SYNC_FAIL_AFTER_CHANGES` — mid-sync stop to observe lag

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::json;
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

/// Extra CUSTOMERS inserts beyond the named-scenario fixture (positions ≥ 1080).
fn extra_logminer_backlog(count: usize) -> Vec<migraloop_capture::LogMinerContent> {
    use migraloop_capture::{LogMinerContent, LogMinerOperation};
    use std::collections::BTreeMap;

    let mut contents = Vec::with_capacity(count);
    for i in 0..count {
        let id = 100 + i as i64;
        let scn = 1080 + i as u64;
        contents.push(LogMinerContent {
            scn,
            operation: LogMinerOperation::Insert,
            seg_owner: "APP".to_string(),
            table_name: "CUSTOMERS".to_string(),
            identity: BTreeMap::from([("ID".to_string(), json!(id))]),
            after_image: Some(BTreeMap::from([
                ("ID".to_string(), json!(id)),
                ("NAME".to_string(), json!(format!("User{id}"))),
                ("EMAIL".to_string(), json!(format!("user{id}@example.com"))),
                ("ACTIVE".to_string(), json!(1)),
                ("BIO".to_string(), json!(format!("blob-bytes-{id}"))),
            ])),
            rs_id: String::new(),
            ssn: 0,
        });
    }
    contents
}

fn extract_sync_lag(status: &str) -> Option<i32> {
    status
        .lines()
        .find(|l| l.contains("Sync Health") && l.contains("lag="))
        .and_then(|l| l.split("lag=").nth(1))
        .and_then(|rest| rest.split_whitespace().next())
        .and_then(|n| n.parse().ok())
}

fn extract_delivery_lag(status: &str) -> Option<i32> {
    status
        .lines()
        .find(|l| l.contains("Delivery Health") && l.contains("lag="))
        .and_then(|l| l.split("lag=").nth(1))
        .and_then(|rest| {
            rest.split(|c: char| c.is_whitespace() || c == ',')
                .next()
                .and_then(|n| n.parse().ok())
        })
}

fn max_queue_depth_reported(output: &str) -> Option<i32> {
    let mut max = None;
    for line in output.lines() {
        if let Some(rest) = line.split("queue_depth=").nth(1) {
            if let Some(n) = rest
                .split_whitespace()
                .next()
                .and_then(|s| s.parse::<i32>().ok())
            {
                max = Some(max.map_or(n, |m: i32| m.max(n)));
            }
        }
    }
    max
}

#[tokio::test]
async fn downstream_slowness_applies_bounded_backpressure_with_visible_lag() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    // Named-scenario fixture + 20 extra inserts so backlog exceeds tiny queue.
    let extra = extra_logminer_backlog(20);
    let doubles =
        common::NamedScenarioDoubles::install_with_extra_logminer(dir.path(), &extra);
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_delivery(&mongo_database),
    );
    migrate_and_apply(&url, &config, &doubles);

    const CAPACITY: &str = "2";

    let mut slow = Command::new(bin());
    slow
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .env("MIGRALOOP_SYNC_QUEUE_CAPACITY", CAPACITY)
        .env("MIGRALOOP_DELIVERY_DELAY_MS", "80")
        .env("MIGRALOOP_SYNC_FAIL_AFTER_CHANGES", "1");
    doubles.apply_env(&mut slow);
    let slow = slow
        .args(["sync", "--platform-store-url", &url])
        .output()
        .expect("run sync under Downstream slowness");
    assert!(
        !slow.status.success(),
        "expected mid-sync stop via FAIL_AFTER, got success: {}",
        String::from_utf8_lossy(&slow.stdout)
    );
    let slow_out = format!(
        "{}{}",
        String::from_utf8_lossy(&slow.stdout),
        String::from_utf8_lossy(&slow.stderr)
    );
    let slow_lower = slow_out.to_ascii_lowercase();
    assert!(
        slow_out.contains("Backpressure:") || slow_lower.contains("backpressure"),
        "expected Backpressure signal while Downstream is slow, got:\n{slow_out}"
    );
    let peak = max_queue_depth_reported(&slow_out).expect("queue_depth= in Backpressure lines");
    assert!(
        peak <= 2,
        "queue_depth must stay within capacity=2 (no unbounded buffer), peak={peak}, out:\n{slow_out}"
    );
    assert!(
        !slow_lower.contains("paused")
            || !slow_out.lines().any(|l| {
                l.to_ascii_lowercase().contains("paused")
                    && l.to_ascii_lowercase().contains("pipeline")
                    && !l.contains("FAIL_AFTER")
            }),
        "must not pause the Pipeline merely for Downstream slowness:\n{slow_out}"
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("status under backpressure");
    assert!(
        status.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    let status_out = String::from_utf8_lossy(&status.stdout);
    let sync_lag = extract_sync_lag(&status_out).expect("Sync Health lag=");
    // 20 injected + remaining default fixture changes − 1 applied ≫ window size.
    assert!(
        sync_lag >= 10,
        "Sync Health lag must reflect Source backlog under backpressure (not only window remainder), got lag={sync_lag}:\n{status_out}"
    );
    let delivery_lag = extract_delivery_lag(&status_out).expect("Delivery Health lag=");
    assert!(
        delivery_lag >= 10,
        "Delivery Health lag must reflect Downstream backlog under delay, got lag={delivery_lag}:\n{status_out}"
    );
    assert!(
        !status_out.contains("Delivery Health: paused"),
        "default must not pause Pipeline for mere slowness:\n{status_out}"
    );

    // Catch-up without Downstream delay: apply remaining changes, lag → 0.
    let mut catch_up = Command::new(bin());
    catch_up
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .env("MIGRALOOP_SYNC_QUEUE_CAPACITY", CAPACITY);
    doubles.apply_env(&mut catch_up);
    let catch_up = catch_up
        .args(["sync", "--platform-store-url", &url])
        .output()
        .expect("run catch-up sync");
    assert!(
        catch_up.status.success(),
        "catch-up sync failed: stdout={} stderr={}",
        String::from_utf8_lossy(&catch_up.stdout),
        String::from_utf8_lossy(&catch_up.stderr)
    );
    let catch_out = String::from_utf8_lossy(&catch_up.stdout);
    if let Some(peak2) = max_queue_depth_reported(&catch_out) {
        assert!(
            peak2 <= 2,
            "catch-up must also honor queue capacity, peak={peak2}:\n{catch_out}"
        );
    }

    let status2 = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("status after catch-up");
    assert!(status2.status.success());
    let status2_out = String::from_utf8_lossy(&status2.stdout);
    assert_eq!(
        extract_sync_lag(&status2_out),
        Some(0),
        "Sync Health lag must return to 0 after catch-up:\n{status2_out}"
    );
    assert_eq!(
        extract_delivery_lag(&status2_out),
        Some(0),
        "Delivery Health lag must return to 0 after catch-up:\n{status2_out}"
    );
    assert!(
        !status2_out.contains("Delivery Health: paused"),
        "Pipeline must remain unpaused after backpressure catch-up:\n{status2_out}"
    );

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
        .expect("target after catch-up");
    assert!(
        target.status.success(),
        "target inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&target.stdout),
        String::from_utf8_lossy(&target.stderr)
    );
    let target_out = String::from_utf8_lossy(&target.stdout);
    assert!(
        target_out.contains("User100") && target_out.contains("User119"),
        "Target must receive injected backlog after catch-up, got:\n{target_out}"
    );
}
