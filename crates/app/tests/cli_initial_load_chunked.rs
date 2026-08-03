//! Operator-visible seam: chunked, rate-limited, pausable Initial Load with backoff
//! (issue #124 / CONTEXT.md Initial Load / ADR-0004 cutover preserved).
//!
//! Agreed seam (PRD / issue #3): CLI `apply` / `status` / `base` / `sync` + Base
//! outcomes on the contract/stub harness. Initial Load must not slam an unbounded
//! full-table read into memory as the normal path; Operators observe bounded
//! chunks, can configure/observe a rate limit, pause/resume without tearing down
//! the Deployment, and see backoff under Downstream/store pressure. No-gap
//! Initial↔Incremental overlap remains intact.
//!
//! This is the non-ignored contract/stub CI twin of Lab Scenario
//! `initial-load-throttled`. It must not run Lab Fixture / live Oracle.
//!
//! Knobs / inject:
//! - `MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE` — bounded Source read window
//! - `MIGRALOOP_INITIAL_LOAD_ROWS_PER_SEC` — Operator-visible throttle
//! - `MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS` — mid-load pause inject
//! - `MIGRALOOP_INITIAL_LOAD_STORE_DELAY_MS` — Downstream/store pressure inject

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use serde_json::{json, Value};
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

/// Non-trivial contract catalog: 250 WIDGETS rows (chunked path must not slam).
fn widgets_catalog_json(row_count: usize) -> String {
    let mut rows = Vec::with_capacity(row_count);
    for i in 1..=row_count {
        rows.push(json!({
            "WID": i,
            "LABEL": format!("w{i}"),
        }));
    }
    let catalog = json!({
        "tables": [{
            "table": "WIDGETS",
            "low_watermark": 9000,
            "primary_key": ["WID"],
            "columns": [
                {
                    "name": "WID",
                    "oracle_type": "NUMBER",
                    "supported": true,
                    "precision": 10,
                    "scale": 0
                },
                {
                    "name": "LABEL",
                    "oracle_type": "VARCHAR2",
                    "supported": true
                }
            ],
            "rows": rows
        }]
    });
    serde_json::to_string_pretty(&catalog).expect("serialize catalog")
}

fn deployment_widgets(mongo_database: &str) -> String {
    format!(
        r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: contract-widgets
spec:
  source:
    kind: oracle
    host: contract
    port: 1521
    database: ORCL
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
    - name: widgets
      mode: direct
      source:
        table: WIDGETS
      target:
        collection: widgets
"#,
        host = mongo_host(),
        port = mongo_port(),
    )
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

fn apply_with_env(
    url: &str,
    config: &Path,
    catalog_path: &Path,
    extra_env: &[(&str, &str)],
) -> (bool, String, String) {
    let mut apply = Command::new(bin());
    apply
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .env(
            "MIGRALOOP_CONTRACT_SOURCE_CATALOG",
            catalog_path.to_str().unwrap(),
        )
        .env("MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING", "all");
    for (k, v) in extra_env {
        apply.env(k, v);
    }
    let output = apply
        .args([
            "apply",
            "--platform-store-url",
            url,
            "--file",
            config.to_str().unwrap(),
        ])
        .output()
        .expect("run apply");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

fn status(url: &str) -> String {
    let output = Command::new(bin())
        .args(["status", "--platform-store-url", url])
        .output()
        .expect("run status");
    assert!(
        output.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn count_progress_events(stdout: &str) -> usize {
    stdout
        .lines()
        .filter(|line| {
            line.contains("\"event\":\"initial_load_progress\"")
                || line.contains("\"event\": \"initial_load_progress\"")
        })
        .count()
}

fn progress_chunk_sizes(stdout: &str) -> Vec<i64> {
    stdout
        .lines()
        .filter_map(|line| {
            if !(line.contains("initial_load_progress")) {
                return None;
            }
            let v: Value = serde_json::from_str(line).ok()?;
            v.get("chunk_size")?.as_i64()
        })
        .collect()
}

#[tokio::test]
async fn initial_load_reads_in_bounded_chunks_with_progress() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let catalog = write_config(&dir, "widgets.json", &widgets_catalog_json(250));
    let config = write_config(&dir, "deployment.yaml", &deployment_widgets(&mongo_database));
    migrate(&url);

    let (ok, stdout, stderr) = apply_with_env(
        &url,
        &config,
        &catalog,
        &[
            ("MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE", "50"),
            ("MIGRALOOP_INITIAL_LOAD_ROWS_PER_SEC", "0"),
        ],
    );
    assert!(ok, "apply failed: stdout={stdout} stderr={stderr}");

    let progress = count_progress_events(&stdout);
    assert!(
        progress >= 5,
        "expected >=5 initial_load_progress events for 250 rows / chunk 50, got {progress}:\n{stdout}"
    );
    let chunk_sizes = progress_chunk_sizes(&stdout);
    assert!(
        chunk_sizes.iter().all(|&s| s == 50),
        "expected chunk_size=50 on progress events, got {chunk_sizes:?}"
    );
    assert!(
        stdout.contains("Initial Load complete") && stdout.contains("WIDGETS"),
        "expected Initial Load complete for WIDGETS:\n{stdout}"
    );
    assert!(
        stdout.contains("\"event\":\"initial_load_complete\"")
            || stdout.contains("\"event\": \"initial_load_complete\""),
        "expected initial_load_complete event:\n{stdout}"
    );

    let st = status(&url);
    assert!(
        st.contains("WIDGETS")
            && st.contains("status=initial_load_complete")
            && st.contains("rows=250"),
        "expected complete Base with 250 rows:\n{st}"
    );
    assert!(
        st.contains("low-watermark=9000"),
        "expected durable cutover watermark from chunked load:\n{st}"
    );
}

#[tokio::test]
async fn initial_load_rate_limit_is_observable_and_slows_apply() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let catalog = write_config(&dir, "widgets.json", &widgets_catalog_json(100));
    let config = write_config(&dir, "deployment.yaml", &deployment_widgets(&mongo_database));
    migrate(&url);

    let started = Instant::now();
    let (ok, stdout, stderr) = apply_with_env(
        &url,
        &config,
        &catalog,
        &[
            ("MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE", "25"),
            ("MIGRALOOP_INITIAL_LOAD_ROWS_PER_SEC", "50"),
        ],
    );
    let elapsed = started.elapsed();
    assert!(ok, "apply failed: stdout={stdout} stderr={stderr}");

    assert!(
        stdout.contains("rate_limit=50")
            || stdout.contains("\"rate_limit_rows_per_sec\":50")
            || stdout.contains("\"rate_limit_rows_per_sec\": 50"),
        "expected Operator-visible rate limit of 50 rows/s:\n{stdout}"
    );
    // 100 rows at 50/s needs ~2s of throttle; allow some slack for scheduling.
    assert!(
        elapsed.as_millis() >= 1500,
        "expected rate limit to slow Initial Load (>=1.5s), elapsed={elapsed:?}"
    );
    assert!(
        stdout.contains("Initial Load complete"),
        "expected completion under rate limit:\n{stdout}"
    );
}

#[tokio::test]
async fn initial_load_pause_and_resume_preserves_cutover_watermark() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let catalog = write_config(&dir, "widgets.json", &widgets_catalog_json(200));
    let config = write_config(&dir, "deployment.yaml", &deployment_widgets(&mongo_database));
    migrate(&url);

    let (ok, stdout, stderr) = apply_with_env(
        &url,
        &config,
        &catalog,
        &[
            ("MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE", "40"),
            ("MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS", "2"),
        ],
    );
    assert!(
        ok,
        "paused apply should succeed without tearing down Deployment: stdout={stdout} stderr={stderr}"
    );
    assert!(
        stdout.contains("Initial Load paused")
            || stdout.contains("initial_load_paused")
            || stdout.contains("\"event\":\"initial_load_paused\""),
        "expected pause signal after 2 chunks:\n{stdout}"
    );
    assert!(
        !stdout.contains("Initial Load complete: Base Dataset WIDGETS (200 rows)"),
        "must not complete full load while paused:\n{stdout}"
    );

    let st = status(&url);
    assert!(
        st.contains("status=initial_load_paused") || st.contains("status=initial_load_in_progress"),
        "expected durable in-progress/paused Base status:\n{st}"
    );
    assert!(
        st.contains("rows=80"),
        "expected 2 chunks × 40 rows durable progress:\n{st}"
    );
    assert!(
        st.contains("low-watermark=9000"),
        "watermark must be established before first chunk and survive pause:\n{st}"
    );

    // Resume: re-apply without pause inject continues from durable cursor.
    let (ok2, stdout2, stderr2) = apply_with_env(
        &url,
        &config,
        &catalog,
        &[("MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE", "40")],
    );
    assert!(ok2, "resume apply failed: stdout={stdout2} stderr={stderr2}");
    assert!(
        stdout2.contains("Initial Load complete") && stdout2.contains("200"),
        "expected resume to finish remaining rows:\n{stdout2}"
    );

    let st2 = status(&url);
    assert!(
        st2.contains("status=initial_load_complete") && st2.contains("rows=200"),
        "expected complete Base after resume:\n{st2}"
    );
    assert!(
        st2.contains("low-watermark=9000"),
        "cutover watermark must be unchanged across pause/resume:\n{st2}"
    );
}

#[tokio::test]
async fn initial_load_backs_off_under_store_pressure() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let catalog = write_config(&dir, "widgets.json", &widgets_catalog_json(120));
    let config = write_config(&dir, "deployment.yaml", &deployment_widgets(&mongo_database));
    migrate(&url);

    let (ok, stdout, stderr) = apply_with_env(
        &url,
        &config,
        &catalog,
        &[
            ("MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE", "30"),
            ("MIGRALOOP_INITIAL_LOAD_STORE_DELAY_MS", "40"),
        ],
    );
    assert!(ok, "apply failed: stdout={stdout} stderr={stderr}");

    assert!(
        stdout.contains("Initial Load backoff")
            || stdout.contains("initial_load_backoff")
            || stdout.contains("\"event\":\"initial_load_backoff\""),
        "expected backoff under store pressure:\n{stdout}"
    );
    let progress = count_progress_events(&stdout);
    assert!(
        progress >= 4,
        "backoff path must still load in chunks, got {progress} progress events:\n{stdout}"
    );
    assert!(
        stdout.contains("Initial Load complete"),
        "expected completion after backoff:\n{stdout}"
    );
}
