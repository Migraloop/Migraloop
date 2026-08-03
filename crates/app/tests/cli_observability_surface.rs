//! Operator-visible seam: Observability Surface (issue #27 / ADR-0008).
//!
//! Agreed seam (PRD / issue #3): CLI Deployment runtime — `sync` / `status` /
//! `run` HTTP metrics, not private module internals.
//!
//! Seams under test:
//! 1. `migraloop sync` stdout/stderr — structured JSON operator event lines
//! 2. `migraloop run --metrics-addr` — Prometheus `/metrics` exposing lag +
//!    alertable failure counters from durable Platform Store state
//! 3. `migraloop status` — Sync Health / Delivery Health / Pipeline status
//!
//! This is the non-ignored contract/stub CI twin of Lab Scenario
//! `observability-surface`. It must not run Lab Fixture / live Oracle.

mod common;

use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

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

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local_addr")
        .port()
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
        mongo_database = mongo_database
    )
}

fn write_injected_logminer_contents(dir: &TempDir, extra_rows: usize) -> PathBuf {
    let path = dir.path().join("inject_contents.json");
    let mut rows = Vec::new();
    for i in 0..extra_rows {
        let id = 100 + i as i64;
        rows.push(serde_json::json!({
            "change_id": format!("inject-{id}"),
            "table": "CUSTOMERS",
            "op": "insert",
            "scn": 9000 + id,
            "rowid": format!("AAAINJ{id:08}"),
            "primary_key": {"ID": id},
            "after": {"ID": id, "NAME": format!("User{id}")}
        }));
    }
    fs::write(&path, serde_json::to_string(&rows).expect("serialize inject")).expect("write inject");
    path
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

fn scrape_metrics(addr: &str) -> String {
    let mut stream =
        std::net::TcpStream::connect(addr).unwrap_or_else(|e| panic!("connect {addr}: {e}"));
    stream
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .expect("write request");
    let mut buf = String::new();
    stream.read_to_string(&mut buf).expect("read response");
    buf
}

fn wait_for_metrics(addr: &str, timeout: Duration) -> String {
    let start = Instant::now();
    loop {
        match std::net::TcpStream::connect(addr) {
            Ok(mut stream) => {
                let _ = stream.write_all(
                    b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
                );
                let mut buf = String::new();
                if stream.read_to_string(&mut buf).is_ok()
                    && buf.contains("HTTP/1.1 200")
                    && buf.contains("migraloop_")
                {
                    return buf;
                }
            }
            Err(_) => {}
        }
        if start.elapsed() > timeout {
            panic!("metrics endpoint at {addr} did not become ready within {timeout:?}");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

struct RunningApp {
    child: Child,
}

impl Drop for RunningApp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_run(url: &str, metrics_addr: &str) -> RunningApp {
    let child = Command::new(bin())
        .args([
            "run",
            "--platform-store-url",
            url,
            "--metrics-addr",
            metrics_addr,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn migraloop run");
    RunningApp { child }
}

fn extract_prometheus_gauge(body: &str, metric: &str) -> Option<f64> {
    for line in body.lines() {
        if line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix(metric) {
            // metric{labels} value  OR  metric value
            let value_str = if rest.starts_with('{') {
                rest.split('}').nth(1)?.trim()
            } else {
                rest.trim()
            };
            if let Some(tok) = value_str.split_whitespace().next() {
                return tok.parse().ok();
            }
        }
    }
    None
}

#[tokio::test]
async fn observability_surface_exposes_metrics_structured_logs_and_health() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_with_direct_delivery(&mongo_database),
    );
    migrate_and_apply(&url, &config);

    // Structured logs on Initial Load / Delivery path (apply already ran).
    // Drive Incremental with Downstream delay + poison so lag + failure counters appear.
    let inject = write_injected_logminer_contents(&dir, 20);
    const CAPACITY: &str = "2";

    let slow = Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .env(
            "MIGRALOOP_INJECT_LOGMINER_CONTENTS",
            inject.to_str().unwrap(),
        )
        .env("MIGRALOOP_SYNC_QUEUE_CAPACITY", CAPACITY)
        .env("MIGRALOOP_DELIVERY_DELAY_MS", "80")
        .env("MIGRALOOP_SYNC_FAIL_AFTER_CHANGES", "1")
        .env("MIGRALOOP_DELIVERY_POISON_IDENTITIES", "100")
        .env("MIGRALOOP_POISON_MAX_ATTEMPTS", "2")
        .args(["sync", "--platform-store-url", &url])
        .output()
        .expect("run sync under Observability Surface probes");
    let sync_out = format!(
        "{}{}",
        String::from_utf8_lossy(&slow.stdout),
        String::from_utf8_lossy(&slow.stderr)
    );
    // Mid-sync stop is expected (FAIL_AFTER); structured events must still appear.
    assert!(
        sync_out.contains("\"event\":\"backpressure\"")
            || sync_out.contains("\"event\": \"backpressure\""),
        "expected structured backpressure event JSON, got:\n{sync_out}"
    );
    assert!(
        sync_out.contains("\"event\":\"incremental_capture\"")
            || sync_out.contains("\"event\": \"incremental_capture\"")
            || sync_out.contains("\"event\":\"poison_quarantine\"")
            || sync_out.contains("\"event\": \"poison_quarantine\""),
        "expected structured Incremental Capture or poison_quarantine event, got:\n{sync_out}"
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("status");
    assert!(
        status.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    let status_out = String::from_utf8_lossy(&status.stdout);
    assert!(
        status_out.contains("Sync Health:") && status_out.contains("lag="),
        "status must include Sync Health with lag, got:\n{status_out}"
    );
    assert!(
        status_out.contains("Delivery Health:") && status_out.contains("Pipeline=customers"),
        "status must include Delivery Health / Pipeline status, got:\n{status_out}"
    );
    assert!(
        status_out.contains("Pipeline: customers"),
        "status must list Pipeline status, got:\n{status_out}"
    );

    let port = free_port();
    let metrics_addr = format!("127.0.0.1:{port}");
    let _app = start_run(&url, &metrics_addr);
    let metrics_body = wait_for_metrics(&metrics_addr, Duration::from_secs(10));
    assert!(
        metrics_body.contains("HTTP/1.1 200"),
        "metrics scrape must return 200, got:\n{metrics_body}"
    );
    assert!(
        metrics_body.contains("migraloop_sync_lag"),
        "Prometheus body must expose sync lag, got:\n{metrics_body}"
    );
    assert!(
        metrics_body.contains("migraloop_delivery_lag"),
        "Prometheus body must expose delivery lag, got:\n{metrics_body}"
    );
    assert!(
        metrics_body.contains("migraloop_quarantined_changes")
            || metrics_body.contains("migraloop_failures"),
        "Prometheus body must expose alertable failure counters, got:\n{metrics_body}"
    );

    let sync_lag = extract_prometheus_gauge(&metrics_body, "migraloop_sync_lag")
        .expect("parse migraloop_sync_lag");
    assert!(
        sync_lag >= 1.0,
        "sync lag metric must reflect backlog under backpressure, got {sync_lag}:\n{metrics_body}"
    );
    let delivery_lag = extract_prometheus_gauge(&metrics_body, "migraloop_delivery_lag")
        .expect("parse migraloop_delivery_lag");
    assert!(
        delivery_lag >= 1.0,
        "delivery lag metric must reflect Downstream backlog, got {delivery_lag}:\n{metrics_body}"
    );

    // Re-scrape to confirm the endpoint stays live (not a one-shot).
    let again = scrape_metrics(&metrics_addr);
    assert!(
        again.contains("migraloop_sync_lag"),
        "second scrape must still expose metrics:\n{again}"
    );
}
