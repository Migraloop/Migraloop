//! Operator-visible seam: Platform Store Guardrails + warn-only disk thresholds
//! (issue #28 / ADR-0010).
//!
//! Agreed seam (PRD Testing Decisions / issue #3): CLI Deployment runtime —
//! `migrate` / `status` / `sync` / `run` metrics — not private module internals.
//!
//! Seams under test:
//! 1. Absurdly low store settings are rejected (migrate / status fail with
//!    Guardrails message) — compose ships safe defaults that override lows.
//! 2. Crossing the free-disk warn threshold surfaces WARN + structured event;
//!    `status` still reports Platform Store healthy.
//! 3. Disk threshold alone does not auto-pause Pipelines.
//!
//! This is the non-ignored contract/stub CI twin of Lab Scenario
//! `platform-store-guardrails`. It must not run Lab Fixture / live Oracle.
//!
//! Fault injection:
//! - `MIGRALOOP_INJECT_PLATFORM_STORE_SHARED_BUFFERS_BYTES` (and siblings)
//! - `MIGRALOOP_INJECT_PLATFORM_STORE_FREE_DISK_BYTES`

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

fn migrate_and_apply(url: &str, config: &Path, doubles: &common::NamedScenarioDoubles) {
    migrate(url);

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
            panic!("metrics not ready at {addr} within {timeout:?}");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn kill_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

#[tokio::test]
async fn migrate_rejects_absurdly_low_store_settings() {
    let url = ephemeral_database_url().await;

    let migrate = Command::new(bin())
        .env(
            "MIGRALOOP_INJECT_PLATFORM_STORE_SHARED_BUFFERS_BYTES",
            "1048576",
        )
        .args(["migrate", "--platform-store-url", &url])
        .output()
        .expect("run migrate");

    assert!(
        !migrate.status.success(),
        "migrate must reject absurdly low shared_buffers"
    );
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(&migrate.stdout),
        String::from_utf8_lossy(&migrate.stderr)
    );
    assert!(
        combined.contains("Guardrails") || combined.contains("guardrails"),
        "expected Guardrails rejection, got:\n{combined}"
    );
    assert!(
        combined.to_ascii_lowercase().contains("shared_buffers"),
        "expected shared_buffers in rejection, got:\n{combined}"
    );
}

#[tokio::test]
async fn status_warns_on_low_disk_without_failing_health() {
    let url = ephemeral_database_url().await;
    migrate(&url);

    // 512 MiB free — below 1 GiB warn threshold.
    let status = Command::new(bin())
        .env(
            "MIGRALOOP_INJECT_PLATFORM_STORE_FREE_DISK_BYTES",
            "536870912",
        )
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("run status");

    assert!(
        status.status.success(),
        "disk warn must not fail status: stderr={}",
        String::from_utf8_lossy(&status.stderr)
    );
    let stdout = String::from_utf8_lossy(&status.stdout);
    let stderr = String::from_utf8_lossy(&status.stderr);
    let combined = format!("{stdout}{stderr}");
    assert!(
        stdout.contains("Platform Store: healthy"),
        "store must remain healthy under disk warn:\n{stdout}"
    );
    assert!(
        combined.contains("WARN:") && combined.to_ascii_lowercase().contains("disk"),
        "expected disk WARN, got:\n{combined}"
    );
    assert!(
        combined.contains("not auto-paused") || combined.contains("warn only"),
        "WARN must state warn-only / no auto-pause:\n{combined}"
    );
    assert!(
        combined.contains("\"event\":\"platform_store_disk_warn\"")
            || combined.contains("platform_store_disk_warn"),
        "expected structured platform_store_disk_warn event:\n{combined}"
    );
}

#[tokio::test]
async fn disk_threshold_does_not_auto_pause_pipeline() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());
    let mongo_db = unique_mongo_database();
    let config = write_config(&dir, "deployment.yaml", &deployment_with_direct_delivery(&mongo_db));
    migrate_and_apply(&url, &config, &doubles);

    let status = Command::new(bin())
        .env(
            "MIGRALOOP_INJECT_PLATFORM_STORE_FREE_DISK_BYTES",
            "536870912",
        )
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
        stdout.contains("WARN:") && stdout.to_ascii_lowercase().contains("disk")
            || String::from_utf8_lossy(&status.stderr)
                .to_ascii_lowercase()
                .contains("disk"),
        "expected disk warn with Pipeline present:\n{stdout}"
    );
    assert!(
        !stdout.contains("Delivery Health: paused"),
        "disk threshold must not auto-pause Pipeline:\n{stdout}"
    );
    assert!(
        !stdout
            .lines()
            .any(|line| line.contains("Pipeline:") && line.contains("paused")),
        "Pipeline line must not show paused for disk pressure:\n{stdout}"
    );

    // sync must also refuse to pause solely for disk pressure
    let mut sync = Command::new(bin());
    sync
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .env(
            "MIGRALOOP_INJECT_PLATFORM_STORE_FREE_DISK_BYTES",
            "536870912",
        );
    doubles.apply_env(&mut sync);
    let sync = sync
        .args(["sync", "--platform-store-url", &url])
        .output()
        .expect("run sync");
    assert!(
        sync.status.success(),
        "sync failed under disk warn: stdout={} stderr={}",
        String::from_utf8_lossy(&sync.stdout),
        String::from_utf8_lossy(&sync.stderr)
    );
    let sync_out = format!(
        "{}{}",
        String::from_utf8_lossy(&sync.stdout),
        String::from_utf8_lossy(&sync.stderr)
    );
    assert!(
        !sync_out.to_ascii_lowercase().contains("auto-pause")
            || sync_out.contains("not auto-paused"),
        "sync must not claim auto-pause for disk:\n{sync_out}"
    );

    let status_after = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("run status after sync");
    let after = String::from_utf8_lossy(&status_after.stdout);
    assert!(
        !after.contains("Delivery Health: paused"),
        "Pipeline must remain unpaused after sync under disk warn:\n{after}"
    );
}

#[tokio::test]
async fn run_metrics_expose_disk_warn_gauge() {
    let url = ephemeral_database_url().await;
    migrate(&url);

    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let mut child = Command::new(bin())
        .env(
            "MIGRALOOP_INJECT_PLATFORM_STORE_FREE_DISK_BYTES",
            "536870912",
        )
        .args([
            "run",
            "--platform-store-url",
            &url,
            "--metrics-addr",
            &addr,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn run");

    let text = wait_for_metrics(&addr, Duration::from_secs(10));
    kill_child(&mut child);

    assert!(
        text.contains("migraloop_platform_store_disk_warn"),
        "expected disk warn gauge in metrics:\n{text}"
    );
    assert!(
        text.contains("migraloop_platform_store_disk_warn 1"),
        "disk warn gauge must be 1 when below threshold:\n{text}"
    );
}
