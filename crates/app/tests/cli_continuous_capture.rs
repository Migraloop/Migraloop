//! Operator-visible seam: continuous Incremental Capture inside `migraloop run`.
//!
//! Agreed seams (issue #145 / PRD #3):
//! 1. Default long-running `migraloop run` continuously runs Incremental Capture →
//!    Affect Analysis → Delivery for active Pipelines without an external sync scheduler
//! 2. Observability Surface (`/metrics`) continues from the same single active instance
//! 3. New Source changes land under continuous catch-up without a separate `migraloop sync`
//!
//! This is the non-ignored contract/stub CI twin for continuous runtime Sync.
//! It must not run Lab Fixture / live Oracle.

mod common;

use std::fs;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

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

struct RunningApp {
    child: Child,
}

impl Drop for RunningApp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_continuous_run(
    url: &str,
    metrics_addr: &str,
    doubles: &common::NamedScenarioDoubles,
) -> RunningApp {
    let mut cmd = Command::new(bin());
    cmd.env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        // Typed SyncOptions poll (#200); do not rely on process env as primary.
        .env_remove("MIGRALOOP_SYNC_POLL_INTERVAL_MS");
    doubles.apply_env(&mut cmd);
    let child = cmd
        .args([
            "run",
            "--platform-store-url",
            url,
            "--metrics-addr",
            metrics_addr,
            // Fast idle poll so the CI twin observes continuous catch-up quickly.
            "--sync-poll-interval-ms",
            "50",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn migraloop run");
    RunningApp { child }
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

fn base_stdout(url: &str) -> String {
    let out = Command::new(bin())
        .args(["base", "--platform-store-url", url, "--table", "CUSTOMERS"])
        .output()
        .expect("base inspect");
    assert!(
        out.status.success(),
        "base failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn target_stdout(url: &str) -> String {
    let out = Command::new(bin())
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "target",
            "--platform-store-url",
            url,
            "--collection",
            "customers",
        ])
        .output()
        .expect("target inspect");
    assert!(
        out.status.success(),
        "target failed: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn wait_until(timeout: Duration, mut probe: impl FnMut() -> bool, label: &str) {
    let start = Instant::now();
    loop {
        if probe() {
            return;
        }
        if start.elapsed() > timeout {
            panic!("timed out waiting for {label} within {timeout:?}");
        }
        thread::sleep(Duration::from_millis(100));
    }
}

fn append_late_source_insert(doubles: &common::NamedScenarioDoubles, id: i64, scn: u64) {
    let raw = fs::read_to_string(&doubles.logminer_path).expect("read logminer inject");
    let mut file: serde_json::Value = serde_json::from_str(&raw).expect("parse logminer inject");
    let contents = file
        .get_mut("contents")
        .and_then(|v| v.as_array_mut())
        .expect("contents array");
    contents.push(json!({
        "scn": scn,
        "operation": "INSERT",
        "seg_owner": "APP",
        "table_name": "CUSTOMERS",
        "identity": { "ID": id },
        "after_image": {
            "ID": id,
            "NAME": format!("Late{id}"),
            "EMAIL": format!("late{id}@example.com"),
            "ACTIVE": 1,
            "BIO": format!("blob-bytes-late-{id}")
        }
    }));
    fs::write(
        &doubles.logminer_path,
        serde_json::to_string_pretty(&file).expect("serialize inject"),
    )
    .expect("rewrite logminer inject");
}

#[tokio::test]
async fn continuous_run_catches_up_source_changes_without_manual_sync() {
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

    // After Initial Load, named-scenario Incremental rows are pending. Continuous
    // `run` must apply them without a separate `migraloop sync` invocation.
    let before = base_stdout(&url);
    assert!(
        before.contains("Alice") && before.contains("Bob"),
        "expected Initial Load baseline before continuous catch-up, got:\n{before}"
    );
    assert!(
        !before.contains("Alicia"),
        "Incremental update must not be applied before continuous run starts, got:\n{before}"
    );

    let port = free_port();
    let metrics_addr = format!("127.0.0.1:{port}");
    let _app = start_continuous_run(&url, &metrics_addr, &doubles);

    // Same single active instance must serve Observability metrics while capturing.
    let metrics_body = wait_for_metrics(&metrics_addr, Duration::from_secs(10));
    assert!(
        metrics_body.contains("migraloop_sync_lag")
            || metrics_body.contains("migraloop_delivery_lag"),
        "continuous run must expose Observability metrics, got:\n{metrics_body}"
    );

    wait_until(
        Duration::from_secs(20),
        || {
            let base = base_stdout(&url);
            base.contains("Alicia")
                && base.contains("Carol")
                && !base.contains("Bob")
                && !base.contains("bob@example.com")
        },
        "named-scenario Incremental Capture via continuous run",
    );

    wait_until(
        Duration::from_secs(20),
        || {
            let target = target_stdout(&url);
            (target.contains("\"_id\": 1") || target.contains("\"_id\":1"))
                && target.contains("Alicia")
                && (target.contains("\"_id\": 3") || target.contains("\"_id\":3"))
                && target.contains("Carol")
                && !(target.contains("\"_id\": 2") || target.contains("\"_id\":2"))
        },
        "Delivery via continuous run",
    );

    // Append a later Source change while `run` is already alive — no manual sync.
    append_late_source_insert(&doubles, 99, 2000);

    wait_until(
        Duration::from_secs(20),
        || {
            let base = base_stdout(&url);
            base.contains("Late99") && base.contains("late99@example.com")
        },
        "late Source insert catch-up without manual sync",
    );

    wait_until(
        Duration::from_secs(20),
        || {
            let target = target_stdout(&url);
            (target.contains("\"_id\": 99") || target.contains("\"_id\":99"))
                && target.contains("Late99")
        },
        "late Source insert Delivery without manual sync",
    );

    // Metrics remain live on the same process after continuous catch-up.
    let again = scrape_metrics(&metrics_addr);
    assert!(
        again.contains("HTTP/1.1 200") && again.contains("migraloop_"),
        "Observability Surface must stay up on the continuous run instance:\n{again}"
    );
}

#[tokio::test]
async fn continuous_run_honors_pause_while_base_capture_continues() {
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

    // Drain the named-scenario Incremental batch first via continuous run.
    let port = free_port();
    let metrics_addr = format!("127.0.0.1:{port}");
    let _app = start_continuous_run(&url, &metrics_addr, &doubles);
    let _ = wait_for_metrics(&metrics_addr, Duration::from_secs(10));
    wait_until(
        Duration::from_secs(20),
        || base_stdout(&url).contains("Alicia"),
        "initial continuous catch-up before pause",
    );

    let pause = Command::new(bin())
        .args([
            "pause",
            "--platform-store-url",
            &url,
            "--pipeline",
            "customers",
        ])
        .output()
        .expect("pause");
    assert!(
        pause.status.success(),
        "pause failed: {}",
        String::from_utf8_lossy(&pause.stderr)
    );

    append_late_source_insert(&doubles, 77, 2100);

    // Shared Base Incremental Capture continues while paused Pipelines skip Delivery.
    wait_until(
        Duration::from_secs(20),
        || {
            let base = base_stdout(&url);
            base.contains("Late77") && base.contains("late77@example.com")
        },
        "Base catch-up under pause via continuous run",
    );

    // Give continuous Delivery a couple of poll cycles; paused Pipeline must not Deliver.
    thread::sleep(Duration::from_millis(400));
    let target_paused = target_stdout(&url);
    assert!(
        !(target_paused.contains("\"_id\": 77") || target_paused.contains("\"_id\":77"))
            && !target_paused.contains("Late77"),
        "paused Pipeline must not Deliver under continuous capture, got:\n{target_paused}"
    );

    let mut resume = Command::new(bin());
    resume
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value");
    doubles.apply_env(&mut resume);
    let resume = resume
        .args([
            "resume",
            "--platform-store-url",
            &url,
            "--pipeline",
            "customers",
        ])
        .output()
        .expect("resume");
    assert!(
        resume.status.success(),
        "resume failed: {}",
        String::from_utf8_lossy(&resume.stderr)
    );

    wait_until(
        Duration::from_secs(20),
        || {
            let target = target_stdout(&url);
            (target.contains("\"_id\": 77") || target.contains("\"_id\":77"))
                && target.contains("Late77")
        },
        "resume catch-up Delivery under continuous run",
    );
}
