//! Release Quality Gate performance seam (issue #97 / ADR-0028).
//!
//! Fixed Direct Pipeline microbench on the contract/stub path:
//! seed N → Initial Load → Incremental → Delivery.
//! Compares wall-clock duration and throughput to a committed in-repo baseline
//! with an allowed regression percentage. Lab `bulk-load` is never invoked.
//!
//! The timed microbench is `#[ignore]` so `rqg-integration` skips it; `rqg-perf`
//! runs this binary with `--ignored`. Pure regression-budget unit tests below
//! stay always-on.

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::json;
use tempfile::TempDir;

/// Committed baseline shape under `ci/rqg/direct_pipeline_microbench_baseline.json`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
struct PerfBaseline {
    seed_rows: u64,
    duration_ms: u64,
    rows_per_s: f64,
    allowed_regression_pct: f64,
}

#[derive(Debug, Clone, PartialEq)]
struct PerfMeasurement {
    duration_ms: u64,
    rows_per_s: f64,
    rows_applied: u64,
}

#[derive(Debug, Clone, PartialEq)]
enum RegressionVerdict {
    Pass,
    DurationRegression { measured_ms: u64, max_ms: u64 },
    ThroughputRegression { measured: f64, min: f64 },
}

fn max_duration_ms(baseline: &PerfBaseline) -> u64 {
    let factor = 1.0 + (baseline.allowed_regression_pct / 100.0);
    ((baseline.duration_ms as f64) * factor).ceil() as u64
}

fn min_rows_per_s(baseline: &PerfBaseline) -> f64 {
    let factor = 1.0 - (baseline.allowed_regression_pct / 100.0);
    baseline.rows_per_s * factor
}

fn compare_to_baseline(measured: &PerfMeasurement, baseline: &PerfBaseline) -> RegressionVerdict {
    let max_ms = max_duration_ms(baseline);
    if measured.duration_ms > max_ms {
        return RegressionVerdict::DurationRegression {
            measured_ms: measured.duration_ms,
            max_ms,
        };
    }
    let min_rps = min_rows_per_s(baseline);
    if measured.rows_per_s + f64::EPSILON < min_rps {
        return RegressionVerdict::ThroughputRegression {
            measured: measured.rows_per_s,
            min: min_rps,
        };
    }
    RegressionVerdict::Pass
}

fn parse_baseline(raw: &str) -> PerfBaseline {
    serde_json::from_str(raw).expect("baseline JSON")
}

fn throughput(rows: u64, duration: Duration) -> f64 {
    let secs = duration.as_secs_f64();
    if secs <= 0.0 {
        return f64::INFINITY;
    }
    (rows as f64) / secs
}

#[test]
fn regression_budget_allows_within_allowed_percentage() {
    let baseline = PerfBaseline {
        seed_rows: 1000,
        duration_ms: 10_000,
        rows_per_s: 100.0,
        allowed_regression_pct: 20.0,
    };
    // 20% slower duration and 20% lower throughput still pass.
    let measured = PerfMeasurement {
        duration_ms: 12_000,
        rows_per_s: 80.0,
        rows_applied: 1000,
    };
    assert_eq!(
        compare_to_baseline(&measured, &baseline),
        RegressionVerdict::Pass
    );
}

#[test]
fn regression_budget_fails_when_duration_exceeds_allowed_percentage() {
    let baseline = PerfBaseline {
        seed_rows: 1000,
        duration_ms: 10_000,
        rows_per_s: 100.0,
        allowed_regression_pct: 20.0,
    };
    let measured = PerfMeasurement {
        duration_ms: 12_001,
        rows_per_s: 200.0,
        rows_applied: 1000,
    };
    assert_eq!(
        compare_to_baseline(&measured, &baseline),
        RegressionVerdict::DurationRegression {
            measured_ms: 12_001,
            max_ms: 12_000,
        }
    );
}

#[test]
fn regression_budget_fails_when_throughput_drops_beyond_allowed_percentage() {
    let baseline = PerfBaseline {
        seed_rows: 1000,
        duration_ms: 10_000,
        rows_per_s: 100.0,
        allowed_regression_pct: 20.0,
    };
    let measured = PerfMeasurement {
        duration_ms: 5_000,
        rows_per_s: 79.9,
        rows_applied: 1000,
    };
    match compare_to_baseline(&measured, &baseline) {
        RegressionVerdict::ThroughputRegression { measured, min } => {
            assert!((measured - 79.9).abs() < 1e-9);
            assert!((min - 80.0).abs() < 1e-9);
        }
        other => panic!("expected throughput regression, got {other:?}"),
    }
}

#[test]
fn parse_baseline_reads_committed_fields() {
    let raw = r#"{
      "seed_rows": 1000,
      "duration_ms": 15000,
      "rows_per_s": 66.6,
      "allowed_regression_pct": 20
    }"#;
    let b = parse_baseline(raw);
    assert_eq!(b.seed_rows, 1000);
    assert_eq!(b.duration_ms, 15_000);
    assert!((b.rows_per_s - 66.6).abs() < 1e-9);
    assert!((b.allowed_regression_pct - 20.0).abs() < 1e-9);
}

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

fn baseline_path() -> PathBuf {
    if let Ok(path) = std::env::var("MIGRALOOP_RQG_PERF_BASELINE") {
        return PathBuf::from(path);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../ci/rqg/direct_pipeline_microbench_baseline.json")
}

fn load_committed_baseline() -> PerfBaseline {
    let path = baseline_path();
    let raw = fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read baseline {}: {err}", path.display()));
    parse_baseline(&raw)
}

async fn ephemeral_database_url() -> String {
    let suffix = common::unique_suffix();
    let db_name = format!("migraloop_rqg_perf_{suffix}");
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
    format!("rqg_perf_{suffix}")
}

/// Override CUSTOMERS with N rows so Incremental contract contents (Alice/Bob/Carol)
/// still apply, while Initial Load + Delivery dominate the timed work.
fn customers_catalog_json(seed_rows: u64) -> String {
    assert!(seed_rows >= 3, "seed_rows must cover Alice/Bob/Carol identities");
    let mut rows = Vec::with_capacity(seed_rows as usize);
    for id in 1..=seed_rows {
        let (name, email, active) = match id {
            1 => ("Alice", "alice@example.com", 1),
            2 => ("Bob", "bob@example.com", 0),
            3 => ("Carol", "carol@example.com", 1),
            _ => ("", "", 1),
        };
        let name = if id > 3 {
            format!("Customer-{id}")
        } else {
            name.to_string()
        };
        let email = if id > 3 {
            format!("customer-{id}@example.com")
        } else {
            email.to_string()
        };
        rows.push(json!({
            "ID": id,
            "NAME": name,
            "EMAIL": email,
            "ACTIVE": active,
            "BIO": format!("blob-bytes-{id}"),
        }));
    }
    serde_json::to_string_pretty(&json!({
        "tables": [{
            "table": "CUSTOMERS",
            "low_watermark": 1000,
            "primary_key": ["ID"],
            "columns": [
                {
                    "name": "ID",
                    "oracle_type": "NUMBER",
                    "supported": true,
                    "precision": 10,
                    "scale": 0
                },
                {
                    "name": "NAME",
                    "oracle_type": "VARCHAR2",
                    "supported": true
                },
                {
                    "name": "EMAIL",
                    "oracle_type": "VARCHAR2",
                    "supported": true
                },
                {
                    "name": "ACTIVE",
                    "oracle_type": "NUMBER",
                    "supported": true,
                    "precision": 1,
                    "scale": 0
                },
                {
                    "name": "BIO",
                    "oracle_type": "BLOB",
                    "supported": false
                }
            ],
            "rows": rows
        }]
    }))
    .expect("serialize catalog")
}

fn deployment_direct_customers(mongo_database: &str) -> String {
    format!(
        r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: rqg-perf-direct
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

fn write_customers_logminer_inject(dir: &TempDir) -> PathBuf {
    // Named-scenario CUSTOMERS Incremental doubles (Alice→Alicia / Carol insert / Bob delete).
    // Product path no longer bakes these in (issue #120); rqg-perf must inject them.
    let contents = migraloop_capture::named_scenario_logminer_contents();
    let customers_only: Vec<_> = contents
        .into_iter()
        .filter(|c| c.table_name.eq_ignore_ascii_case("CUSTOMERS"))
        .collect();
    write_config(
        dir,
        "customers-logminer.json",
        &serde_json::to_string_pretty(&json!({ "contents": customers_only }))
            .expect("serialize logminer inject"),
    )
}

fn apply_with_catalog(
    url: &str,
    config: &Path,
    catalog_path: &Path,
    logminer_path: &Path,
) -> std::process::Output {
    Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .env(
            "MIGRALOOP_CONTRACT_SOURCE_CATALOG",
            catalog_path.to_str().unwrap(),
        )
        .env(
            "MIGRALOOP_INJECT_LOGMINER_CONTENTS",
            logminer_path.to_str().unwrap(),
        )
        .env("MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING", "all")
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

fn run_sync_with_catalog(
    url: &str,
    catalog_path: &Path,
    logminer_path: &Path,
) -> std::process::Output {
    Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .env(
            "MIGRALOOP_CONTRACT_SOURCE_CATALOG",
            catalog_path.to_str().unwrap(),
        )
        .env(
            "MIGRALOOP_INJECT_LOGMINER_CONTENTS",
            logminer_path.to_str().unwrap(),
        )
        .env("MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING", "all")
        .args(["sync", "--platform-store-url", url])
        .output()
        .expect("run sync")
}

async fn run_timed_microbench(seed_rows: u64) -> PerfMeasurement {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let catalog_path = write_config(&dir, "catalog.json", &customers_catalog_json(seed_rows));
    let logminer_path = write_customers_logminer_inject(&dir);
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_direct_customers(&mongo_database),
    );

    migrate(&url);

    // Timed path: seed N → Initial Load → Incremental → Delivery.
    let started = Instant::now();
    let apply = apply_with_catalog(&url, &config, &catalog_path, &logminer_path);
    assert!(
        apply.status.success(),
        "apply failed: stdout={} stderr={}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
    let apply_out = String::from_utf8_lossy(&apply.stdout);
    assert!(
        apply_out.contains("Initial Load complete") && apply_out.contains("CUSTOMERS"),
        "expected Initial Load for CUSTOMERS, got:\n{apply_out}"
    );
    assert!(
        apply_out.contains("Delivery complete"),
        "expected Delivery after Initial Load, got:\n{apply_out}"
    );

    let sync = run_sync_with_catalog(&url, &catalog_path, &logminer_path);
    assert!(
        sync.status.success(),
        "sync failed: stdout={} stderr={}",
        String::from_utf8_lossy(&sync.stdout),
        String::from_utf8_lossy(&sync.stderr)
    );
    let sync_out = String::from_utf8_lossy(&sync.stdout);
    assert!(
        sync_out.to_ascii_lowercase().contains("incremental")
            || sync_out.to_ascii_lowercase().contains("sync"),
        "expected Incremental Capture progress in sync output, got:\n{sync_out}"
    );
    assert!(
        sync_out.contains("Delivery complete")
            || sync_out.to_ascii_lowercase().contains("deliver"),
        "expected Delivery after Incremental Capture, got:\n{sync_out}"
    );

    let elapsed = started.elapsed();
    let duration_ms = elapsed.as_millis() as u64;
    let rows_per_s = throughput(seed_rows, elapsed);

    // Operator-visible Delivery proof (not timed separately).
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
        .expect("target inspect");
    assert!(
        target.status.success(),
        "target inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&target.stdout),
        String::from_utf8_lossy(&target.stderr)
    );
    let target_out = String::from_utf8_lossy(&target.stdout);
    assert!(
        target_out.contains("Alicia") || target_out.contains("Alice"),
        "expected Delivery rows after IL+Incremental, got:\n{target_out}"
    );

    println!(
        "rqg-perf direct-pipeline microbench seed_rows={seed_rows} duration_ms={duration_ms} rows_per_s={rows_per_s:.3} rows_applied={seed_rows}"
    );

    PerfMeasurement {
        duration_ms,
        rows_per_s,
        rows_applied: seed_rows,
    }
}

#[tokio::test]
#[ignore = "rqg-perf job only — not part of rqg-integration; Lab bulk-load is never invoked"]
async fn direct_pipeline_microbench_meets_committed_baseline() {
    let baseline = load_committed_baseline();
    assert!(
        baseline.seed_rows >= 3,
        "baseline seed_rows must be >= 3 for Alice/Bob/Carol Incremental coverage"
    );
    assert!(
        baseline.allowed_regression_pct > 0.0,
        "allowed_regression_pct must be positive (committed baseline uses ~55 for GHA noise)"
    );

    // Warmup pass (discarded) to reduce cold-start noise on shared runners.
    let _warmup = run_timed_microbench(baseline.seed_rows.min(50).max(3)).await;

    let measured = run_timed_microbench(baseline.seed_rows).await;
    println!(
        "rqg-perf baseline duration_ms={} rows_per_s={:.3} allowed_regression_pct={}",
        baseline.duration_ms, baseline.rows_per_s, baseline.allowed_regression_pct
    );
    println!(
        "rqg-perf limits max_duration_ms={} min_rows_per_s={:.3}",
        max_duration_ms(&baseline),
        min_rows_per_s(&baseline)
    );

    match compare_to_baseline(&measured, &baseline) {
        RegressionVerdict::Pass => {}
        RegressionVerdict::DurationRegression {
            measured_ms,
            max_ms,
        } => panic!(
            "rqg-perf duration regression: measured duration_ms={measured_ms} exceeds max_ms={max_ms} (baseline {} + {}%)",
            baseline.duration_ms, baseline.allowed_regression_pct
        ),
        RegressionVerdict::ThroughputRegression { measured, min } => panic!(
            "rqg-perf throughput regression: measured rows_per_s={measured:.3} below min={min:.3} (baseline {} − {}%)",
            baseline.rows_per_s, baseline.allowed_regression_pct
        ),
    }
}
