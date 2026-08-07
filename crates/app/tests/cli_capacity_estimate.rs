//! Operator-visible seam: Capacity Estimate command (issue #249 / ADR-0031).
//!
//! Seams under test:
//! 1. `migraloop capacity-estimate` — `limiting_component` + coarse `max_e2e_qps`
//! 2. Injectable component pressure selects limiting component deterministically
//! 3. Command is read-only (never mutates Source/Target DB configuration)
//! 4. Same stable component names as Observability Surface / Lab reports
//!
//! This is the non-ignored contract/stub CI twin for Capacity Estimate behavior.
//! It must not run Lab Fixture / live Oracle.

mod common;

use std::process::Command;

fn admin_url() -> String {
    std::env::var("MIGRALOOP_TEST_ADMIN_URL").unwrap_or_else(|_| {
        "postgres://migraloop:migraloop@127.0.0.1:5432/postgres".to_string()
    })
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

#[tokio::test]
async fn capacity_estimate_reports_limiting_component_and_max_e2e_qps() {
    let url = ephemeral_database_url().await;
    migrate(&url);

    let status_before = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("status before");
    assert!(status_before.status.success());
    let before = String::from_utf8_lossy(&status_before.stdout).to_string();

    let est = Command::new(bin())
        .args([
            "capacity-estimate",
            "--platform-store-url",
            &url,
            "--component-pressure-override",
            "source=95:1,target=10:0,platform_store=10:0,app=5:0",
        ])
        .output()
        .expect("capacity-estimate");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&est.stdout),
        String::from_utf8_lossy(&est.stderr)
    );
    assert!(
        est.status.success(),
        "capacity-estimate must succeed (advisory), got:\n{out}"
    );
    assert!(
        out.contains("limiting_component=source"),
        "expected limiting_component=source, got:\n{out}"
    );
    assert!(
        out.contains("max_e2e_qps=0"),
        "infra-saturated estimate must report coarse max_e2e_qps=0, got:\n{out}"
    );
    assert!(
        out.contains("infra_saturated=yes"),
        "expected infra_saturated=yes, got:\n{out}"
    );
    for name in ["app", "source", "platform_store", "target"] {
        assert!(
            out.contains(&format!("{name}:")),
            "expected component pressure line for {name}, got:\n{out}"
        );
    }
    assert!(
        out.contains("never mutates Source System or Target System"),
        "Operator note must state read-only advisory, got:\n{out}"
    );
    assert!(
        out.contains("resize") && out.contains("not a product failure"),
        "infra-saturated guidance must tell Operators to resize, got:\n{out}"
    );

    // Read-only: Deployment inventory narrative must be unchanged after estimate.
    let status_after = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("status after");
    assert!(status_after.status.success());
    let after = String::from_utf8_lossy(&status_after.stdout).to_string();
    assert_eq!(
        before.lines().filter(|l| l.starts_with("Deployment:")).collect::<Vec<_>>(),
        after.lines().filter(|l| l.starts_with("Deployment:")).collect::<Vec<_>>(),
        "capacity-estimate must not mutate Deployment inventory"
    );
}

#[tokio::test]
async fn capacity_estimate_headroom_when_components_not_saturated() {
    let url = ephemeral_database_url().await;
    migrate(&url);

    let est = Command::new(bin())
        .args([
            "capacity-estimate",
            "--platform-store-url",
            &url,
            "--component-pressure-override",
            "target=40:0,source=10:0,platform_store=10:0,app=5:0",
        ])
        .output()
        .expect("capacity-estimate");
    let out = String::from_utf8_lossy(&est.stdout);
    assert!(est.status.success(), "stderr={}", String::from_utf8_lossy(&est.stderr));
    assert!(
        out.contains("limiting_component=target"),
        "highest non-saturated pressure wins, got:\n{out}"
    );
    assert!(
        out.contains("infra_saturated=no"),
        "expected infra_saturated=no, got:\n{out}"
    );
    // REFERENCE 100000 * (100-40)/100 = 60000
    assert!(
        out.contains("max_e2e_qps=60000"),
        "expected coarse max_e2e_qps=60000, got:\n{out}"
    );
}

#[tokio::test]
async fn status_exposes_same_component_pressure_names() {
    let url = ephemeral_database_url().await;
    migrate(&url);

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("status");
    assert!(status.status.success());
    let out = String::from_utf8_lossy(&status.stdout);
    assert!(
        out.contains("Component pressure:"),
        "status must expose component pressure, got:\n{out}"
    );
    for name in ["app", "source", "platform_store", "target"] {
        assert!(
            out.contains(&format!("  {name}: pressure=")),
            "status must list stable name {name}, got:\n{out}"
        );
    }
}
