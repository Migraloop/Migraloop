//! Deployment runtime Observability Surface assembly seam (issue #174).
//!
//! Agreed seams (#168 Testing Decisions / #174 AC):
//! 1. Operator CLI / RQG contract-path twins remain the Release Quality Gate.
//! 2. Deployment runtime interface — in-process Observability assembly for typed
//!    Sync/Delivery Health (+ lag, quarantine, schema-impact, disk-warn).
//!
//! CLI `status` / Prometheus scrape the same assembly facts.

mod common;

use std::collections::BTreeMap;
use std::time::Duration;

use migraloop_capture::{
    LogMinerContent, LogMinerOperation, CONTRACT_SOURCE_CATALOG_ENV, INJECT_LOGMINER_CONTENTS_ENV,
};
use migraloop_platform_store::{
    Deployment, Pipeline, PlatformStore, SecretRef, SecretRefKind, SystemConnection, TlsSettings,
};
use migraloop_runtime::{
    assemble_observability_surface, render_prometheus_metrics, status_inventory, BackpressureOptions,
    SyncHealth, SyncInvocation, SyncOptions,
};
use tempfile::TempDir;

fn admin_url() -> String {
    std::env::var("MIGRALOOP_TEST_ADMIN_URL")
        .unwrap_or_else(|_| "postgres://migraloop:migraloop@127.0.0.1:5432/postgres".to_string())
}

fn mongo_host() -> String {
    std::env::var("MIGRALOOP_TEST_MONGO_HOST").unwrap_or_else(|_| "127.0.0.1".to_string())
}

fn mongo_port() -> i32 {
    std::env::var("MIGRALOOP_TEST_MONGO_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(27017)
}

async fn ephemeral_database_url() -> String {
    let suffix = common::unique_suffix();
    let db_name = format!("migraloop_runtime_obs_{suffix}");
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

fn unique_mongo_database() -> String {
    format!("runtime_obs_appdb_{}", common::unique_suffix())
}

fn sample_deployment(name: &str, mongo_database: &str) -> Deployment {
    Deployment {
        name: name.to_string(),
        source: SystemConnection {
            kind: "oracle".to_string(),
            host: "stub".to_string(),
            port: 1521,
            database: "STUB".to_string(),
            username: "sync_user".to_string(),
            password_ref: SecretRef {
                kind: SecretRefKind::Env,
                value: "ORACLE_PASSWORD".to_string(),
            },
            timezone: String::new(),
            tls: TlsSettings::default(),
        },
        target: SystemConnection {
            kind: "mongodb".to_string(),
            host: mongo_host(),
            port: mongo_port(),
            database: mongo_database.to_string(),
            username: "deliver_user".to_string(),
            password_ref: SecretRef {
                kind: SecretRefKind::Env,
                value: "MONGO_PASSWORD".to_string(),
            },
            timezone: String::new(),
            tls: TlsSettings::default(),
        },
    }
}

fn customers_pipeline(deployment_name: &str) -> Pipeline {
    Pipeline {
        deployment_name: deployment_name.to_string(),
        name: "customers".to_string(),
        mode: "direct".to_string(),
        source_table: "CUSTOMERS".to_string(),
        source_schema: String::new(),
        target_collection: "customers".to_string(),
        delivery_status: "pending".to_string(),
        delivery_applied_changes: 0,
        delivery_lag: 0,
        paused: false,
        description: String::new(),
        field_mappings: BTreeMap::new(),
        output_identity: vec![],
        transform_json: None,
        drift_status: "unknown".to_string(),
        drift_checked_rows: 0,
        drift_mismatched_rows: 0,
    }
}

fn extra_logminer_backlog(count: usize) -> Vec<LogMinerContent> {
    let mut contents = Vec::with_capacity(count);
    for i in 0..count {
        let id = 100 + i as i64;
        let scn = 1080 + i as u64;
        contents.push(LogMinerContent {
            scn,
            operation: LogMinerOperation::Insert,
            seg_owner: "APP".to_string(),
            table_name: "CUSTOMERS".to_string(),
            identity: BTreeMap::from([("ID".to_string(), serde_json::json!(id))]),
            after_image: Some(BTreeMap::from([
                ("ID".to_string(), serde_json::json!(id)),
                ("NAME".to_string(), serde_json::json!(format!("User{id}"))),
                (
                    "EMAIL".to_string(),
                    serde_json::json!(format!("user{id}@example.com")),
                ),
                ("ACTIVE".to_string(), serde_json::json!(1)),
            ])),
            rs_id: format!("0x{scn:x}"),
            ssn: 0,
        });
    }
    contents
}

/// Mid-sync backlog: assembly Sync Health is `lagging`, and Prometheus text
/// agrees on the same lag / failure facts (no disk auto-pause).
#[tokio::test]
async fn runtime_observability_assembly_sync_health_lags_and_matches_prometheus() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let extra = extra_logminer_backlog(20);
    let doubles =
        common::NamedScenarioDoubles::install_with_extra_logminer(dir.path(), &extra);

    std::env::set_var("ORACLE_PASSWORD", "oracle-secret-value");
    std::env::set_var("MONGO_PASSWORD", "mongo-secret-value");
    std::env::set_var(CONTRACT_SOURCE_CATALOG_ENV, &doubles.catalog_path);
    std::env::set_var(INJECT_LOGMINER_CONTENTS_ENV, &doubles.logminer_path);
    // Typed SyncOptions — not process env fault injection (#180).
    std::env::remove_var("MIGRALOOP_SYNC_QUEUE_CAPACITY");
    std::env::remove_var("MIGRALOOP_DELIVERY_DELAY_MS");
    std::env::remove_var("MIGRALOOP_SYNC_FAIL_AFTER_CHANGES");

    let store = PlatformStore::open(&url)
        .await
        .expect("open Platform Store");
    store.migrate().await.expect("migrate");

    let deployment_name = "oracle-to-mongo";
    let deployment = sample_deployment(deployment_name, &mongo_database);
    let pipelines = vec![customers_pipeline(deployment_name)];
    migraloop_runtime::apply(&store, deployment, pipelines)
        .await
        .expect("runtime apply");

    let options = SyncOptions {
        backpressure: BackpressureOptions {
            queue_capacity: 2,
            delivery_delay_ms: Some(80),
        },
        fail_after_changes: Some(1),
        ..SyncOptions::default()
    };
    let sync_result = tokio::time::timeout(
        Duration::from_secs(30),
        migraloop_runtime::run_incremental_sync(&store, SyncInvocation::OneShot, options),
    )
    .await
    .expect("sync should finish or fail within timeout");
    assert!(
        sync_result.is_err(),
        "FAIL_AFTER should stop mid-sync with backlog remaining"
    );

    let inventory = status_inventory(&store)
        .await
        .expect("status inventory via runtime");
    let surface = assemble_observability_surface(&inventory);

    let customers = surface
        .sync
        .iter()
        .find(|s| s.source_table.eq_ignore_ascii_case("CUSTOMERS"))
        .expect("CUSTOMERS Sync Health observation");
    assert_eq!(
        customers.health,
        SyncHealth::Lagging,
        "Sync Health must reflect catch-up backlog beyond ok/unknown placeholders"
    );
    assert!(
        customers.lag >= 1,
        "assembled lag must reflect Source backlog, got {}",
        customers.lag
    );

    let delivery = surface
        .delivery
        .iter()
        .find(|d| d.pipeline_name == "customers")
        .expect("customers Delivery Health observation");
    assert!(
        delivery.lag >= 1,
        "assembled Delivery lag must reflect Downstream backlog, got {}",
        delivery.lag
    );

    // Disk warn remains a fact on the assembly (warn-only); default fixtures are not under warn.
    assert!(
        !surface.disk_warn || surface.free_disk_bytes.is_some(),
        "disk_warn without free_disk_bytes would be inconsistent"
    );

    let metrics = render_prometheus_metrics(&surface);
    assert!(
        metrics.contains(&format!(
            "migraloop_sync_lag{{deployment=\"{deployment_name}\",table=\"CUSTOMERS\"}} {}",
            customers.lag
        )),
        "Prometheus must use the same sync lag as the assembly:\n{metrics}"
    );
    assert!(
        metrics.contains(&format!(
            "migraloop_delivery_lag{{deployment=\"{deployment_name}\",pipeline=\"customers\"}} {}",
            delivery.lag
        )),
        "Prometheus must use the same delivery lag as the assembly:\n{metrics}"
    );
    assert!(
        metrics.contains(&format!("migraloop_failures {}", surface.failure_count)),
        "Prometheus failures must match assembly failure_count:\n{metrics}"
    );
    assert!(
        metrics.contains(&format!(
            "migraloop_platform_store_disk_warn {}",
            if surface.disk_warn { 1 } else { 0 }
        )),
        "Prometheus disk_warn must match assembly (ADR-0010 warn-only):\n{metrics}"
    );
}
