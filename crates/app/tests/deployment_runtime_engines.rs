//! Source / Target engine interface seam (issue #156).
//!
//! Agreed seams (#149 Testing Decisions / #156 AC):
//! 1. Operator CLI contract-path twins remain the Release Quality Gate.
//! 2. Engine interfaces validated with v1 adapters + fakes — swapping adapters
//!    must not require rewriting Sync/Delivery orchestration.
//!
//! This test drives production [`migraloop_runtime::deliver_direct_pipeline_with_options`]
//! with an in-memory [`FakeTarget`], proving Delivery orchestration is trait-bound
//! (Mongo remains covered by existing contract twins / other runtime seams).

mod common;

use migraloop_capture::CONTRACT_SOURCE_CATALOG_ENV;
use migraloop_delivery::{FakeTarget, TargetEngine};
use migraloop_platform_store::{
    BaseColumn, BaseDataset, Deployment, Pipeline, PlatformStore, SecretRef, SecretRefKind,
    SystemConnection, TlsSettings,
};
use serde_json::json;
use tempfile::TempDir;

fn admin_url() -> String {
    std::env::var("MIGRALOOP_TEST_ADMIN_URL")
        .unwrap_or_else(|_| "postgres://migraloop:migraloop@127.0.0.1:5432/postgres".to_string())
}

async fn ephemeral_database_url() -> String {
    let suffix = common::unique_suffix();
    let db_name = format!("migraloop_runtime_engines_{suffix}");
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

#[tokio::test]
async fn production_delivery_orchestration_accepts_fake_target_adapter() {
    let url = ephemeral_database_url().await;
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());

    std::env::set_var("ORACLE_PASSWORD", "oracle-secret-value");
    std::env::set_var(CONTRACT_SOURCE_CATALOG_ENV, &doubles.catalog_path);

    let store = PlatformStore::open(&url)
        .await
        .expect("open Platform Store session");
    store.migrate().await.expect("migrate via session");

    let deployment = Deployment {
        name: "engine-seam".into(),
        source: SystemConnection {
            kind: "oracle".into(),
            host: "stub".into(),
            port: 1521,
            database: "STUB".into(),
            username: "sync_user".into(),
            password_ref: SecretRef {
                kind: SecretRefKind::Env,
                value: "ORACLE_PASSWORD".into(),
            },
            timezone: String::new(),
            tls: TlsSettings::default(),
        },
        target: SystemConnection {
            kind: "mongodb".into(),
            host: "unused".into(),
            port: 27017,
            database: "unused".into(),
            username: "unused".into(),
            password_ref: SecretRef {
                kind: SecretRefKind::Env,
                value: "ORACLE_PASSWORD".into(),
            },
            timezone: String::new(),
            tls: TlsSettings::default(),
        },
    };
    store
        .upsert_deployment(&deployment)
        .await
        .expect("upsert deployment");

    let pipeline = Pipeline {
        deployment_name: deployment.name.clone(),
        name: "customers".into(),
        mode: "direct".into(),
        source_table: "CUSTOMERS".into(),
        source_schema: String::new(),
        target_collection: "customers".into(),
        delivery_status: "pending".into(),
        delivery_applied_changes: 0,
        delivery_lag: 0,
        paused: false,
        description: String::new(),
        field_mappings: Default::default(),
        output_identity: vec![],
        transform_json: None,
        drift_status: "unknown".into(),
        drift_checked_rows: 0,
        drift_mismatched_rows: 0,
    };
    store
        .replace_pipelines(&deployment.name, std::slice::from_ref(&pipeline))
        .await
        .expect("upsert pipeline");

    let mut row = serde_json::Map::new();
    row.insert("ID".into(), json!(1));
    row.insert("NAME".into(), json!("Ada"));
    let dataset = BaseDataset {
        deployment_name: deployment.name.clone(),
        source_schema: String::new(),
        source_table: "CUSTOMERS".into(),
        status: "initial_load_complete".into(),
        primary_key: vec!["ID".into()],
        columns: vec![
            BaseColumn {
                name: "ID".into(),
                oracle_type: "NUMBER".into(),
                precision: Some(10),
                scale: Some(0),
            },
            BaseColumn {
                name: "NAME".into(),
                oracle_type: "VARCHAR2".into(),
                precision: None,
                scale: None,
            },
        ],
        omitted_columns: vec![],
        row_count: 1,
        sync_applied_changes: 0,
        sync_health: "ok".into(),
        capture_low_watermark: Some(1000),
        capture_checkpoint: Some(999),
        sync_lag: 0,
        source_alignment: "unknown".into(),
        source_alignment_checked_rows: 0,
        source_alignment_mismatched_rows: 0,
        initial_load_cursor: None,
    };
    store
        .replace_base_dataset(&dataset, std::slice::from_ref(&row))
        .await
        .expect("seed Base Dataset");

    // Production Delivery verb — Fake Target adapter, no Mongo required.
    let fake = FakeTarget::new();
    assert_eq!(fake.kind_label(), "fake");
    migraloop_runtime::deliver_direct_pipeline_with_options(
        &store, &deployment, &pipeline, &fake, false,
    )
    .await
    .expect("Delivery orchestration against FakeTarget");

    let listed = fake
        .list_documents("customers")
        .await
        .expect("list via TargetEngine");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["NAME"], json!("Ada"));
    assert_eq!(listed[0]["_id"], json!(1));
}
