//! Deployment runtime cutover hand-off seam (ADR-0004 / issue #175).
//!
//! Agreed seams (#168 Testing Decisions / #175 AC):
//! 1. Operator CLI / RQG contract-path twins (`cli_cutover_no_gap`) remain the
//!    Release Quality Gate for gap-free overlap behaviour.
//! 2. Deployment runtime interface — in-process cutover facts, missing-watermark
//!    reject, and successful hand-off through Fake Source/Target Incremental Sync.
//!
//! Operator cutover status lines format from the same [`CutoverFacts`].

mod common;

use std::collections::BTreeMap;

use migraloop_capture::{
    CapturePosition, ChangeEvent, ChangeOp, FakeSource, FakeSourceTable, SourceColumn, SourceEngine,
};
use migraloop_delivery::{FakeTarget, TargetEngine};
use migraloop_platform_store::{
    BaseColumn, BaseDataset, Deployment, Pipeline, PlatformStore, SecretRef, SecretRefKind,
    SystemConnection, TlsSettings,
};
use migraloop_runtime::{
    cutover_facts_from_base, handoff_from_low_watermark, resume_for_incremental, SyncCycleOutcome,
    SyncInvocation, SyncOptions,
};
use serde_json::json;

fn admin_url() -> String {
    std::env::var("MIGRALOOP_TEST_ADMIN_URL")
        .unwrap_or_else(|_| "postgres://migraloop:migraloop@127.0.0.1:5432/postgres".to_string())
}

async fn ephemeral_database_url() -> String {
    let suffix = common::unique_suffix();
    let db_name = format!("migraloop_runtime_cutover_{suffix}");
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

fn fake_deployment(name: &str) -> Deployment {
    Deployment {
        name: name.into(),
        source: SystemConnection {
            kind: "fake".into(),
            host: "unused".into(),
            port: 1,
            database: "unused".into(),
            username: "unused".into(),
            password_ref: SecretRef {
                kind: SecretRefKind::Env,
                value: "UNUSED_PASSWORD".into(),
            },
            timezone: String::new(),
            tls: TlsSettings::default(),
        },
        target: SystemConnection {
            kind: "fake".into(),
            host: "unused".into(),
            port: 1,
            database: "unused".into(),
            username: "unused".into(),
            password_ref: SecretRef {
                kind: SecretRefKind::Env,
                value: "UNUSED_PASSWORD".into(),
            },
            timezone: String::new(),
            tls: TlsSettings::default(),
        },
    }
}

fn customers_pipeline(deployment_name: &str) -> Pipeline {
    Pipeline {
        deployment_name: deployment_name.into(),
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
    }
}

fn customers_base(
    deployment_name: &str,
    low_watermark: Option<i64>,
    checkpoint: Option<i64>,
) -> BaseDataset {
    BaseDataset {
        deployment_name: deployment_name.into(),
        source_schema: String::new(),
        source_table: "CUSTOMERS".into(),
        status: "initial_load_complete".into(),
        primary_key: vec!["ID".into()],
        columns: vec![
            BaseColumn {
                name: "ID".into(),
                data_type: "NUMBER".into(),
                precision: Some(10),
                scale: Some(0),
            },
            BaseColumn {
                name: "NAME".into(),
                data_type: "VARCHAR2".into(),
                precision: None,
                scale: None,
            },
        ],
        omitted_columns: vec![],
        row_count: 1,
        sync_applied_changes: 0,
        sync_health: "unknown".into(),
        capture_low_watermark: low_watermark,
        capture_checkpoint: checkpoint,
        sync_lag: 0,
        source_alignment: "unknown".into(),
        source_alignment_checked_rows: 0,
        source_alignment_mismatched_rows: 0,
        initial_load_cursor: None,
    }
}

fn fake_customers_source(change_name: &str) -> FakeSource {
    let mut identity = BTreeMap::new();
    identity.insert("ID".into(), json!(1));
    let mut updated_row = BTreeMap::new();
    updated_row.insert("ID".into(), json!(1));
    updated_row.insert("NAME".into(), json!(change_name));

    FakeSource::new().with_table(
        "CUSTOMERS",
        FakeSourceTable {
            columns: vec![
                SourceColumn {
                    name: "ID".into(),
                    oracle_type: "NUMBER".into(),
                    supported: true,
                    precision: Some(10),
                    scale: Some(0),
                    size: None,
                },
                SourceColumn {
                    name: "NAME".into(),
                    oracle_type: "VARCHAR2".into(),
                    supported: true,
                    precision: None,
                    scale: None,
                    size: Some(100),
                },
            ],
            primary_key: vec!["ID".into()],
            rows: vec![{
                let mut row = BTreeMap::new();
                row.insert("ID".into(), json!(1));
                row.insert("NAME".into(), json!("Ada"));
                row
            }],
            low_watermark: CapturePosition(1000),
            changes: vec![ChangeEvent {
                table: "CUSTOMERS".into(),
                op: ChangeOp::Update,
                identity,
                row: Some(updated_row),
                position: CapturePosition(1001),
                change_id: "fake-cutover-1001".into(),
            }],
        },
    )
}

/// Missing cutover low-watermark must fail Incremental Sync (gap-tolerant reject).
#[tokio::test]
async fn incremental_sync_rejects_missing_cutover_watermark() {
    let url = ephemeral_database_url().await;
    let store = PlatformStore::open(&url)
        .await
        .expect("open Platform Store session");
    store.migrate().await.expect("migrate via session");

    let deployment = fake_deployment("cutover-missing-wm");
    store
        .upsert_deployment(&deployment)
        .await
        .expect("upsert deployment");
    store
        .replace_pipelines(
            &deployment.name,
            std::slice::from_ref(&customers_pipeline(&deployment.name)),
        )
        .await
        .expect("upsert pipeline");

    let mut seed_row = serde_json::Map::new();
    seed_row.insert("ID".into(), json!(1));
    seed_row.insert("NAME".into(), json!("Ada"));
    store
        .replace_base_dataset(
            &customers_base(&deployment.name, None, None),
            std::slice::from_ref(&seed_row),
        )
        .await
        .expect("seed Base without cutover watermark");

    let facts = cutover_facts_from_base(&customers_base(&deployment.name, None, None));
    assert!(!facts.ready_for_incremental);

    let err = resume_for_incremental("CUSTOMERS", None, None).expect_err("must reject");
    let msg = err.to_string().to_ascii_lowercase();
    assert!(
        msg.contains("watermark") || msg.contains("overlap") || msg.contains("cutover"),
        "rejection must mention watermark/overlap/cutover, got: {msg}"
    );

    let fake_source = fake_customers_source("Ada Lovelace");
    let fake_target = FakeTarget::new();
    assert_eq!(fake_source.kind_label(), "fake");
    assert_eq!(fake_target.kind_label(), "fake");

    let outcome = migraloop_runtime::run_incremental_sync_with_engines(
        &store,
        SyncInvocation::OneShot,
        &fake_source,
        &fake_target,
        SyncOptions::default(),
    )
    .await;
    let err = outcome.expect_err("Incremental without low-watermark must fail");
    let lower = err.to_string().to_ascii_lowercase();
    assert!(
        lower.contains("watermark") || lower.contains("overlap") || lower.contains("cutover"),
        "rejection must mention watermark/overlap/cutover, got: {err}"
    );
}

/// Successful cutover hand-off: wm + wm−1 → Incremental applies overlap-safe change.
#[tokio::test]
async fn cutover_handoff_enables_incremental_sync() {
    let url = ephemeral_database_url().await;
    let store = PlatformStore::open(&url)
        .await
        .expect("open Platform Store session");
    store.migrate().await.expect("migrate via session");

    let deployment = fake_deployment("cutover-handoff");
    store
        .upsert_deployment(&deployment)
        .await
        .expect("upsert deployment");
    store
        .replace_pipelines(
            &deployment.name,
            std::slice::from_ref(&customers_pipeline(&deployment.name)),
        )
        .await
        .expect("upsert pipeline");

    // Same rule Initial Load uses: Source wm → durable hand-off via cutover module.
    let handoff = handoff_from_low_watermark(CapturePosition(1000));
    assert_eq!(handoff.low_watermark, 1000);
    assert_eq!(handoff.checkpoint, 999);

    let mut seed_row = serde_json::Map::new();
    seed_row.insert("ID".into(), json!(1));
    seed_row.insert("NAME".into(), json!("Ada"));
    let dataset = customers_base(
        &deployment.name,
        Some(handoff.low_watermark),
        Some(handoff.checkpoint),
    );
    let facts = cutover_facts_from_base(&dataset);
    assert!(facts.ready_for_incremental);
    assert_eq!(facts.low_watermark, Some(1000));
    assert_eq!(facts.checkpoint, Some(999));

    store
        .replace_base_dataset(&dataset, std::slice::from_ref(&seed_row))
        .await
        .expect("seed Base with cutover hand-off");

    let fake_source = fake_customers_source("Ada Lovelace");
    let fake_target = FakeTarget::new();

    let outcome = migraloop_runtime::run_incremental_sync_with_engines(
        &store,
        SyncInvocation::OneShot,
        &fake_source,
        &fake_target,
        SyncOptions::default(),
    )
    .await
    .expect("Incremental Sync after cutover hand-off");
    assert_eq!(outcome, SyncCycleOutcome::Progressed);

    let (after, rows) = store
        .get_base_rows("CUSTOMERS", Some(&deployment.name))
        .await
        .expect("Base after Incremental");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].data.get("NAME"), Some(&json!("Ada Lovelace")));
    // Checkpoint advanced past the cutover seed (999) to the applied change SCN.
    assert_eq!(after.capture_checkpoint, Some(1001));
    assert_eq!(after.capture_low_watermark, Some(1000));

    let listed = fake_target
        .list_documents("customers")
        .await
        .expect("list via TargetEngine");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["NAME"], json!("Ada Lovelace"));
}
