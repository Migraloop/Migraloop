//! Source / Target engine interface seam (issues #156 / #169 / #206).
//!
//! Agreed seams (#149 / #199 Testing Decisions / #156 / #169 / #206 AC):
//! 1. Operator CLI contract-path twins remain the Release Quality Gate.
//! 2. Engine interfaces validated with v1 adapters + fakes — swapping adapters
//!    must not require rewriting Sync/Delivery orchestration.
//! 3. Full Incremental Sync accepts injected Source/Target engines so Fake
//!    adapters exercise the production Sync path without orchestration kind gates.
//! 4. Apply / Initial Load accepts injected engines (`apply_with_engines`) so Fake
//!    is not Incremental-only (#206).
//!
//! This file drives production Incremental Sync and apply with in-memory Fake
//! adapters (Mongo/Oracle contract twins cover the default factory path elsewhere).
//! Direct Base→Target Delivery with Fake Target is also covered in-crate by the
//! runtime engine-seam unit tests (`deliver_initial_load_chunk_via_engines`);
//! the public runtime surface no longer exposes Delivery helper verbs (#172).

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
use migraloop_runtime::{ApplyOptions, SyncCycleOutcome, SyncInvocation, SyncOptions};
use serde_json::json;

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

/// Full production Incremental Sync with Fake Source + Fake Target (issue #169).
///
/// Deployment kinds are intentionally non-oracle / non-mongodb so the injected
/// path cannot lean on Oracle-kind string gates or default factories.
#[tokio::test]
async fn production_incremental_sync_accepts_fake_source_and_target_engines() {
    let url = ephemeral_database_url().await;

    let store = PlatformStore::open(&url)
        .await
        .expect("open Platform Store session");
    store.migrate().await.expect("migrate via session");

    let deployment = Deployment {
        name: "fake-sync-seam".into(),
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
    };
    store
        .upsert_deployment(&deployment)
        .await
        .expect("upsert deployment");

    let pipeline = customers_direct_pipeline(&deployment.name);
    store
        .replace_pipelines(&deployment.name, std::slice::from_ref(&pipeline))
        .await
        .expect("upsert pipeline");

    let seed_row: serde_json::Map<String, serde_json::Value> =
        customers_seed_row().into_iter().collect();
    let dataset = BaseDataset {
        deployment_name: deployment.name.clone(),
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
        .replace_base_dataset(&dataset, std::slice::from_ref(&seed_row))
        .await
        .expect("seed Base Dataset");

    let mut identity = BTreeMap::new();
    identity.insert("ID".into(), json!(1));
    let mut updated_row = BTreeMap::new();
    updated_row.insert("ID".into(), json!(1));
    updated_row.insert("NAME".into(), json!("Ada Lovelace"));

    let fake_source = FakeSource::new().with_table(
        "CUSTOMERS",
        customers_fake_table(vec![ChangeEvent {
            table: "CUSTOMERS".into(),
            op: ChangeOp::Update,
            identity,
            row: Some(updated_row),
            position: CapturePosition(1001),
            change_id: "fake-change-1001".into(),
        }]),
    );
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
    .await
    .expect("production Incremental Sync with Fake Source/Target");
    assert_eq!(outcome, SyncCycleOutcome::Progressed);

    let (_dataset, rows) = store
        .get_base_rows("CUSTOMERS", Some("fake-sync-seam"))
        .await
        .expect("Base rows after Sync");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].data.get("NAME"), Some(&json!("Ada Lovelace")));

    let listed = fake_target
        .list_documents("customers")
        .await
        .expect("list via TargetEngine");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["NAME"], json!("Ada Lovelace"));
    assert_eq!(listed[0]["_id"], json!(1));
}

fn fake_system_connection() -> SystemConnection {
    SystemConnection {
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
    }
}

fn customers_source_columns() -> Vec<SourceColumn> {
    vec![
        SourceColumn {
            name: "ID".into(),
            data_type: "NUMBER".into(),
            supported: true,
            precision: Some(10),
            scale: Some(0),
            size: None,
        },
        SourceColumn {
            name: "NAME".into(),
            data_type: "VARCHAR2".into(),
            supported: true,
            precision: None,
            scale: None,
            size: Some(100),
        },
    ]
}

fn customers_seed_row() -> BTreeMap<String, serde_json::Value> {
    let mut row = BTreeMap::new();
    row.insert("ID".into(), json!(1));
    row.insert("NAME".into(), json!("Ada"));
    row
}

fn customers_direct_pipeline(deployment_name: &str) -> Pipeline {
    Pipeline {
        deployment_name: deployment_name.to_string(),
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

fn customers_fake_table(changes: Vec<ChangeEvent>) -> FakeSourceTable {
    FakeSourceTable {
        columns: customers_source_columns(),
        primary_key: vec!["ID".into()],
        rows: vec![customers_seed_row()],
        low_watermark: CapturePosition(1000),
        changes,
    }
}

/// Full production apply / Initial Load / Delivery with Fake Source + Fake Target (#206).
///
/// Deployment kinds are intentionally non-oracle / non-mongodb so the injected
/// path cannot lean on orchestration kind gates or default factories.
#[tokio::test]
async fn production_apply_accepts_fake_source_and_target_engines() {
    let url = ephemeral_database_url().await;

    let store = PlatformStore::open(&url)
        .await
        .expect("open Platform Store session");
    store.migrate().await.expect("migrate via session");

    let deployment = Deployment {
        name: "fake-apply-seam".into(),
        source: fake_system_connection(),
        target: fake_system_connection(),
    };

    let pipeline = customers_direct_pipeline(&deployment.name);

    let fake_source =
        FakeSource::new().with_table("CUSTOMERS", customers_fake_table(vec![]));
    let fake_target = FakeTarget::new();
    assert_eq!(fake_source.kind_label(), "fake");
    assert_eq!(fake_target.kind_label(), "fake");

    migraloop_runtime::apply_with_engines(
        &store,
        deployment,
        vec![pipeline],
        ApplyOptions::default(),
        &fake_source,
        &fake_target,
    )
    .await
    .expect("production apply with Fake Source/Target");

    let (dataset, rows) = store
        .get_base_rows("CUSTOMERS", Some("fake-apply-seam"))
        .await
        .expect("Base rows after apply Initial Load");
    assert_eq!(dataset.status, "initial_load_complete");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].data.get("NAME"), Some(&json!("Ada")));

    let listed = fake_target
        .list_documents("customers")
        .await
        .expect("list via TargetEngine after apply Delivery");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["NAME"], json!("Ada"));
    assert_eq!(listed[0]["_id"], json!(1));
}

/// Factory Sync path fails via factory kind selection — not an orchestration
/// `kind == "oracle"` gate (#206).
#[tokio::test]
async fn factory_incremental_sync_rejects_non_oracle_kind_via_factory() {
    let url = ephemeral_database_url().await;
    let store = PlatformStore::open(&url)
        .await
        .expect("open Platform Store session");
    store.migrate().await.expect("migrate via session");

    let deployment = Deployment {
        name: "factory-kind-seam".into(),
        source: fake_system_connection(),
        target: SystemConnection {
            kind: "mongodb".into(),
            host: "127.0.0.1".into(),
            port: 27017,
            database: "unused".into(),
            username: "unused".into(),
            password_ref: SecretRef {
                kind: SecretRefKind::Env,
                value: "UNUSED_PASSWORD".into(),
            },
            timezone: String::new(),
            tls: TlsSettings::default(),
        },
    };
    store
        .upsert_deployment(&deployment)
        .await
        .expect("upsert deployment");
    let pipeline = customers_direct_pipeline(&deployment.name);
    store
        .replace_pipelines(&deployment.name, std::slice::from_ref(&pipeline))
        .await
        .expect("upsert pipeline");
    store
        .replace_base_dataset(
            &BaseDataset {
                deployment_name: deployment.name.clone(),
                source_schema: String::new(),
                source_table: "CUSTOMERS".into(),
                status: "initial_load_complete".into(),
                primary_key: vec!["ID".into()],
                columns: vec![BaseColumn {
                    name: "ID".into(),
                    data_type: "NUMBER".into(),
                    precision: Some(10),
                    scale: Some(0),
                }],
                omitted_columns: vec![],
                row_count: 0,
                sync_applied_changes: 0,
                sync_health: "ok".into(),
                capture_low_watermark: Some(1000),
                capture_checkpoint: Some(999),
                sync_lag: 0,
                source_alignment: "unknown".into(),
                source_alignment_checked_rows: 0,
                source_alignment_mismatched_rows: 0,
                initial_load_cursor: None,
            },
            &[],
        )
        .await
        .expect("seed Base Dataset");

    let err = migraloop_runtime::run_incremental_sync(
        &store,
        SyncInvocation::OneShot,
        SyncOptions::default(),
    )
    .await
    .expect_err("non-oracle kind must fail at Source factory");
    let msg = err.to_string();
    assert!(
        msg.contains("unsupported Source System kind") && msg.contains("fake"),
        "expected factory kind error, got: {msg}"
    );
    assert!(
        !msg.contains("requires an Oracle Source System"),
        "orchestration must not re-gate on Oracle brand, got: {msg}"
    );
}
