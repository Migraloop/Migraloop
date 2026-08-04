//! Typed Sync options seam for Poison / Schema Change / Backpressure (issue #176).
//!
//! Agreed seams (#168 Testing Decisions / #176 / #180 AC):
//! 1. Operator CLI / RQG contract-path twins remain the Release Quality Gate
//!    (typed SyncOptions CLI flags are the primary fault path; env is a thin shim).
//! 2. Deployment runtime interface — in-process tests exercise fault paths via
//!    typed [`SyncOptions`], not process env vars.
//!
//! This file drives production Incremental Sync with Fake Source/Target and
//! explicit poison identity options (ADR-0015). Schema Change pause (ADR-0009)
//! and bounded Backpressure (ADR-0020) remain distinct internal seams; their
//! Operator-visible outcomes stay covered by existing CLI twins.

mod common;

use std::collections::BTreeMap;

use migraloop_capture::{
    CapturePosition, ChangeEvent, ChangeOp, FakeSource, FakeSourceTable, SourceColumn,
};
use migraloop_delivery::{FakeTarget, TargetEngine};
use migraloop_platform_store::{
    BaseColumn, BaseDataset, Deployment, Pipeline, PlatformStore, SecretRef, SecretRefKind,
    SystemConnection, TlsSettings,
};
use migraloop_runtime::{
    PoisonOptions, SyncCycleOutcome, SyncInvocation, SyncOptions,
};
use serde_json::json;

fn admin_url() -> String {
    std::env::var("MIGRALOOP_TEST_ADMIN_URL")
        .unwrap_or_else(|_| "postgres://migraloop:migraloop@127.0.0.1:5432/postgres".to_string())
}

async fn ephemeral_database_url() -> String {
    let suffix = common::unique_suffix();
    let db_name = format!("migraloop_runtime_sync_opts_{suffix}");
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

fn fake_connection(kind: &str) -> SystemConnection {
    SystemConnection {
        kind: kind.into(),
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

/// Poison quarantine via typed SyncOptions — no env fault injection (issue #176).
#[tokio::test]
async fn typed_sync_options_quarantine_poison_identity_without_env() {
    // Ensure leftover process env cannot satisfy this test — typed options must.
    std::env::remove_var("MIGRALOOP_DELIVERY_POISON_IDENTITIES");
    std::env::remove_var("MIGRALOOP_POISON_MAX_ATTEMPTS");

    let url = ephemeral_database_url().await;
    let store = PlatformStore::open(&url)
        .await
        .expect("open Platform Store session");
    store.migrate().await.expect("migrate via session");

    let deployment = Deployment {
        name: "typed-opts-poison".into(),
        source: fake_connection("fake"),
        target: fake_connection("fake"),
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

    let mut seed_poison = serde_json::Map::new();
    seed_poison.insert("ID".into(), json!(1));
    seed_poison.insert("NAME".into(), json!("Alice"));
    let mut seed_peer = serde_json::Map::new();
    seed_peer.insert("ID".into(), json!(3));
    seed_peer.insert("NAME".into(), json!("Carol"));

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
        row_count: 2,
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
        .replace_base_dataset(&dataset, &[seed_poison, seed_peer])
        .await
        .expect("seed Base Dataset");

    let mut id1 = BTreeMap::new();
    id1.insert("ID".into(), json!(1));
    let mut row1 = BTreeMap::new();
    row1.insert("ID".into(), json!(1));
    row1.insert("NAME".into(), json!("Alicia"));

    let mut id3 = BTreeMap::new();
    id3.insert("ID".into(), json!(3));
    let mut row3 = BTreeMap::new();
    row3.insert("ID".into(), json!(3));
    row3.insert("NAME".into(), json!("Caroline"));

    let fake_source = FakeSource::new().with_table(
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
            rows: vec![],
            low_watermark: CapturePosition(1000),
            changes: vec![
                ChangeEvent {
                    table: "CUSTOMERS".into(),
                    op: ChangeOp::Update,
                    identity: id1,
                    row: Some(row1),
                    position: CapturePosition(1001),
                    change_id: "fake-poison-1001".into(),
                },
                ChangeEvent {
                    table: "CUSTOMERS".into(),
                    op: ChangeOp::Update,
                    identity: id3,
                    row: Some(row3),
                    position: CapturePosition(1002),
                    change_id: "fake-peer-1002".into(),
                },
            ],
        },
    );
    let fake_target = FakeTarget::new();

    // Typed options only — identity key "1" matches JSON number Output Identity.
    let options = SyncOptions {
        poison: PoisonOptions {
            max_attempts: 2,
            poison_identity_keys: ["1".into()].into_iter().collect(),
        },
        ..SyncOptions::default()
    };

    let outcome = migraloop_runtime::run_incremental_sync_with_engines(
        &store,
        SyncInvocation::OneShot,
        &fake_source,
        &fake_target,
        options,
    )
    .await
    .expect("Incremental Sync should succeed after typed-options poison quarantine");
    assert_eq!(outcome, SyncCycleOutcome::Progressed);

    let quarantined = store
        .list_quarantined_changes(Some("typed-opts-poison"))
        .await
        .expect("list quarantine");
    assert!(
        !quarantined.is_empty(),
        "expected poison Output Identity quarantined via typed SyncOptions"
    );
    let poison = quarantined
        .iter()
        .find(|q| q.pipeline_name == "customers")
        .expect("customers quarantine row");
    assert!(
        poison.output_identity.to_string().contains('1'),
        "expected quarantined identity 1, got {}",
        poison.output_identity
    );
    assert_eq!(poison.attempts, 2);

    let pipelines = store.list_pipelines().await.expect("list Pipelines");
    let customers = pipelines
        .iter()
        .find(|p| p.name == "customers")
        .expect("customers pipeline");
    assert!(
        !customers.paused,
        "poison quarantine must not pause the Pipeline (ADR-0015 ≠ ADR-0009)"
    );

    let (_dataset, rows) = store
        .get_base_rows("CUSTOMERS", Some("typed-opts-poison"))
        .await
        .expect("Base rows");
    let base_json = serde_json::to_string(&rows).expect("serialize base");
    assert!(
        base_json.contains("Alicia") && base_json.contains("Caroline"),
        "Base must apply all Incremental changes including poison identity, got:\n{base_json}"
    );

    let listed = fake_target
        .list_documents("customers")
        .await
        .expect("list via TargetEngine");
    let docs = serde_json::to_string(&listed).expect("serialize docs");
    assert!(
        !docs.contains("Alicia"),
        "poison identity must not Deliver the failing update, got:\n{docs}"
    );
    assert!(
        docs.contains("Caroline"),
        "Pipeline must continue and Deliver non-poison peer, got:\n{docs}"
    );
}
