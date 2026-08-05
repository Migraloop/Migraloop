//! Platform Store Deployment-intent persistence verbs (issues #177 / #204 / #168).
//!
//! Agreed seam (#168 / #199 Testing Decisions): exercise session intent verbs for
//! Sync/Delivery progress, quarantine, schema-impact, Source Alignment, Drift,
//! and Pipeline lifecycle composites — not fine-grained column CRUD sequencing.
//! Postgres remains the only store engine (ADR-0001).

mod common;

use migraloop_platform_store::{
    BaseColumn, BaseDataset, Deployment, Pipeline, PlatformStore, QuarantinedChange,
    SchemaChangeImpact, SecretRef, SecretRefKind, SystemConnection, TlsSettings,
};

fn admin_url() -> String {
    std::env::var("MIGRALOOP_TEST_ADMIN_URL").unwrap_or_else(|_| {
        "postgres://migraloop:migraloop@127.0.0.1:5432/postgres".to_string()
    })
}

async fn ephemeral_database_url() -> String {
    let suffix = common::unique_suffix();
    let db_name = format!("migraloop_intent_{suffix}");
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

fn sample_deployment(name: &str) -> Deployment {
    Deployment {
        name: name.to_string(),
        source: SystemConnection {
            kind: "oracle".to_string(),
            host: "oracle.example.com".to_string(),
            port: 1521,
            database: "ORCLPDB1".to_string(),
            username: "sync_user".to_string(),
            password_ref: SecretRef {
                kind: SecretRefKind::Env,
                value: "ORACLE_PASSWORD".to_string(),
            },
            timezone: "UTC".to_string(),
            tls: TlsSettings::default(),
        },
        target: SystemConnection {
            kind: "mongodb".to_string(),
            host: "mongo.example.com".to_string(),
            port: 27017,
            database: "appdb".to_string(),
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

fn sample_pipeline(deployment_name: &str, name: &str) -> Pipeline {
    Pipeline {
        deployment_name: deployment_name.to_string(),
        name: name.to_string(),
        mode: "direct".to_string(),
        source_table: "CUSTOMERS".to_string(),
        source_schema: "APP".to_string(),
        target_collection: "customers".to_string(),
        delivery_status: "pending".to_string(),
        delivery_applied_changes: 0,
        delivery_lag: 0,
        paused: false,
        description: String::new(),
        field_mappings: Default::default(),
        output_identity: vec!["ID".to_string()],
        transform_json: None,
        drift_status: "unknown".to_string(),
        drift_checked_rows: 0,
        drift_mismatched_rows: 0,
    }
}

fn sample_base(deployment_name: &str) -> BaseDataset {
    BaseDataset {
        deployment_name: deployment_name.to_string(),
        source_table: "CUSTOMERS".to_string(),
        source_schema: "APP".to_string(),
        status: "incremental".to_string(),
        primary_key: vec!["ID".to_string()],
        columns: vec![BaseColumn {
            name: "ID".to_string(),
            data_type: "NUMBER".to_string(),
            precision: Some(10),
            scale: Some(0),
        }],
        omitted_columns: vec![],
        row_count: 1,
        sync_applied_changes: 1,
        sync_health: "ok".to_string(),
        capture_low_watermark: Some(100),
        capture_checkpoint: Some(120),
        sync_lag: 0,
        source_alignment: "unknown".to_string(),
        source_alignment_checked_rows: 0,
        source_alignment_mismatched_rows: 0,
        initial_load_cursor: None,
    }
}

async fn open_migrated_store() -> PlatformStore {
    let url = ephemeral_database_url().await;
    let store = PlatformStore::open(&url)
        .await
        .expect("open Platform Store session");
    store.migrate().await.expect("migrate via session");
    store
}

#[tokio::test]
async fn record_sync_window_progress_persists_base_and_applied_change_ids() {
    let store = open_migrated_store().await;
    store
        .upsert_deployment(&sample_deployment("intent-sync"))
        .await
        .expect("upsert Deployment");

    let mut row = serde_json::Map::new();
    row.insert("ID".to_string(), serde_json::json!(1));
    let dataset = sample_base("intent-sync");

    store
        .record_sync_window_progress(
            &dataset,
            &[row],
            &[("chg-120".to_string(), 120)],
        )
        .await
        .expect("record Sync window progress");

    let (loaded, rows) = store
        .get_base_rows("CUSTOMERS", Some("intent-sync"))
        .await
        .expect("load Base");
    assert_eq!(loaded.sync_applied_changes, 1);
    assert_eq!(loaded.capture_checkpoint, Some(120));
    assert_eq!(loaded.sync_health, "ok");
    assert_eq!(rows.len(), 1);

    let unapplied = store
        .filter_unapplied_change_ids(
            "intent-sync",
            "APP",
            "CUSTOMERS",
            &["chg-120".to_string(), "chg-121".to_string()],
        )
        .await
        .expect("filter applied");
    assert_eq!(unapplied, vec!["chg-121".to_string()]);
}

#[tokio::test]
async fn record_delivery_progress_updates_status_applied_and_lag() {
    let store = open_migrated_store().await;
    store
        .upsert_deployment(&sample_deployment("intent-delivery"))
        .await
        .expect("upsert Deployment");
    store
        .replace_pipelines(
            "intent-delivery",
            &[sample_pipeline("intent-delivery", "customers")],
        )
        .await
        .expect("replace Pipelines");

    store
        .record_delivery_progress(
            "intent-delivery",
            "customers",
            Some("delivered"),
            Some(3),
            Some(7),
        )
        .await
        .expect("record Delivery progress");

    let pipeline = store
        .list_pipelines()
        .await
        .expect("list")
        .into_iter()
        .find(|p| p.name == "customers")
        .expect("pipeline");
    assert_eq!(pipeline.delivery_status, "delivered");
    assert_eq!(pipeline.delivery_applied_changes, 3);
    assert_eq!(pipeline.delivery_lag, 7);

    // Lag-only update must not rewrite status or applied count.
    store
        .record_delivery_progress("intent-delivery", "customers", None, None, Some(0))
        .await
        .expect("record Delivery lag");
    let pipeline = store
        .list_pipelines()
        .await
        .expect("relist")
        .into_iter()
        .find(|p| p.name == "customers")
        .expect("pipeline");
    assert_eq!(pipeline.delivery_status, "delivered");
    assert_eq!(pipeline.delivery_applied_changes, 3);
    assert_eq!(pipeline.delivery_lag, 0);
}

#[tokio::test]
async fn quarantine_change_persists_active_poison_record() {
    let store = open_migrated_store().await;
    store
        .upsert_deployment(&sample_deployment("intent-quarantine"))
        .await
        .expect("upsert Deployment");
    store
        .replace_pipelines(
            "intent-quarantine",
            &[sample_pipeline("intent-quarantine", "customers")],
        )
        .await
        .expect("replace Pipelines");

    let record = QuarantinedChange {
        deployment_name: "intent-quarantine".to_string(),
        pipeline_name: "customers".to_string(),
        source_schema: "APP".to_string(),
        source_table: "CUSTOMERS".to_string(),
        change_id: "poison-1".to_string(),
        capture_position: 55,
        output_identity: serde_json::json!({"ID": 9}),
        stage: "delivery".to_string(),
        attempts: 3,
        last_error: "target rejected".to_string(),
        status: "quarantined".to_string(),
    };
    store
        .quarantine_change(&record)
        .await
        .expect("quarantine change");

    let listed = store
        .list_quarantined_changes(Some("intent-quarantine"))
        .await
        .expect("list quarantine");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].change_id, "poison-1");
    assert_eq!(listed[0].status, "quarantined");
    assert_eq!(
        store
            .count_active_quarantines("intent-quarantine", "customers")
            .await
            .expect("count"),
        1
    );
}

#[tokio::test]
async fn mark_schema_impact_pauses_pipeline_and_persists_blocking_impact() {
    let store = open_migrated_store().await;
    store
        .upsert_deployment(&sample_deployment("intent-schema"))
        .await
        .expect("upsert Deployment");
    store
        .replace_pipelines(
            "intent-schema",
            &[sample_pipeline("intent-schema", "customers")],
        )
        .await
        .expect("replace Pipelines");

    let record = SchemaChangeImpact {
        deployment_name: "intent-schema".to_string(),
        pipeline_name: "customers".to_string(),
        source_schema: "APP".to_string(),
        source_table: "CUSTOMERS".to_string(),
        change_id: "ddl-1".to_string(),
        capture_position: 200,
        ddl_summary: "ALTER TABLE APP.CUSTOMERS DROP COLUMN X".to_string(),
        impact: "blocking".to_string(),
        status: "active".to_string(),
    };
    store
        .mark_schema_impact(&record)
        .await
        .expect("mark schema impact");

    let pipeline = store
        .list_pipelines()
        .await
        .expect("list")
        .into_iter()
        .find(|p| p.name == "customers")
        .expect("pipeline");
    assert!(
        pipeline.paused,
        "mark_schema_impact must pause the affected Pipeline"
    );

    let impacts = store
        .list_schema_change_impacts(Some("intent-schema"))
        .await
        .expect("list impacts");
    assert_eq!(impacts.len(), 1);
    assert_eq!(impacts[0].change_id, "ddl-1");
    assert_eq!(impacts[0].impact, "blocking");
    assert_eq!(impacts[0].status, "active");
}

#[tokio::test]
async fn record_source_alignment_progress_persists_alignment_and_base_rows() {
    let store = open_migrated_store().await;
    store
        .upsert_deployment(&sample_deployment("intent-align"))
        .await
        .expect("upsert Deployment");

    let mut seed = serde_json::Map::new();
    seed.insert("ID".to_string(), serde_json::json!(1));
    store
        .replace_base_dataset(&sample_base("intent-align"), &[seed])
        .await
        .expect("seed Base");

    let mut repaired = serde_json::Map::new();
    repaired.insert("ID".to_string(), serde_json::json!(2));
    let mut dataset = sample_base("intent-align");
    dataset.row_count = 1;
    dataset.source_alignment = "aligned".to_string();
    dataset.source_alignment_checked_rows = 1;
    dataset.source_alignment_mismatched_rows = 1;
    // Alignment repair clears Initial Load cursor; Sync checkpoints stay.
    dataset.initial_load_cursor = None;

    store
        .record_source_alignment_progress(&dataset, &[repaired])
        .await
        .expect("record Source Alignment progress");

    let (loaded, rows) = store
        .get_base_rows("CUSTOMERS", Some("intent-align"))
        .await
        .expect("load Base");
    assert_eq!(loaded.source_alignment, "aligned");
    assert_eq!(loaded.source_alignment_checked_rows, 1);
    assert_eq!(loaded.source_alignment_mismatched_rows, 1);
    assert_eq!(loaded.capture_checkpoint, Some(120));
    assert_eq!(loaded.capture_low_watermark, Some(100));
    assert_eq!(loaded.sync_applied_changes, 1);
    assert_eq!(loaded.sync_health, "ok");
    assert!(loaded.initial_load_cursor.is_none());
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].data.get("ID"), Some(&serde_json::json!(2)));
}

#[tokio::test]
async fn record_drift_outcome_persists_pipeline_drift_fields() {
    let store = open_migrated_store().await;
    store
        .upsert_deployment(&sample_deployment("intent-drift"))
        .await
        .expect("upsert Deployment");
    store
        .replace_pipelines(
            "intent-drift",
            &[sample_pipeline("intent-drift", "customers")],
        )
        .await
        .expect("replace Pipelines");

    store
        .record_drift_outcome("intent-drift", "customers", "ok", 5, 0)
        .await
        .expect("record Drift outcome");

    let pipeline = store
        .list_pipelines()
        .await
        .expect("list")
        .into_iter()
        .find(|p| p.name == "customers")
        .expect("pipeline");
    assert_eq!(pipeline.drift_status, "ok");
    assert_eq!(pipeline.drift_checked_rows, 5);
    assert_eq!(pipeline.drift_mismatched_rows, 0);

    let err = store
        .record_drift_outcome("intent-drift", "missing", "partial", 1, 1)
        .await
        .expect_err("missing Pipeline must NotFound");
    assert!(
        err.to_string().contains("not found"),
        "expected NotFound, got {err}"
    );
}

#[tokio::test]
async fn resume_pipeline_unpauses_and_clears_schema_impacts() {
    let store = open_migrated_store().await;
    store
        .upsert_deployment(&sample_deployment("intent-resume"))
        .await
        .expect("upsert Deployment");
    store
        .replace_pipelines(
            "intent-resume",
            &[sample_pipeline("intent-resume", "customers")],
        )
        .await
        .expect("replace Pipelines");

    store
        .mark_schema_impact(&SchemaChangeImpact {
            deployment_name: "intent-resume".to_string(),
            pipeline_name: "customers".to_string(),
            source_schema: "APP".to_string(),
            source_table: "CUSTOMERS".to_string(),
            change_id: "ddl-resume".to_string(),
            capture_position: 300,
            ddl_summary: "ALTER TABLE APP.CUSTOMERS ADD (Y NUMBER)".to_string(),
            impact: "blocking".to_string(),
            status: "active".to_string(),
        })
        .await
        .expect("mark schema impact");

    store
        .resume_pipeline("intent-resume", "customers")
        .await
        .expect("resume Pipeline persistence");

    let pipeline = store
        .list_pipelines()
        .await
        .expect("list")
        .into_iter()
        .find(|p| p.name == "customers")
        .expect("pipeline");
    assert!(
        !pipeline.paused,
        "resume_pipeline must clear durable pause"
    );

    let active = store
        .list_schema_change_impacts(Some("intent-resume"))
        .await
        .expect("list active impacts");
    assert!(
        active.is_empty(),
        "resume_pipeline must clear active schema impacts"
    );
}

#[tokio::test]
async fn remove_pipeline_deletes_pipeline_and_prunes_unreferenced_bases() {
    let store = open_migrated_store().await;
    store
        .upsert_deployment(&sample_deployment("intent-remove"))
        .await
        .expect("upsert Deployment");

    let customers = sample_pipeline("intent-remove", "customers");
    let mut orders = sample_pipeline("intent-remove", "orders");
    orders.source_table = "ORDERS".to_string();
    orders.target_collection = "orders".to_string();
    store
        .replace_pipelines("intent-remove", &[customers, orders])
        .await
        .expect("replace Pipelines");

    let customers_base = sample_base("intent-remove");
    let mut orders_base = sample_base("intent-remove");
    orders_base.source_table = "ORDERS".to_string();
    let mut crow = serde_json::Map::new();
    crow.insert("ID".to_string(), serde_json::json!(1));
    let mut orow = serde_json::Map::new();
    orow.insert("ID".to_string(), serde_json::json!(10));
    store
        .replace_base_dataset(&customers_base, &[crow])
        .await
        .expect("seed customers Base");
    store
        .replace_base_dataset(&orders_base, &[orow])
        .await
        .expect("seed orders Base");

    // Runtime supplies keep_tables from remaining Pipeline Base refs.
    store
        .remove_pipeline(
            "intent-remove",
            "orders",
            &[("APP".to_string(), "CUSTOMERS".to_string())],
        )
        .await
        .expect("remove Pipeline with Base cleanup");

    let pipelines = store.list_pipelines().await.expect("list pipelines");
    assert_eq!(pipelines.len(), 1);
    assert_eq!(pipelines[0].name, "customers");

    let bases = store.list_base_datasets().await.expect("list bases");
    assert_eq!(bases.len(), 1);
    assert_eq!(bases[0].source_table, "CUSTOMERS");
    assert_eq!(bases[0].source_schema, "APP");
}
