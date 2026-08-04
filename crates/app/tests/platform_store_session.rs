//! Platform Store session seam (issue #151).
//!
//! Agreed seam (#149 Testing Decisions): exercise session verbs for apply-path
//! persistence (Deployment / Pipeline / Base Dataset) rather than URL-per-call CRUD.
//! Operator CLI twins remain the Release Quality Gate; this test covers the store
//! session interface that apply (and later Deployment runtime) should reuse.

mod common;

use migraloop_platform_store::{
    BaseColumn, BaseDataset, Deployment, Pipeline, PlatformStore, PlatformStoreHealth, SecretRef,
    SecretRefKind, SystemConnection, TlsSettings,
};

fn admin_url() -> String {
    std::env::var("MIGRALOOP_TEST_ADMIN_URL").unwrap_or_else(|_| {
        "postgres://migraloop:migraloop@127.0.0.1:5432/postgres".to_string()
    })
}

async fn ephemeral_database_url() -> String {
    let suffix = common::unique_suffix();
    let db_name = format!("migraloop_session_{suffix}");
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

#[tokio::test]
async fn platform_store_session_reuses_one_open_for_apply_persistence_verbs() {
    let url = ephemeral_database_url().await;
    let store = PlatformStore::open(&url)
        .await
        .expect("open Platform Store session");

    store.migrate().await.expect("migrate via session");
    match store.health().await {
        PlatformStoreHealth::Healthy { schema_version } => {
            assert!(schema_version > 0, "migrated schema version");
        }
        other => panic!("expected Healthy after migrate, got {other:?}"),
    }

    // Guardrails probe uses the same session pool (ADR-0010 stays on store module).
    let settings = store
        .probe_settings()
        .await
        .expect("probe settings via session");
    migraloop_platform_store::check_store_settings(&settings)
        .expect("default test Postgres meets Platform Store Guardrails");

    let deployment = sample_deployment("session-apply");
    store
        .upsert_deployment(&deployment)
        .await
        .expect("upsert Deployment via session");

    let pipeline = sample_pipeline("session-apply", "customers");
    store
        .replace_pipelines("session-apply", &[pipeline.clone()])
        .await
        .expect("replace Pipelines via session");

    let dataset = BaseDataset {
        deployment_name: "session-apply".to_string(),
        source_table: "CUSTOMERS".to_string(),
        source_schema: "APP".to_string(),
        status: "initial_load_complete".to_string(),
        primary_key: vec!["ID".to_string()],
        columns: vec![BaseColumn {
            name: "ID".to_string(),
            data_type: "NUMBER".to_string(),
            precision: Some(10),
            scale: Some(0),
        }],
        omitted_columns: vec![],
        row_count: 1,
        sync_applied_changes: 0,
        sync_health: "unknown".to_string(),
        capture_low_watermark: None,
        capture_checkpoint: None,
        sync_lag: 0,
        source_alignment: "unknown".to_string(),
        source_alignment_checked_rows: 0,
        source_alignment_mismatched_rows: 0,
        initial_load_cursor: None,
    };
    let mut row = serde_json::Map::new();
    row.insert("ID".to_string(), serde_json::json!(1));
    store
        .append_base_dataset_chunk(&dataset, &[row], 0)
        .await
        .expect("append Base Dataset chunk via session");

    assert!(
        store
            .base_dataset_exists("session-apply", "APP", "CUSTOMERS")
            .await
            .expect("exists"),
        "Base Dataset should exist after chunk append"
    );

    let deployments = store.list_deployments().await.expect("list Deployments");
    assert_eq!(deployments.len(), 1);
    assert_eq!(deployments[0].name, "session-apply");
    assert_eq!(deployments[0].source.password_ref.value, "ORACLE_PASSWORD");

    let pipelines = store.list_pipelines().await.expect("list Pipelines");
    assert_eq!(pipelines.len(), 1);
    assert_eq!(pipelines[0].name, "customers");
    assert_eq!(pipelines[0].source_table, "CUSTOMERS");

    let (loaded, rows) = store
        .get_base_rows("CUSTOMERS", Some("session-apply"))
        .await
        .expect("load Base rows via session");
    assert_eq!(loaded.row_count, 1);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].data.get("ID"), Some(&serde_json::json!(1)));

    store
        .set_pipeline_paused("session-apply", "customers", true)
        .await
        .expect("pause Pipeline via session");
    let paused = store
        .list_pipelines()
        .await
        .expect("relist")
        .into_iter()
        .find(|p| p.name == "customers")
        .expect("pipeline");
    assert!(paused.paused);
}
