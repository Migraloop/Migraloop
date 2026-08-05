//! Typed Apply / Initial Load options seam (issue #200).
//!
//! Agreed seams (#199 Testing Decisions / #200 AC):
//! 1. Operator CLI / RQG contract-path twins remain the Release Quality Gate
//!    (typed ApplyOptions CLI flags are the primary path; env is a thin shim).
//! 2. Deployment runtime interface — in-process tests exercise Initial Load knobs
//!    via typed [`ApplyOptions`], not process env vars.

mod common;

use std::fs;

use migraloop_capture::CONTRACT_SOURCE_CATALOG_ENV;
use migraloop_platform_store::{
    Deployment, Pipeline, PlatformStore, SecretRef, SecretRefKind, SystemConnection, TlsSettings,
};
use migraloop_runtime::{ApplyOptions, InitialLoadOptions};
use serde_json::json;
use tempfile::TempDir;

fn admin_url() -> String {
    std::env::var("MIGRALOOP_TEST_ADMIN_URL")
        .unwrap_or_else(|_| "postgres://migraloop:migraloop@127.0.0.1:5432/postgres".to_string())
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

async fn ephemeral_database_url() -> String {
    let suffix = common::unique_suffix();
    let db_name = format!("migraloop_runtime_apply_opts_{suffix}");
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

fn widgets_catalog_json(row_count: usize) -> String {
    let mut rows = Vec::with_capacity(row_count);
    for i in 1..=row_count {
        rows.push(json!({
            "WID": i,
            "LABEL": format!("w{i}"),
        }));
    }
    serde_json::to_string_pretty(&json!({
        "tables": [{
            "table": "WIDGETS",
            "low_watermark": 9000,
            "primary_key": ["WID"],
            "columns": [
                {
                    "name": "WID",
                    "oracle_type": "NUMBER",
                    "supported": true,
                    "precision": 10,
                    "scale": 0
                },
                {
                    "name": "LABEL",
                    "oracle_type": "VARCHAR2",
                    "supported": true
                }
            ],
            "rows": rows
        }]
    }))
    .expect("serialize catalog")
}

fn sample_deployment(name: &str, mongo_database: &str) -> Deployment {
    Deployment {
        name: name.to_string(),
        source: SystemConnection {
            kind: "oracle".to_string(),
            host: "contract".to_string(),
            port: 1521,
            database: "ORCL".to_string(),
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
            port: i32::from(mongo_port()),
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

fn widgets_pipeline(deployment_name: &str) -> Pipeline {
    Pipeline {
        deployment_name: deployment_name.to_string(),
        name: "widgets".to_string(),
        mode: "direct".to_string(),
        source_table: "WIDGETS".to_string(),
        source_schema: String::new(),
        target_collection: "widgets".to_string(),
        delivery_status: "pending".to_string(),
        delivery_applied_changes: 0,
        delivery_lag: 0,
        paused: false,
        description: String::new(),
        field_mappings: Default::default(),
        output_identity: vec![],
        transform_json: None,
        drift_status: "unknown".to_string(),
        drift_checked_rows: 0,
        drift_mismatched_rows: 0,
    }
}

/// Pause inject via typed ApplyOptions — no Initial Load env knobs (issue #200).
#[tokio::test]
async fn typed_apply_options_pause_initial_load_without_env() {
    // Leftover process env must not satisfy this test — typed options must.
    std::env::remove_var("MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE");
    std::env::remove_var("MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS");
    std::env::remove_var("MIGRALOOP_INITIAL_LOAD_ROWS_PER_SEC");
    std::env::remove_var("MIGRALOOP_INITIAL_LOAD_STORE_DELAY_MS");
    // Poison any leftover env so a regression that re-reads env would misbehave.
    std::env::set_var("MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE", "999");
    std::env::set_var("MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS", "99");

    let url = ephemeral_database_url().await;
    let mongo_database = format!("runtime_apply_opts_{}", common::unique_suffix());
    let dir = TempDir::new().expect("tempdir");
    let catalog_path = dir.path().join("widgets.json");
    fs::write(&catalog_path, widgets_catalog_json(200)).expect("write catalog");

    std::env::set_var("ORACLE_PASSWORD", "oracle-secret-value");
    std::env::set_var("MONGO_PASSWORD", "mongo-secret-value");
    std::env::set_var(CONTRACT_SOURCE_CATALOG_ENV, &catalog_path);
    std::env::set_var("MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING", "all");

    let store = PlatformStore::open(&url)
        .await
        .expect("open Platform Store session");
    store.migrate().await.expect("migrate via session");

    let options = ApplyOptions {
        initial_load: InitialLoadOptions {
            chunk_size: 40,
            rows_per_sec: None,
            pause_after_chunks: Some(2),
            store_delay_ms: None,
        },
    };

    migraloop_runtime::apply_with_options(
        &store,
        sample_deployment("typed-apply-opts", &mongo_database),
        vec![widgets_pipeline("typed-apply-opts")],
        options,
    )
    .await
    .expect("runtime apply_with_options");

    let (dataset, _) = store
        .get_base_rows("WIDGETS", Some("typed-apply-opts"))
        .await
        .expect("load Base rows");
    assert_eq!(
        dataset.status, "initial_load_paused",
        "typed pause_after_chunks=2 must pause after 2×40 rows"
    );
    assert_eq!(dataset.row_count, 80);
    assert_eq!(dataset.capture_low_watermark, Some(9000));

    std::env::remove_var("MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE");
    std::env::remove_var("MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS");
}
