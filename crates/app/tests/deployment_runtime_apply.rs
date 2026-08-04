//! Deployment runtime seam (issue #152).
//!
//! Agreed seams (#149 Testing Decisions / #152 AC):
//! 1. Operator CLI contract-path twins remain the Release Quality Gate.
//! 2. In-process Deployment runtime interface for apply / Direct Delivery —
//!    not only binary spawn.
//!
//! This test hits `migraloop_runtime::apply` directly with a Platform Store
//! session, contract Source doubles, and a real Mongo Target.

mod common;

use std::process::Command;

use migraloop_capture::CONTRACT_SOURCE_CATALOG_ENV;
use migraloop_capture::INJECT_LOGMINER_CONTENTS_ENV;
use migraloop_platform_store::{
    Deployment, Pipeline, PlatformStore, SecretRef, SecretRefKind, SystemConnection, TlsSettings,
};
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
    let db_name = format!("migraloop_runtime_{suffix}");
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
    format!("runtime_appdb_{}", common::unique_suffix())
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

fn sample_direct_pipeline(deployment_name: &str) -> Pipeline {
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
        field_mappings: Default::default(),
        output_identity: vec![],
        transform_json: None,
        drift_status: "unknown".to_string(),
        drift_checked_rows: 0,
        drift_mismatched_rows: 0,
    }
}

fn mongo_documents(database: &str, collection: &str) -> String {
    let status = Command::new("python3")
        .args([
            "-c",
            &format!(
                r#"
from pymongo import MongoClient
import json
c = MongoClient(
    "mongodb://deliver_user:mongo-secret-value@{host}:{port}/{database}?authSource=admin",
    serverSelectionTimeoutMS=5000,
)
docs = list(c["{database}"]["{collection}"].find())
print(json.dumps(docs, default=str))
"#,
                host = mongo_host(),
                port = mongo_port(),
                database = database,
                collection = collection,
            ),
        ])
        .output()
        .expect("list mongo documents");
    assert!(
        status.status.success(),
        "mongo list failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    String::from_utf8_lossy(&status.stdout).to_string()
}

#[tokio::test]
async fn runtime_apply_runs_initial_load_and_direct_delivery_in_process() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());

    std::env::set_var("ORACLE_PASSWORD", "oracle-secret-value");
    std::env::set_var("MONGO_PASSWORD", "mongo-secret-value");
    std::env::set_var(CONTRACT_SOURCE_CATALOG_ENV, &doubles.catalog_path);
    std::env::set_var(INJECT_LOGMINER_CONTENTS_ENV, &doubles.logminer_path);

    let store = PlatformStore::open(&url)
        .await
        .expect("open Platform Store session");
    store.migrate().await.expect("migrate via session");

    let deployment = sample_deployment("runtime-apply", &mongo_database);
    let pipelines = vec![sample_direct_pipeline("runtime-apply")];

    migraloop_runtime::apply(&store, deployment, pipelines)
        .await
        .expect("runtime apply");

    assert!(
        store
            .base_dataset_exists("runtime-apply", "", "CUSTOMERS")
            .await
            .expect("exists"),
        "Base Dataset should exist after runtime Initial Load"
    );
    let (dataset, rows) = store
        .get_base_rows("CUSTOMERS", Some("runtime-apply"))
        .await
        .expect("load Base rows via session");
    assert_eq!(dataset.status, "initial_load_complete");
    assert!(
        rows.len() >= 2,
        "expected CUSTOMERS fixture rows in Base, got {}",
        rows.len()
    );

    let pipelines = store.list_pipelines().await.expect("list Pipelines");
    let customers = pipelines
        .iter()
        .find(|p| p.name == "customers")
        .expect("customers pipeline");
    assert_eq!(customers.delivery_status, "delivered");

    let docs = mongo_documents(&mongo_database, "customers");
    assert!(
        docs.contains("\"_id\": 1") || docs.contains("\"_id\":1"),
        "expected Output Identity _id=1 from source PK, got:\n{docs}"
    );
    assert!(
        docs.contains("Alice"),
        "expected Managed field from Base row, got:\n{docs}"
    );
}
