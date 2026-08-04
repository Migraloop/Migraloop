//! Deployment runtime lifecycle / Align / Drift / status inventory seam (issue #155).
//!
//! Agreed seams (#149 Testing Decisions / #155 AC):
//! 1. Operator CLI contract-path twins remain the Release Quality Gate.
//! 2. In-process Deployment runtime interface for pause / resume / Align / Drift /
//!    status inventory — not only binary spawn.
//!
//! Change (Pipeline revision) is already exercised via `runtime::apply` (#152).

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
    let db_name = format!("migraloop_runtime_life_{suffix}");
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
    format!("runtime_life_appdb_{}", common::unique_suffix())
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

fn corrupt_mongo_managed_field(database: &str, collection: &str) {
    let status = Command::new("python3")
        .args([
            "-c",
            &format!(
                r#"
from pymongo import MongoClient
c = MongoClient(
    "mongodb://deliver_user:mongo-secret-value@{host}:{port}/{database}?authSource=admin",
    serverSelectionTimeoutMS=5000,
)
c["{database}"]["{collection}"].update_one({{"_id": 1}}, {{"$set": {{"NAME": "DRIFTED"}}}})
"#,
                host = mongo_host(),
                port = mongo_port(),
                database = database,
                collection = collection,
            ),
        ])
        .output()
        .expect("corrupt mongo managed field");
    assert!(
        status.status.success(),
        "mongo corrupt failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
}

fn mongo_name_for_id(database: &str, collection: &str, id: i64) -> String {
    let status = Command::new("python3")
        .args([
            "-c",
            &format!(
                r#"
from pymongo import MongoClient
c = MongoClient(
    "mongodb://deliver_user:mongo-secret-value@{host}:{port}/{database}?authSource=admin",
    serverSelectionTimeoutMS=5000,
)
doc = c["{database}"]["{collection}"].find_one({{"_id": {id}}})
print(doc.get("NAME", "") if doc else "")
"#,
                host = mongo_host(),
                port = mongo_port(),
                database = database,
                collection = collection,
                id = id,
            ),
        ])
        .output()
        .expect("read mongo NAME");
    assert!(
        status.status.success(),
        "mongo read failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    String::from_utf8_lossy(&status.stdout).trim().to_string()
}

#[tokio::test]
async fn runtime_lifecycle_pause_align_drift_resume_and_status_inventory() {
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

    let deployment_name = "runtime-life";
    let deployment = sample_deployment(deployment_name, &mongo_database);
    let pipelines = vec![sample_direct_pipeline(deployment_name)];

    migraloop_runtime::apply(&store, deployment, pipelines)
        .await
        .expect("runtime apply");

    migraloop_runtime::pause_pipeline(&store, "customers", Some(deployment_name))
        .await
        .expect("runtime pause");
    let paused = store
        .list_pipelines()
        .await
        .expect("list")
        .into_iter()
        .find(|p| p.name == "customers")
        .expect("customers");
    assert!(paused.paused, "Pipeline should be paused via runtime verb");

    migraloop_runtime::source_alignment_check(&store, Some("CUSTOMERS"), Some(deployment_name), 100)
        .await
        .expect("runtime Source Alignment Check");
    let (base, _) = store
        .get_base_rows("CUSTOMERS", Some(deployment_name))
        .await
        .expect("base rows");
    assert!(
        base.source_alignment == "aligned" || base.source_alignment == "partial",
        "alignment status should be set, got {}",
        base.source_alignment
    );
    assert!(
        base.source_alignment_checked_rows > 0,
        "alignment should check at least one row"
    );

    corrupt_mongo_managed_field(&mongo_database, "customers");
    assert_eq!(
        mongo_name_for_id(&mongo_database, "customers", 1),
        "DRIFTED"
    );

    migraloop_runtime::drift_check(&store, Some("customers"), Some(deployment_name), 100)
        .await
        .expect("runtime Drift Check");
    assert_eq!(
        mongo_name_for_id(&mongo_database, "customers", 1),
        "Alice",
        "Drift Check must auto-repair Managed fields"
    );
    let drifted = store
        .list_pipelines()
        .await
        .expect("list")
        .into_iter()
        .find(|p| p.name == "customers")
        .expect("customers");
    assert_ne!(drifted.drift_status, "unknown");
    assert!(drifted.drift_checked_rows > 0);
    assert!(drifted.drift_mismatched_rows >= 1);

    migraloop_runtime::resume_pipeline(&store, "customers", Some(deployment_name))
        .await
        .expect("runtime resume");
    let resumed = store
        .list_pipelines()
        .await
        .expect("list")
        .into_iter()
        .find(|p| p.name == "customers")
        .expect("customers");
    assert!(!resumed.paused, "Pipeline should be unpaused via runtime verb");

    let inventory = migraloop_runtime::status_inventory(&store)
        .await
        .expect("status inventory via runtime");
    assert!(
        inventory.guardrail_error.is_none(),
        "healthy store should pass guardrails"
    );
    assert!(
        inventory
            .deployments
            .iter()
            .any(|d| d.name == deployment_name),
        "inventory should include Deployment"
    );
    assert!(
        inventory.pipelines.iter().any(|p| p.name == "customers"),
        "inventory should include Pipeline"
    );
    assert!(
        inventory
            .bases
            .iter()
            .any(|b| b.source_table.eq_ignore_ascii_case("CUSTOMERS")),
        "inventory should include Base Dataset"
    );

    migraloop_runtime::remove_pipeline(&store, "customers", Some(deployment_name))
        .await
        .expect("runtime remove");
    let remaining = store.list_pipelines().await.expect("list after remove");
    assert!(
        remaining.iter().all(|p| p.name != "customers"),
        "removed Pipeline must not remain in inventory"
    );
    assert!(
        !store
            .base_dataset_exists(deployment_name, "", "CUSTOMERS")
            .await
            .expect("exists"),
        "unreferenced Base Dataset should be pruned on remove"
    );
}
