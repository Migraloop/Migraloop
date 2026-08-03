//! Operator-visible seam: change Pipeline as a new revision (issue #21 / ADR-0007).
//!
//! Agreed seam: CLI `apply` (declarative revision) / `sync` / `status` / `base` /
//! `derived` / `target`. Semantic transform/binding changes pause old Delivery,
//! rebuild that Pipeline's Derived and re-Deliver (with delete reconciliation),
//! then continue incremental; shared Bases are not rebuilt. Metadata-only
//! `description` changes skip rebuild.
//!
//! This is the non-ignored contract/stub CI twin of Lab Scenario `change-pipeline`.
//! It must not run Lab Fixture / live Oracle.

mod common;

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

fn admin_url() -> String {
    std::env::var("MIGRALOOP_TEST_ADMIN_URL").unwrap_or_else(|_| {
        "postgres://migraloop:migraloop@127.0.0.1:5432/postgres".to_string()
    })
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

fn bin() -> String {
    env!("CARGO_BIN_EXE_migraloop").to_string()
}

fn unique_mongo_database() -> String {
    let suffix = common::unique_suffix();
    format!("appdb_{suffix}")
}

async fn ephemeral_database_url() -> String {
    let suffix = common::unique_suffix();
    let db_name = format!("migraloop_test_{suffix}");
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

fn write_config(dir: &TempDir, name: &str, contents: &str) -> PathBuf {
    let path = dir.path().join(name);
    fs::write(&path, contents).expect("write config");
    path
}

fn deployment_with_pipelines(mongo_database: &str, pipelines_yaml: &str) -> String {
    format!(
        r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: oracle-to-mongo
spec:
  source:
    kind: oracle
    host: stub
    port: 1521
    database: STUB
    username: sync_user
    password:
      fromEnv: ORACLE_PASSWORD
  target:
    kind: mongodb
    host: {host}
    port: {port}
    database: {mongo_database}
    username: deliver_user
    password:
      fromEnv: MONGO_PASSWORD
  pipelines:
{pipelines_yaml}
"#,
        host = mongo_host(),
        port = mongo_port(),
    )
}

fn active_customers_and_reporting(
    active_eq: i32,
    description: &str,
    reporting_collection: &str,
) -> String {
    format!(
        r#"    - name: active_customers
      mode: transform
      description: {description}
      source:
        table: CUSTOMERS
      target:
        collection: active_customers
      outputIdentity: [ID]
      transform:
        - project:
            fields: [ID, NAME, ACTIVE]
        - filter:
            field: ACTIVE
            eq: {active_eq}
    - name: customers_reporting
      mode: direct
      source:
        table: CUSTOMERS
      target:
        collection: {reporting_collection}
"#
    )
}

fn migrate(url: &str) {
    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", url])
        .output()
        .expect("run migrate");
    assert!(
        migrate.status.success(),
        "migrate failed: {}",
        String::from_utf8_lossy(&migrate.stderr)
    );
}

fn apply(url: &str, config: &Path, doubles: &common::NamedScenarioDoubles) -> String {
    let mut apply = Command::new(bin());
    apply
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value");
    doubles.apply_env(&mut apply);
    let apply = apply
        .args([
            "apply",
            "--platform-store-url",
            url,
            "--file",
            config.to_str().unwrap(),
        ])
        .output()
        .expect("run apply");

    assert!(
        apply.status.success(),
        "apply failed: stdout={} stderr={}",
        String::from_utf8_lossy(&apply.stdout),
        String::from_utf8_lossy(&apply.stderr)
    );
    String::from_utf8_lossy(&apply.stdout).into_owned()
}

fn run_sync(url: &str, doubles: &common::NamedScenarioDoubles) -> String {
    let mut sync = Command::new(bin());
    sync
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value");
    doubles.apply_env(&mut sync);
    let sync = sync
        .args(["sync", "--platform-store-url", url])
        .output()
        .expect("run sync");

    assert!(
        sync.status.success(),
        "sync failed: stdout={} stderr={}",
        String::from_utf8_lossy(&sync.stdout),
        String::from_utf8_lossy(&sync.stderr)
    );
    String::from_utf8_lossy(&sync.stdout).into_owned()
}

fn target_stdout(url: &str, collection: &str) -> String {
    let target = Command::new(bin())
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "target",
            "--platform-store-url",
            url,
            "--collection",
            collection,
        ])
        .output()
        .expect("run target");
    assert!(
        target.status.success(),
        "target inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&target.stdout),
        String::from_utf8_lossy(&target.stderr)
    );
    String::from_utf8_lossy(&target.stdout).into_owned()
}

fn derived_stdout(url: &str, pipeline: &str) -> String {
    let derived = Command::new(bin())
        .args([
            "derived",
            "--platform-store-url",
            url,
            "--pipeline",
            pipeline,
        ])
        .output()
        .expect("run derived");
    assert!(
        derived.status.success(),
        "derived inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&derived.stdout),
        String::from_utf8_lossy(&derived.stderr)
    );
    String::from_utf8_lossy(&derived.stdout).into_owned()
}

fn base_stdout(url: &str, table: &str) -> String {
    let base = Command::new(bin())
        .args(["base", "--platform-store-url", url, "--table", table])
        .output()
        .expect("run base");
    assert!(
        base.status.success(),
        "base inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&base.stdout),
        String::from_utf8_lossy(&base.stderr)
    );
    String::from_utf8_lossy(&base.stdout).into_owned()
}

fn status(url: &str) -> String {
    let status = Command::new(bin())
        .args(["status", "--platform-store-url", url])
        .output()
        .expect("run status");
    assert!(
        status.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    String::from_utf8_lossy(&status.stdout).into_owned()
}

#[tokio::test]
async fn semantic_transform_change_rebuilds_derived_re_delivers_without_base_reload() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());

    let v1 = write_config(
        &dir,
        "v1.yaml",
        &deployment_with_pipelines(
            &mongo_database,
            &active_customers_and_reporting(1, "active", "customers_reporting"),
        ),
    );
    migrate(&url);
    let first = apply(&url, &v1, &doubles);
    assert!(
        first.contains("Derived Dataset materialized: Pipeline active_customers")
            || first.contains("Delivery complete: Pipeline active_customers"),
        "first apply must Deliver Transform Pipeline, got:\n{first}"
    );

    let target_v1 = target_stdout(&url, "active_customers");
    assert!(
        target_v1.contains("Alice") && target_v1.contains("Carol") && !target_v1.contains("Bob"),
        "ACTIVE==1 revision must Deliver Alice/Carol only, got:\n{target_v1}"
    );

    let base_before = base_stdout(&url, "CUSTOMERS");
    assert!(
        base_before.contains("Alice")
            && base_before.contains("Bob")
            && base_before.contains("Carol"),
        "Shared Base must hold full Initial Load before revision, got:\n{base_before}"
    );

    let v2 = write_config(
        &dir,
        "v2.yaml",
        &deployment_with_pipelines(
            &mongo_database,
            &active_customers_and_reporting(0, "active", "customers_reporting"),
        ),
    );
    let revision = apply(&url, &v2, &doubles);
    let revision_lower = revision.to_ascii_lowercase();
    assert!(
        revision_lower.contains("revision") && revision.contains("active_customers"),
        "semantic change must report Pipeline revision for active_customers, got:\n{revision}"
    );
    assert!(
        revision_lower.contains("paused") || revision_lower.contains("pause"),
        "revision transition must pause old Delivery, got:\n{revision}"
    );
    assert!(
        revision.contains("Derived Dataset materialized: Pipeline active_customers"),
        "semantic transform change must rebuild Derived, got:\n{revision}"
    );
    assert!(
        !revision.contains("Initial Load complete: Base Dataset CUSTOMERS"),
        "shared Base must not be rebuilt on Pipeline revision, got:\n{revision}"
    );
    assert!(
        !revision.contains("Delivery complete: Pipeline customers_reporting")
            || revision.contains("Runtime Pipeline add"),
        "unchanged sibling Pipeline must not be re-Delivered, got:\n{revision}"
    );

    let derived = derived_stdout(&url, "active_customers");
    assert!(
        derived.contains("Bob") && !derived.contains("Alice") && !derived.contains("Carol"),
        "rebuilt Derived must match ACTIVE==0 filter, got:\n{derived}"
    );

    let target_v2 = target_stdout(&url, "active_customers");
    assert!(
        target_v2.contains("Bob") && !target_v2.contains("Alice") && !target_v2.contains("Carol"),
        "re-Delivery must upsert new identity and reconcile deletes for old identities, got:\n{target_v2}"
    );

    let base_after = base_stdout(&url, "CUSTOMERS");
    assert!(
        base_after.contains("Alice") && base_after.contains("Bob") && base_after.contains("Carol"),
        "Shared Base rows must remain after Pipeline revision, got:\n{base_after}"
    );

    let reporting = target_stdout(&url, "customers_reporting");
    assert!(
        reporting.contains("Alice") && reporting.contains("Bob") && reporting.contains("Carol"),
        "sibling Direct Pipeline Target must remain from Shared Base, got:\n{reporting}"
    );

    let status_out = status(&url);
    assert!(
        status_out.contains("active_customers")
            && !status_out
                .to_ascii_lowercase()
                .lines()
                .any(|line| line.contains("active_customers") && line.contains("paused")),
        "revision must resume incremental (not leave Pipeline paused), got:\n{status_out}"
    );

    // Incremental continues under the new revision. Stub CDC deletes Bob and updates
    // Alice→Alicia (ACTIVE==1); the ACTIVE==0 transform must not Deliver Alicia, and
    // Bob's Source delete must clear the previous revision's Target identity.
    let sync_out = run_sync(&url, &doubles);
    assert!(
        !sync_out.to_ascii_lowercase().contains("error"),
        "incremental sync after revision must succeed, got:\n{sync_out}"
    );
    let target_after_sync = target_stdout(&url, "active_customers");
    assert!(
        !target_after_sync.contains("Alice")
            && !target_after_sync.contains("Alicia")
            && !target_after_sync.contains("Carol")
            && !target_after_sync.contains("Bob"),
        "incremental under ACTIVE==0 revision must not Deliver ACTIVE==1 rows and must \
         apply Bob's Source delete, got:\n{target_after_sync}"
    );
}

#[tokio::test]
async fn metadata_only_description_change_skips_derived_rebuild() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());

    let v1 = write_config(
        &dir,
        "v1.yaml",
        &deployment_with_pipelines(
            &mongo_database,
            &active_customers_and_reporting(1, "initial comment", "customers_reporting"),
        ),
    );
    migrate(&url);
    apply(&url, &v1, &doubles);

    let target_before = target_stdout(&url, "active_customers");
    assert!(
        target_before.contains("Alice") && target_before.contains("Carol"),
        "baseline Target must be ACTIVE==1 Delivery, got:\n{target_before}"
    );

    let v2 = write_config(
        &dir,
        "v2.yaml",
        &deployment_with_pipelines(
            &mongo_database,
            &active_customers_and_reporting(1, "renamed comment", "customers_reporting"),
        ),
    );
    let revision = apply(&url, &v2, &doubles);
    let revision_lower = revision.to_ascii_lowercase();
    assert!(
        revision_lower.contains("metadata")
            && revision_lower.contains("skip")
            && revision.contains("active_customers"),
        "metadata-only change must report skip for active_customers, got:\n{revision}"
    );
    assert!(
        !revision.contains("Derived Dataset materialized: Pipeline active_customers"),
        "metadata-only change must not rebuild Derived, got:\n{revision}"
    );
    assert!(
        !revision.contains("Delivery complete: Pipeline active_customers"),
        "metadata-only change must not re-Deliver, got:\n{revision}"
    );
    assert!(
        !revision.contains("Initial Load complete: Base Dataset CUSTOMERS"),
        "metadata-only change must not rebuild Shared Base, got:\n{revision}"
    );

    let target_after = target_stdout(&url, "active_customers");
    assert_eq!(
        target_before, target_after,
        "metadata-only change must leave Target Managed documents unchanged"
    );
}

#[tokio::test]
async fn semantic_binding_change_re_delivers_to_new_collection_without_base_reload() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = common::NamedScenarioDoubles::install(dir.path());

    let v1 = write_config(
        &dir,
        "v1.yaml",
        &deployment_with_pipelines(
            &mongo_database,
            &active_customers_and_reporting(1, "active", "customers_reporting"),
        ),
    );
    migrate(&url);
    apply(&url, &v1, &doubles);

    let reporting_v1 = target_stdout(&url, "customers_reporting");
    assert!(
        reporting_v1.contains("Alice") && reporting_v1.contains("Bob"),
        "baseline Direct Target must be Delivered, got:\n{reporting_v1}"
    );

    let v2 = write_config(
        &dir,
        "v2.yaml",
        &deployment_with_pipelines(
            &mongo_database,
            &active_customers_and_reporting(1, "active", "customers_reporting_v2"),
        ),
    );
    let revision = apply(&url, &v2, &doubles);
    let revision_lower = revision.to_ascii_lowercase();
    assert!(
        revision_lower.contains("revision") && revision.contains("customers_reporting"),
        "binding change must report Pipeline revision, got:\n{revision}"
    );
    assert!(
        revision.contains("Delivery complete: Pipeline customers_reporting"),
        "binding change must re-Deliver Direct Pipeline, got:\n{revision}"
    );
    assert!(
        !revision.contains("Initial Load complete: Base Dataset CUSTOMERS"),
        "binding change must not rebuild Shared Base, got:\n{revision}"
    );
    assert!(
        !revision.contains("Derived Dataset materialized: Pipeline active_customers"),
        "unchanged Transform sibling must not rebuild Derived, got:\n{revision}"
    );

    let reporting_v2 = target_stdout(&url, "customers_reporting_v2");
    assert!(
        reporting_v2.contains("Alice")
            && reporting_v2.contains("Bob")
            && reporting_v2.contains("Carol"),
        "new Target Binding must receive re-Delivery from Shared Base, got:\n{reporting_v2}"
    );

    let transform_target = target_stdout(&url, "active_customers");
    assert!(
        transform_target.contains("Alice") && transform_target.contains("Carol"),
        "unchanged Transform Target must remain, got:\n{transform_target}"
    );
}
