//! Operator-visible seam: Rich Transform union multi-Base (issue #130).
//!
//! Agreed seams: CLI config apply → Initial Load of primary + union.from Bases;
//! Derived concatenation + Mongo Delivery by Output Identity; Affect Analysis on
//! both contributing Bases; free-form `$unionWith` extensions and scripts fail apply.

mod common;

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use migraloop_capture::{
    CapturePosition, ContractSourceCatalog, InitialLoadSnapshot, LogMinerContent,
    LogMinerOperation, SourceColumn, CONTRACT_SOURCE_CATALOG_ENV, INJECT_LOGMINER_CONTENTS_ENV,
};
use serde_json::{json, Value};
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

fn unique_mongo_database() -> String {
    let suffix = common::unique_suffix();
    format!("appdb_{suffix}")
}

fn json_num(n: i64) -> Value {
    json!(n)
}

fn json_str(s: &str) -> Value {
    json!(s)
}

fn row(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

fn number_col(name: &str, precision: i32, scale: i32) -> SourceColumn {
    SourceColumn {
        name: name.to_string(),
        data_type: "NUMBER".to_string(),
        supported: true,
        precision: Some(precision),
        scale: Some(scale),
        size: None,
    }
}

fn varchar_col(name: &str) -> SourceColumn {
    SourceColumn {
        name: name.to_string(),
        data_type: "VARCHAR2".to_string(),
        supported: true,
        precision: None,
        scale: None,
        size: None,
    }
}

fn west_customers_fixture() -> InitialLoadSnapshot {
    InitialLoadSnapshot {
        table: "WEST_CUSTOMERS".to_string(),
        low_watermark: CapturePosition(4000),
        primary_key: vec!["ID".to_string()],
        columns: vec![
            number_col("ID", 10, 0),
            varchar_col("NAME"),
            varchar_col("EMAIL"),
            number_col("ACTIVE", 1, 0),
            varchar_col("BIO"),
        ],
        rows: vec![
            row(&[
                ("ID", json_num(10)),
                ("NAME", json_str("Zoe")),
                ("EMAIL", json_str("zoe@example.com")),
                ("ACTIVE", json_num(1)),
                ("BIO", json_str("blob-bytes-zoe")),
            ]),
            row(&[
                ("ID", json_num(11)),
                ("NAME", json_str("Wade")),
                ("EMAIL", json_str("wade@example.com")),
                ("ACTIVE", json_num(1)),
                ("BIO", json_str("blob-bytes-wade")),
            ]),
        ],
    }
}

fn union_logminer_contents() -> Vec<LogMinerContent> {
    let mut contents = migraloop_capture::named_scenario_logminer_contents();
    // EMAIL-only primary update before stock CUSTOMERS batch (unused after project).
    contents.insert(
        0,
        LogMinerContent {
            scn: 1040,
            operation: LogMinerOperation::Update,
            seg_owner: "APP".to_string(),
            table_name: "CUSTOMERS".to_string(),
            identity: row(&[("ID", json_num(1))]),
            after_image: Some(row(&[
                ("ID", json_num(1)),
                ("NAME", json_str("Alice")),
                ("EMAIL", json_str("only-email@example.com")),
                ("ACTIVE", json_num(1)),
                ("BIO", json_str("blob-bytes-alice")),
            ])),
            rs_id: String::new(),
            ssn: 0,
        },
    );
    contents.extend([
        LogMinerContent {
            scn: 4010,
            operation: LogMinerOperation::Update,
            seg_owner: "APP".to_string(),
            table_name: "WEST_CUSTOMERS".to_string(),
            identity: row(&[("ID", json_num(10))]),
            after_image: Some(row(&[
                ("ID", json_num(10)),
                ("NAME", json_str("Zoe")),
                ("EMAIL", json_str("zoe-only@example.com")),
                ("ACTIVE", json_num(1)),
                ("BIO", json_str("blob-bytes-zoe")),
            ])),
            rs_id: String::new(),
            ssn: 0,
        },
        LogMinerContent {
            scn: 4020,
            operation: LogMinerOperation::Update,
            seg_owner: "APP".to_string(),
            table_name: "WEST_CUSTOMERS".to_string(),
            identity: row(&[("ID", json_num(10))]),
            after_image: Some(row(&[
                ("ID", json_num(10)),
                ("NAME", json_str("Zora")),
                ("EMAIL", json_str("zoe@example.com")),
                ("ACTIVE", json_num(1)),
                ("BIO", json_str("blob-bytes-zoe")),
            ])),
            rs_id: String::new(),
            ssn: 0,
        },
    ]);
    contents
}

struct UnionDoubles {
    catalog_path: PathBuf,
    logminer_path: PathBuf,
}

impl UnionDoubles {
    fn install(dir: &Path) -> Self {
        let catalog_path = dir.join(format!("union_catalog_{}.json", common::unique_suffix()));
        let logminer_path = dir.join(format!("union_logminer_{}.json", common::unique_suffix()));

        let mut catalog = ContractSourceCatalog::with_default_fixtures();
        catalog.insert(west_customers_fixture());
        fs::write(
            &catalog_path,
            serde_json::to_string_pretty(&catalog.to_file()).expect("serialize catalog"),
        )
        .expect("write catalog");

        let inject = json!({ "contents": union_logminer_contents() });
        fs::write(
            &logminer_path,
            serde_json::to_string_pretty(&inject).expect("serialize logminer"),
        )
        .expect("write logminer");

        Self {
            catalog_path,
            logminer_path,
        }
    }

    fn apply_env<'a>(&self, cmd: &'a mut Command) -> &'a mut Command {
        cmd.env(CONTRACT_SOURCE_CATALOG_ENV, &self.catalog_path)
            .env(INJECT_LOGMINER_CONTENTS_ENV, &self.logminer_path)
    }
}

fn deployment_shell(mongo_database: &str, pipeline_yaml: &str) -> String {
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
{pipeline_yaml}
"#,
        host = mongo_host(),
        port = mongo_port(),
    )
}

fn union_pipeline() -> &'static str {
    r#"
    - name: all-customers
      mode: transform
      source:
        table: CUSTOMERS
      target:
        collection: all_customers
      outputIdentity: [ID]
      transform:
        - $unionWith: WEST_CUSTOMERS
        - $project:
            ID: 1
            NAME: 1
"#
}

fn migrate_and_apply(url: &str, config: &Path, doubles: &UnionDoubles) -> String {
    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", url])
        .output()
        .expect("run migrate");
    assert!(
        migrate.status.success(),
        "migrate failed: {}",
        String::from_utf8_lossy(&migrate.stderr)
    );

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
    let mut out = String::from_utf8_lossy(&apply.stdout).into_owned();
    out.push_str(&String::from_utf8_lossy(&apply.stderr));
    out
}

fn apply_expect_failure(url: &str, config: &Path, doubles: &UnionDoubles) -> String {
    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", url])
        .output()
        .expect("run migrate");
    assert!(
        migrate.status.success(),
        "migrate failed: {}",
        String::from_utf8_lossy(&migrate.stderr)
    );

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
        !apply.status.success(),
        "apply should fail, but succeeded: stdout={}",
        String::from_utf8_lossy(&apply.stdout)
    );
    let mut combined = String::from_utf8_lossy(&apply.stderr).into_owned();
    combined.push_str(&String::from_utf8_lossy(&apply.stdout));
    combined
}

fn run_sync_fail_after(url: &str, after: u32, doubles: &UnionDoubles) -> String {
    let mut cmd = Command::new(bin());
    cmd.env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value");
    doubles.apply_env(&mut cmd);
    cmd.args(["sync", "--platform-store-url", url]);
    common::SyncCliOptions::fail_after(after).append_to(&mut cmd);
    let out = cmd.output().expect("run sync with fail-after");
    let mut text = String::from_utf8_lossy(&out.stdout).into_owned();
    text.push_str(&String::from_utf8_lossy(&out.stderr));
    text
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

#[tokio::test]
async fn union_materializes_multi_base_derived_and_delivers() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = UnionDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_shell(&mongo_database, union_pipeline()),
    );

    let apply_out = migrate_and_apply(&url, &config, &doubles);
    assert!(
        apply_out.contains("Initial Load complete: Base Dataset CUSTOMERS")
            && apply_out.contains("Initial Load complete: Base Dataset WEST_CUSTOMERS"),
        "union must Initial Load both Bases, got:\n{apply_out}"
    );

    let customers_base = base_stdout(&url, "CUSTOMERS");
    let west_base = base_stdout(&url, "WEST_CUSTOMERS");
    assert!(
        customers_base.contains("Alice") && west_base.contains("Zoe"),
        "both Bases must be materialized, customers=\n{customers_base}\nwest=\n{west_base}"
    );

    let derived = derived_stdout(&url, "all-customers");
    assert!(
        derived.contains("Alice") && derived.contains("Zoe") && derived.contains("Wade"),
        "Derived must concatenate both Base sides, got:\n{derived}"
    );
    assert!(
        !derived.contains("alice@example.com")
            && !derived.contains("zoe@example.com")
            && !derived.contains("\"EMAIL\""),
        "project after union must drop EMAIL, got:\n{derived}"
    );

    let target = target_stdout(&url, "all_customers");
    assert!(
        (target.contains("\"_id\": 1") || target.contains("\"_id\":1"))
            && (target.contains("\"_id\": 10") || target.contains("\"_id\":10"))
            && target.contains("Alice")
            && target.contains("Zoe"),
        "Mongo Delivery must upsert by Output Identity for both sides, got:\n{target}"
    );
}

#[tokio::test]
async fn union_affect_analysis_updates_on_either_base_side() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = UnionDoubles::install(dir.path());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &deployment_shell(&mongo_database, union_pipeline()),
    );

    migrate_and_apply(&url, &config, &doubles);

    // CUSTOMERS first (BTreeSet). fail_after=1 → EMAIL-only skip.
    let unused_out = run_sync_fail_after(&url, 1, &doubles);
    assert!(
        unused_out.to_ascii_lowercase().contains("skip")
            || unused_out.to_ascii_lowercase().contains("unused"),
        "EMAIL-only primary update must Affect-Analysis skip, got:\n{unused_out}"
    );
    let derived_after_skip = derived_stdout(&url, "all-customers");
    assert!(
        derived_after_skip.contains("Alice") && !derived_after_skip.contains("only-email"),
        "unused EMAIL must not alter Derived, got:\n{derived_after_skip}"
    );

    // Next CUSTOMERS change: Alice→Alicia (NAME used) — recompute.
    let name_out = run_sync_fail_after(&url, 1, &doubles);
    let name_lower = name_out.to_ascii_lowercase();
    assert!(
        name_lower.contains("affect")
            && (name_lower.contains("recomput")
                || name_lower.contains("affected")
                || name_lower.contains("delivery complete")),
        "NAME update must recompute primary side, got:\n{name_out}"
    );
    let derived_name = derived_stdout(&url, "all-customers");
    assert!(
        derived_name.contains("Alicia"),
        "primary NAME change must update Derived, got:\n{derived_name}"
    );

    // Drain remaining CUSTOMERS changes (Carol insert, Bob delete) so WEST sync starts.
    let _ = run_sync_fail_after(&url, 2, &doubles);

    // WEST EMAIL-only (SCN 4010): unused after project → skip.
    let west_unused = run_sync_fail_after(&url, 1, &doubles);
    assert!(
        west_unused.to_ascii_lowercase().contains("skip")
            || west_unused.to_ascii_lowercase().contains("unused"),
        "EMAIL-only secondary update must Affect-Analysis skip, got:\n{west_unused}"
    );
    let derived_west_skip = derived_stdout(&url, "all-customers");
    assert!(
        derived_west_skip.contains("Zoe") && !derived_west_skip.contains("zoe-only"),
        "unused WEST EMAIL must not alter Derived, got:\n{derived_west_skip}"
    );

    // WEST NAME Zoe→Zora (SCN 4020).
    let west_name = run_sync_fail_after(&url, 1, &doubles);
    let west_lower = west_name.to_ascii_lowercase();
    assert!(
        west_lower.contains("affect")
            && (west_lower.contains("recomput")
                || west_lower.contains("affected")
                || west_lower.contains("delivery complete")),
        "secondary NAME change must Affect Analysis recompute, got:\n{west_name}"
    );
    let derived_west = derived_stdout(&url, "all-customers");
    assert!(
        derived_west.contains("Zora"),
        "secondary NAME must update Derived, got:\n{derived_west}"
    );
    assert!(
        !derived_west.contains("\"Zoe\"") && !derived_west.contains(": Zoe"),
        "stale Zoe must not remain, got:\n{derived_west}"
    );

    let target = target_stdout(&url, "all_customers");
    assert!(
        target.contains("Alicia") && target.contains("Zora"),
        "Mongo Delivery must reflect both Base sides, got:\n{target}"
    );
}

#[tokio::test]
async fn union_rejects_pipeline_union_with_and_scripts_on_apply() {
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let doubles = UnionDoubles::install(dir.path());

    // Issue #232: constrained Aggregation `$unionWith` is accepted; nested `pipeline` is not.
    let union_with_pipeline = r#"
    - name: bad-union-with
      mode: transform
      source:
        table: CUSTOMERS
      target:
        collection: bad_union_with
      outputIdentity: [ID]
      transform:
        - $unionWith:
            coll: WEST_CUSTOMERS
            pipeline: []
"#;
    let union_with_config = write_config(
        &dir,
        "union_with.yaml",
        &deployment_shell(&mongo_database, union_with_pipeline),
    );
    let err = apply_expect_failure(&url, &union_with_config, &doubles);
    let lower = err.to_ascii_lowercase();
    assert!(
        lower.contains("pipeline"),
        "expected clear rejection of $unionWith pipeline extension, got:\n{err}"
    );

    let url2 = ephemeral_database_url().await;
    let mongo2 = unique_mongo_database();
    let dir2 = TempDir::new().expect("tempdir");
    let doubles2 = UnionDoubles::install(dir2.path());
    let script_pipeline = r#"
    - name: scripted
      mode: transform
      source:
        table: CUSTOMERS
      target:
        collection: scripted
      outputIdentity: [ID]
      transform:
        - $unionWith:
            from: WEST_CUSTOMERS
        - script: "return true"
"#;
    let script_config = write_config(
        &dir2,
        "script.yaml",
        &deployment_shell(&mongo2, script_pipeline),
    );
    let err = apply_expect_failure(&url2, &script_config, &doubles2);
    let lower = err.to_ascii_lowercase();
    assert!(
        lower.contains("script") || lower.contains("free-form"),
        "expected clear free-form script failure, got:\n{err}"
    );
}
