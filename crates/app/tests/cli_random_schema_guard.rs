//! Operator-seam guard (issue #41 / PRD #39 / ADR-0026): randomly generated Source
//! table + field names exercise the full product path —
//! schema discovery → Initial Load → Base Dataset → Delivery → Incremental Capture.
//!
//! Named scenario fixtures (CUSTOMERS/ORDERS/…) may stay elsewhere. This guard must
//! not be satisfied by the hard-coded stub/fixture catalog alone: the schema is
//! injected via `MIGRALOOP_CONTRACT_SOURCE_CATALOG` and Incremental changes via
//! `MIGRALOOP_INJECT_LOGMINER_CONTENTS` on `host: contract`.

mod common;

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::json;
use tempfile::TempDir;

/// Fixture / prior-proof table names that must not count as "random schema".
const FORBIDDEN_TABLES: &[&str] = &[
    "CUSTOMERS",
    "ORDERS",
    "EVENTS",
    "ACCOUNTS",
    "WIDGETS",
    "NOT_IN_CATALOG",
];

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

/// Oracle-safe identifier: letter start, A–Z/0–9/_, ≤30 chars, not a fixture name.
fn random_oracle_ident(prefix: &str, seed: &str) -> String {
    let cleaned: String = seed
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_uppercase())
        .collect();
    let mut name = format!("{prefix}{cleaned}");
    if name.len() > 30 {
        name.truncate(30);
    }
    // Ensure we never accidentally collide with named fixtures.
    if FORBIDDEN_TABLES
        .iter()
        .any(|f| name.eq_ignore_ascii_case(f))
    {
        name = format!("X{name}");
        if name.len() > 30 {
            name.truncate(30);
        }
    }
    name
}

struct RandomSchema {
    table: String,
    pk: String,
    label: String,
    note: String,
    /// Unsupported Source type column name (omitted from Base / Delivery Managed fields).
    unsupported_col: String,
    collection: String,
    pipeline: String,
    low_watermark: u64,
}

impl RandomSchema {
    fn generate() -> Self {
        let seed = common::unique_suffix();
        let table = random_oracle_ident("T", &seed);
        let pk = random_oracle_ident("K", &format!("{seed}a"));
        let label = random_oracle_ident("L", &format!("{seed}b"));
        let note = random_oracle_ident("N", &format!("{seed}c"));
        let unsupported_col = random_oracle_ident("B", &format!("{seed}d"));
        // Distinct identifiers — collision would mean the seed scrub collapsed names.
        let names = [&table, &pk, &label, &note, &unsupported_col];
        let unique: BTreeSet<_> = names.iter().map(|s| s.as_str()).collect();
        assert_eq!(
            unique.len(),
            names.len(),
            "random identifiers must be distinct: {names:?}"
        );
        for forbidden in FORBIDDEN_TABLES {
            assert!(
                !table.eq_ignore_ascii_case(forbidden),
                "table must not be a named fixture: {table}"
            );
        }
        let alnum: String = seed
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .collect::<String>()
            .to_ascii_lowercase();
        let collection = format!("c{}", alnum.chars().take(20).collect::<String>());
        let pipeline = format!("p{}", alnum.chars().take(12).collect::<String>());
        Self {
            table,
            pk,
            label,
            note,
            unsupported_col,
            collection,
            pipeline,
            low_watermark: 8_000,
        }
    }

    fn catalog_json(&self) -> String {
        serde_json::to_string_pretty(&json!({
            "tables": [{
                "table": self.table,
                "low_watermark": self.low_watermark,
                "primary_key": [self.pk],
                "columns": [
                    {
                        "name": self.pk,
                        "oracle_type": "NUMBER",
                        "supported": true,
                        "precision": 10,
                        "scale": 0
                    },
                    {
                        "name": self.label,
                        "oracle_type": "VARCHAR2",
                        "supported": true
                    },
                    {
                        "name": self.note,
                        "oracle_type": "VARCHAR2",
                        "supported": true
                    },
                    {
                        "name": self.unsupported_col,
                        "oracle_type": "BLOB",
                        "supported": false
                    }
                ],
                "rows": [
                    {
                        self.pk.clone(): 1,
                        self.label.clone(): "seed-one",
                        self.note.clone(): "note-a",
                        self.unsupported_col.clone(): "blob-bytes-one"
                    },
                    {
                        self.pk.clone(): 2,
                        self.label.clone(): "seed-two",
                        self.note.clone(): "note-b",
                        self.unsupported_col.clone(): "blob-bytes-two"
                    }
                ]
            }]
        }))
        .expect("catalog json")
    }

    fn logminer_inject_json(&self) -> String {
        let scn_update = self.low_watermark + 50;
        let scn_insert = self.low_watermark + 60;
        let scn_delete = self.low_watermark + 70;
        serde_json::to_string_pretty(&json!({
            "contents": [
                {
                    "scn": scn_update,
                    "operation": "UPDATE",
                    "seg_owner": "APP",
                    "table_name": self.table,
                    "identity": { self.pk.clone(): 1 },
                    "after_image": {
                        self.pk.clone(): 1,
                        self.label.clone(): "seed-one-updated",
                        self.note.clone(): "note-a-revised",
                        self.unsupported_col.clone(): "blob-bytes-one-rev"
                    }
                },
                {
                    "scn": scn_insert,
                    "operation": "INSERT",
                    "seg_owner": "APP",
                    "table_name": self.table,
                    "identity": { self.pk.clone(): 3 },
                    "after_image": {
                        self.pk.clone(): 3,
                        self.label.clone(): "seed-three",
                        self.note.clone(): "note-c",
                        self.unsupported_col.clone(): "blob-bytes-three"
                    }
                },
                {
                    "scn": scn_delete,
                    "operation": "DELETE",
                    "seg_owner": "APP",
                    "table_name": self.table,
                    "identity": { self.pk.clone(): 2 },
                    "after_image": null
                }
            ]
        }))
        .expect("inject json")
    }

    fn deployment_yaml(&self, mongo_database: &str) -> String {
        format!(
            r#"
apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: random-schema-guard
spec:
  source:
    kind: oracle
    host: contract
    port: 1521
    database: ORCL
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
    - name: {pipeline}
      mode: direct
      source:
        table: {table}
      target:
        collection: {collection}
"#,
            host = mongo_host(),
            port = mongo_port(),
            pipeline = self.pipeline,
            table = self.table,
            collection = self.collection,
        )
    }
}

fn migrate_and_apply(url: &str, config: &Path, catalog_path: &Path) -> String {
    let migrate = Command::new(bin())
        .args(["migrate", "--platform-store-url", url])
        .output()
        .expect("run migrate");
    assert!(
        migrate.status.success(),
        "migrate failed: {}",
        String::from_utf8_lossy(&migrate.stderr)
    );

    let apply = Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .env(
            "MIGRALOOP_CONTRACT_SOURCE_CATALOG",
            catalog_path.to_str().unwrap(),
        )
        // Injected random table must pass table supplemental-logging probe.
        .env("MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING", "all")
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

fn run_sync(url: &str, catalog_path: &Path, inject_path: &Path) -> std::process::Output {
    Command::new(bin())
        .env("ORACLE_PASSWORD", "oracle-secret-value")
        .env("MONGO_PASSWORD", "mongo-secret-value")
        // Catalog must remain visible so `STUB_TABLE_SUPPLEMENTAL_LOGGING=all`
        // covers the injected random table (same harness as apply / issue #40).
        .env(
            "MIGRALOOP_CONTRACT_SOURCE_CATALOG",
            catalog_path.to_str().unwrap(),
        )
        .env(
            "MIGRALOOP_INJECT_LOGMINER_CONTENTS",
            inject_path.to_str().unwrap(),
        )
        .env("MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING", "all")
        .args(["sync", "--platform-store-url", url])
        .output()
        .expect("run sync")
}

#[tokio::test]
async fn random_schema_full_sync_delivery_path_on_operator_seam() {
    let schema = RandomSchema::generate();
    let url = ephemeral_database_url().await;
    let mongo_database = unique_mongo_database();
    let dir = TempDir::new().expect("tempdir");
    let catalog = write_config(&dir, "random-catalog.json", &schema.catalog_json());
    let inject = write_config(&dir, "random-logminer.json", &schema.logminer_inject_json());
    let config = write_config(
        &dir,
        "deployment.yaml",
        &schema.deployment_yaml(&mongo_database),
    );

    // --- discovery → Initial Load → Base → Delivery ---
    let apply_out = migrate_and_apply(&url, &config, &catalog);
    assert!(
        apply_out.contains("Initial Load complete") && apply_out.contains(&schema.table),
        "expected Initial Load for random table {}, got:\n{apply_out}",
        schema.table
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", &url])
        .output()
        .expect("run status");
    assert!(
        status.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );
    let status_out = String::from_utf8_lossy(&status.stdout);
    assert!(
        status_out.contains(&format!("Pipeline: {}", schema.pipeline))
            && status_out.to_lowercase().contains("direct"),
        "expected Direct Pipeline {} in status, got:\n{status_out}",
        schema.pipeline
    );
    assert!(
        status_out.contains(&format!("Base Dataset: {}", schema.table))
            && status_out.contains("initial_load_complete"),
        "expected Base Dataset for random table after Initial Load, got:\n{status_out}"
    );
    assert!(
        status_out.contains(&schema.unsupported_col)
            && (status_out.contains("BLOB")
                || status_out.to_lowercase().contains("omitted")
                || status_out.to_lowercase().contains("unsupported")),
        "unsupported {}/BLOB must be operator-visible, got:\n{status_out}",
        schema.unsupported_col
    );

    let base_il = Command::new(bin())
        .args([
            "base",
            "--platform-store-url",
            &url,
            "--table",
            &schema.table,
        ])
        .output()
        .expect("run base after Initial Load");
    assert!(
        base_il.status.success(),
        "base inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&base_il.stdout),
        String::from_utf8_lossy(&base_il.stderr)
    );
    let base_il_out = String::from_utf8_lossy(&base_il.stdout);
    for col in [&schema.pk, &schema.label, &schema.note] {
        assert!(
            base_il_out.contains(col.as_str()),
            "Base must keep supported column {col}, got:\n{base_il_out}"
        );
    }
    assert!(
        base_il_out.contains("seed-one") && base_il_out.contains("seed-two"),
        "expected Initial Load rows in Base for random schema, got:\n{base_il_out}"
    );
    assert!(
        !base_il_out.to_lowercase().contains("blob-bytes"),
        "unsupported {} payload must be omitted from Base rows, got:\n{base_il_out}",
        schema.unsupported_col
    );

    let target_il = Command::new(bin())
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "target",
            "--platform-store-url",
            &url,
            "--collection",
            &schema.collection,
        ])
        .output()
        .expect("run target after Delivery");
    assert!(
        target_il.status.success(),
        "target inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&target_il.stdout),
        String::from_utf8_lossy(&target_il.stderr)
    );
    let target_il_out = String::from_utf8_lossy(&target_il.stdout);
    assert!(
        target_il_out.contains("\"_id\": 1") || target_il_out.contains("\"_id\":1"),
        "expected Output Identity from random PK, got:\n{target_il_out}"
    );
    for col in [&schema.label, &schema.note] {
        assert!(
            target_il_out.contains(col.as_str()),
            "Target must expose Managed field name {col} for random schema, got:\n{target_il_out}"
        );
    }
    assert!(
        target_il_out.contains("seed-one") && target_il_out.contains("seed-two"),
        "expected Managed field values delivered for random schema, got:\n{target_il_out}"
    );
    assert!(
        target_il_out.contains("note-a") && target_il_out.contains("note-b"),
        "expected Managed {} values on Target, got:\n{target_il_out}",
        schema.note
    );
    assert!(
        !target_il_out.to_lowercase().contains("blob-bytes")
            && !target_il_out.contains(&schema.unsupported_col),
        "unsupported {} must not be delivered, got:\n{target_il_out}",
        schema.unsupported_col
    );

    // --- Incremental Capture (LogMiner contract + inject) → Base + Target ---
    let sync = run_sync(&url, &catalog, &inject);
    assert!(
        sync.status.success(),
        "sync failed: stdout={} stderr={}",
        String::from_utf8_lossy(&sync.stdout),
        String::from_utf8_lossy(&sync.stderr)
    );
    let sync_out = String::from_utf8_lossy(&sync.stdout);
    assert!(
        sync_out.to_ascii_lowercase().contains("logminer"),
        "expected LogMiner Incremental Capture on product path, got:\n{sync_out}"
    );

    let base_inc = Command::new(bin())
        .args([
            "base",
            "--platform-store-url",
            &url,
            "--table",
            &schema.table,
        ])
        .output()
        .expect("base after Incremental");
    assert!(base_inc.status.success());
    let base_inc_out = String::from_utf8_lossy(&base_inc.stdout);
    assert!(
        base_inc_out.contains("seed-one-updated")
            && base_inc_out.contains("seed-three")
            && !base_inc_out.contains("seed-two"),
        "Incremental must update/insert/delete Base for random schema, got:\n{base_inc_out}"
    );
    assert!(
        base_inc_out.contains("note-a-revised") && base_inc_out.contains("note-c"),
        "Incremental must carry Managed {} values into Base, got:\n{base_inc_out}",
        schema.note
    );

    let target_inc = Command::new(bin())
        .env("MONGO_PASSWORD", "mongo-secret-value")
        .args([
            "target",
            "--platform-store-url",
            &url,
            "--collection",
            &schema.collection,
        ])
        .output()
        .expect("target after Incremental");
    assert!(
        target_inc.status.success(),
        "target inspect failed: stdout={} stderr={}",
        String::from_utf8_lossy(&target_inc.stdout),
        String::from_utf8_lossy(&target_inc.stderr)
    );
    let target_inc_out = String::from_utf8_lossy(&target_inc.stdout);
    for col in [&schema.label, &schema.note] {
        assert!(
            target_inc_out.contains(col.as_str()),
            "Target must still expose Managed field name {col} after Incremental, got:\n{target_inc_out}",
        );
    }
    assert!(
        target_inc_out.contains("seed-one-updated")
            && target_inc_out.contains("seed-three")
            && !target_inc_out.contains("seed-two"),
        "Mongo Delivery must follow Incremental for random schema, got:\n{target_inc_out}"
    );
    assert!(
        target_inc_out.contains("note-a-revised") && target_inc_out.contains("note-c"),
        "Target Managed field values must reflect Incremental for random schema, got:\n{target_inc_out}"
    );
    assert!(
        !target_inc_out.to_lowercase().contains("blob-bytes")
            && !target_inc_out.contains(&schema.unsupported_col),
        "unsupported {} must remain undelivered after Incremental, got:\n{target_inc_out}",
        schema.unsupported_col
    );
}
