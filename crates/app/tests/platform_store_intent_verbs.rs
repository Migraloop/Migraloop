//! Platform Store Deployment-intent persistence verbs (issues #177 / #204 / #168).
//!
//! Agreed seam (#168 / #199 Testing Decisions): exercise session intent verbs for
//! Sync/Delivery progress, quarantine, schema-impact, Source Alignment, Drift,
//! and Pipeline lifecycle composites — not fine-grained column CRUD sequencing.
//! Postgres remains the only store engine (ADR-0001).

mod common;

use migraloop_platform_store::{
    BaseColumn, BaseDataset, BaseRowMutation, Deployment, DerivedDataset, Pipeline, PlatformStore,
    QuarantinedChange, SchemaChangeImpact, SecretRef, SecretRefKind, SystemConnection,
    TlsSettings,
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
async fn persist_base_dataset_sync_fields_updates_metadata_without_rewriting_rows() {
    let store = open_migrated_store().await;
    store
        .upsert_deployment(&sample_deployment("intent-sync-meta"))
        .await
        .expect("upsert Deployment");

    let mut row_a = serde_json::Map::new();
    row_a.insert("ID".to_string(), serde_json::json!(1));
    let mut row_b = serde_json::Map::new();
    row_b.insert("ID".to_string(), serde_json::json!(2));
    let mut dataset = sample_base("intent-sync-meta");
    dataset.row_count = 2;
    dataset.status = "initial_load_complete".to_string();
    store
        .replace_base_dataset(&dataset, &[row_a, row_b])
        .await
        .expect("seed Base rows");

    dataset.status = "incremental".to_string();
    dataset.capture_checkpoint = Some(999);
    dataset.sync_lag = 0;
    dataset.sync_health = "ok".to_string();
    store
        .persist_base_dataset_sync_fields(&dataset)
        .await
        .expect("persist Sync fields only");

    let (loaded, rows) = store
        .get_base_rows("CUSTOMERS", Some("intent-sync-meta"))
        .await
        .expect("load Base");
    assert_eq!(loaded.status, "incremental");
    assert_eq!(loaded.capture_checkpoint, Some(999));
    assert_eq!(loaded.row_count, 2);
    assert_eq!(
        rows.len(),
        2,
        "empty-window Sync metadata must not DELETE+rewrite base_rows"
    );
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

/// Direct Sync throughput seam (#230): one Base mutation must not DELETE+rewrite peers.
#[tokio::test]
async fn record_sync_row_progress_upserts_one_row_without_dropping_peers() {
    let store = open_migrated_store().await;
    store
        .upsert_deployment(&sample_deployment("intent-sync-row"))
        .await
        .expect("upsert Deployment");

    let mut seed_rows = Vec::new();
    for id in 1..=100 {
        let mut row = serde_json::Map::new();
        row.insert("ID".to_string(), serde_json::json!(id));
        row.insert("NAME".to_string(), serde_json::json!(format!("name-{id}")));
        seed_rows.push(row);
    }
    let mut dataset = sample_base("intent-sync-row");
    dataset.row_count = 100;
    dataset.status = "incremental".to_string();
    dataset.sync_applied_changes = 0;
    dataset.capture_checkpoint = Some(100);
    dataset.columns = vec![
        BaseColumn {
            name: "ID".to_string(),
            data_type: "NUMBER".to_string(),
            precision: Some(10),
            scale: Some(0),
        },
        BaseColumn {
            name: "NAME".to_string(),
            data_type: "VARCHAR2".to_string(),
            precision: Some(100),
            scale: None,
        },
    ];
    store
        .replace_base_dataset(&dataset, &seed_rows)
        .await
        .expect("seed 100 Base rows");

    let mut identity = serde_json::Map::new();
    identity.insert("ID".to_string(), serde_json::json!(50));
    let mut updated = serde_json::Map::new();
    updated.insert("ID".to_string(), serde_json::json!(50));
    updated.insert("NAME".to_string(), serde_json::json!("name-50-updated"));

    dataset.sync_applied_changes = 1;
    dataset.capture_checkpoint = Some(150);
    dataset.sync_lag = 0;
    store
        .record_sync_row_progress(
            &dataset,
            BaseRowMutation::Upsert {
                identity: &identity,
                row: &updated,
            },
            &[("chg-150".to_string(), 150)],
        )
        .await
        .expect("delta upsert Base row");

    let mut insert_identity = serde_json::Map::new();
    insert_identity.insert("ID".to_string(), serde_json::json!(101));
    let mut inserted = insert_identity.clone();
    inserted.insert("NAME".to_string(), serde_json::json!("name-101"));
    dataset.row_count = 101;
    dataset.sync_applied_changes = 2;
    dataset.capture_checkpoint = Some(151);
    store
        .record_sync_row_progress(
            &dataset,
            BaseRowMutation::Upsert {
                identity: &insert_identity,
                row: &inserted,
            },
            &[("chg-151".to_string(), 151)],
        )
        .await
        .expect("delta insert Base row");

    let mut delete_identity = serde_json::Map::new();
    delete_identity.insert("ID".to_string(), serde_json::json!(1));
    dataset.row_count = 100;
    dataset.sync_applied_changes = 3;
    dataset.capture_checkpoint = Some(152);
    store
        .record_sync_row_progress(
            &dataset,
            BaseRowMutation::Delete {
                identity: &delete_identity,
            },
            &[("chg-152".to_string(), 152)],
        )
        .await
        .expect("delta delete Base row");

    let (loaded, rows) = store
        .get_base_rows("CUSTOMERS", Some("intent-sync-row"))
        .await
        .expect("load Base after delta mutations");
    assert_eq!(loaded.row_count, 100);
    assert_eq!(loaded.sync_applied_changes, 3);
    assert_eq!(loaded.capture_checkpoint, Some(152));
    assert_eq!(rows.len(), 100, "peers must survive delta Sync persist");

    let row_50 = rows
        .iter()
        .find(|r| r.data.get("ID") == Some(&serde_json::json!(50)))
        .expect("row 50 present");
    assert_eq!(
        row_50.data.get("NAME"),
        Some(&serde_json::json!("name-50-updated"))
    );
    assert!(
        rows.iter()
            .any(|r| r.data.get("ID") == Some(&serde_json::json!(101))),
        "inserted row 101 must be present"
    );
    assert!(
        rows
            .iter()
            .all(|r| r.data.get("ID") != Some(&serde_json::json!(1))),
        "deleted row 1 must be gone"
    );
    assert!(
        rows
            .iter()
            .any(|r| r.data.get("ID") == Some(&serde_json::json!(2))),
        "untouched peer row 2 must remain"
    );

    let unapplied = store
        .filter_unapplied_change_ids(
            "intent-sync-row",
            "APP",
            "CUSTOMERS",
            &[
                "chg-150".to_string(),
                "chg-151".to_string(),
                "chg-152".to_string(),
                "chg-153".to_string(),
            ],
        )
        .await
        .expect("filter applied");
    assert_eq!(unapplied, vec!["chg-153".to_string()]);
}

/// Direct Incremental window batch persist (#252): many Base mutations + change ids
/// in one TX must leave peers intact and record every applied id.
#[tokio::test]
async fn record_sync_rows_progress_batches_mutations_without_dropping_peers() {
    let store = open_migrated_store().await;
    store
        .upsert_deployment(&sample_deployment("intent-sync-rows"))
        .await
        .expect("upsert Deployment");

    let mut seed_rows = Vec::new();
    for id in 1..=50 {
        let mut row = serde_json::Map::new();
        row.insert("ID".to_string(), serde_json::json!(id));
        row.insert("NAME".to_string(), serde_json::json!(format!("name-{id}")));
        seed_rows.push(row);
    }
    let mut dataset = sample_base("intent-sync-rows");
    dataset.row_count = 50;
    dataset.status = "incremental".to_string();
    dataset.sync_applied_changes = 0;
    dataset.capture_checkpoint = Some(100);
    dataset.columns = vec![
        BaseColumn {
            name: "ID".to_string(),
            data_type: "NUMBER".to_string(),
            precision: Some(10),
            scale: Some(0),
        },
        BaseColumn {
            name: "NAME".to_string(),
            data_type: "VARCHAR2".to_string(),
            precision: Some(100),
            scale: None,
        },
    ];
    store
        .replace_base_dataset(&dataset, &seed_rows)
        .await
        .expect("seed Base rows");

    let mut identity_10 = serde_json::Map::new();
    identity_10.insert("ID".to_string(), serde_json::json!(10));
    let mut updated_10 = identity_10.clone();
    updated_10.insert("NAME".to_string(), serde_json::json!("name-10-batched"));

    let mut identity_51 = serde_json::Map::new();
    identity_51.insert("ID".to_string(), serde_json::json!(51));
    let mut inserted_51 = identity_51.clone();
    inserted_51.insert("NAME".to_string(), serde_json::json!("name-51"));

    let mut identity_1 = serde_json::Map::new();
    identity_1.insert("ID".to_string(), serde_json::json!(1));

    dataset.row_count = 50;
    dataset.sync_applied_changes = 3;
    dataset.capture_checkpoint = Some(160);
    dataset.sync_lag = 0;
    store
        .record_sync_rows_progress(
            &dataset,
            &[
                BaseRowMutation::Upsert {
                    identity: &identity_10,
                    row: &updated_10,
                },
                BaseRowMutation::Insert {
                    identity: &identity_51,
                    row: &inserted_51,
                },
                BaseRowMutation::Delete {
                    identity: &identity_1,
                },
            ],
            &[
                ("chg-158".to_string(), 158),
                ("chg-159".to_string(), 159),
                ("chg-160".to_string(), 160),
            ],
        )
        .await
        .expect("batched Base mutations");

    let (loaded, rows) = store
        .get_base_rows("CUSTOMERS", Some("intent-sync-rows"))
        .await
        .expect("load Base after batch");
    assert_eq!(loaded.row_count, 50);
    assert_eq!(loaded.sync_applied_changes, 3);
    assert_eq!(loaded.capture_checkpoint, Some(160));
    assert_eq!(rows.len(), 50, "peers must survive batched Sync persist");

    let row_10 = rows
        .iter()
        .find(|r| r.data.get("ID") == Some(&serde_json::json!(10)))
        .expect("row 10 present");
    assert_eq!(
        row_10.data.get("NAME"),
        Some(&serde_json::json!("name-10-batched"))
    );
    assert!(
        rows.iter()
            .any(|r| r.data.get("ID") == Some(&serde_json::json!(51))),
        "inserted row 51 must be present"
    );
    assert!(
        rows
            .iter()
            .all(|r| r.data.get("ID") != Some(&serde_json::json!(1))),
        "deleted row 1 must be gone"
    );
    assert!(
        rows
            .iter()
            .any(|r| r.data.get("ID") == Some(&serde_json::json!(2))),
        "untouched peer row 2 must remain"
    );

    let unapplied = store
        .filter_unapplied_change_ids(
            "intent-sync-rows",
            "APP",
            "CUSTOMERS",
            &[
                "chg-158".to_string(),
                "chg-159".to_string(),
                "chg-160".to_string(),
                "chg-161".to_string(),
            ],
        )
        .await
        .expect("filter applied");
    assert_eq!(unapplied, vec!["chg-161".to_string()]);
}

/// Transform Sync throughput seam (#231): Affect recompute must not DELETE+rewrite
/// untouched Derived peers (ADR-0029 Transform path, mirrors Base #230).
#[tokio::test]
async fn apply_derived_identity_changes_upserts_without_dropping_peers() {
    let store = open_migrated_store().await;
    store
        .upsert_deployment(&sample_deployment("intent-derived-row"))
        .await
        .expect("upsert Deployment");
    let mut pipeline = sample_pipeline("intent-derived-row", "active-customers");
    pipeline.mode = "transform".to_string();
    pipeline.target_collection = "active_customers".to_string();
    pipeline.output_identity = vec!["ID".to_string()];
    store
        .replace_pipelines("intent-derived-row", &[pipeline])
        .await
        .expect("upsert Transform Pipeline");

    let mut seed_rows = Vec::new();
    for id in 1..=100 {
        let mut row = serde_json::Map::new();
        row.insert("ID".to_string(), serde_json::json!(id));
        row.insert(
            "NAME".to_string(),
            serde_json::json!(format!("customer-{id}")),
        );
        seed_rows.push(row);
    }
    let mut dataset = DerivedDataset {
        deployment_name: "intent-derived-row".to_string(),
        pipeline_name: "active-customers".to_string(),
        status: "materialized".to_string(),
        output_identity: vec!["ID".to_string()],
        columns: vec![
            BaseColumn {
                name: "ID".to_string(),
                data_type: "NUMBER".to_string(),
                precision: Some(10),
                scale: Some(0),
            },
            BaseColumn {
                name: "NAME".to_string(),
                data_type: "VARCHAR2".to_string(),
                precision: Some(100),
                scale: None,
            },
        ],
        row_count: 100,
    };
    store
        .replace_derived_dataset(&dataset, &seed_rows)
        .await
        .expect("seed 100 Derived rows");

    let mut remove_id = serde_json::Map::new();
    remove_id.insert("ID".to_string(), serde_json::json!(50));
    let mut updated = serde_json::Map::new();
    updated.insert("ID".to_string(), serde_json::json!(50));
    updated.insert("NAME".to_string(), serde_json::json!("customer-50-updated"));

    let mut insert_id = serde_json::Map::new();
    insert_id.insert("ID".to_string(), serde_json::json!(101));
    let mut inserted = insert_id.clone();
    inserted.insert("NAME".to_string(), serde_json::json!("customer-101"));

    let mut delete_id = serde_json::Map::new();
    delete_id.insert("ID".to_string(), serde_json::json!(1));

    dataset.row_count = 100; // -1 delete +1 insert; update in place
    store
        .apply_derived_identity_changes(
            &dataset,
            &[remove_id, insert_id.clone(), delete_id],
            &[updated, inserted],
        )
        .await
        .expect("delta Derived identity mutations");

    let (loaded, rows) = store
        .get_derived_rows("active-customers", Some("intent-derived-row"))
        .await
        .expect("load Derived after delta mutations");
    assert_eq!(loaded.row_count, 100);
    assert_eq!(rows.len(), 100, "peers must survive delta Derived persist");

    let row_50 = rows
        .iter()
        .find(|r| r.data.get("ID") == Some(&serde_json::json!(50)))
        .expect("row 50 present");
    assert_eq!(
        row_50.data.get("NAME"),
        Some(&serde_json::json!("customer-50-updated"))
    );
    assert!(
        rows.iter()
            .any(|r| r.data.get("ID") == Some(&serde_json::json!(101))),
        "inserted Derived row 101 must be present"
    );
    assert!(
        rows.iter()
            .all(|r| r.data.get("ID") != Some(&serde_json::json!(1))),
        "deleted Derived row 1 must be gone"
    );
    assert!(
        rows.iter()
            .any(|r| r.data.get("ID") == Some(&serde_json::json!(2))),
        "untouched peer Derived row 2 must remain"
    );
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
async fn delete_deployment_removes_poison_quarantine_rows() {
    let store = open_migrated_store().await;
    store
        .upsert_deployment(&sample_deployment("intent-quarantine-delete"))
        .await
        .expect("upsert Deployment");
    store
        .replace_pipelines(
            "intent-quarantine-delete",
            &[sample_pipeline("intent-quarantine-delete", "customers")],
        )
        .await
        .expect("replace Pipelines");

    store
        .quarantine_change(&QuarantinedChange {
            deployment_name: "intent-quarantine-delete".to_string(),
            pipeline_name: "customers".to_string(),
            source_schema: "APP".to_string(),
            source_table: "CUSTOMERS".to_string(),
            change_id: "poison-delete-1".to_string(),
            capture_position: 77,
            output_identity: serde_json::json!({"ID": 1}),
            stage: "delivery".to_string(),
            attempts: 2,
            last_error: "injected".to_string(),
            status: "quarantined".to_string(),
        })
        .await
        .expect("quarantine change");

    store
        .delete_deployment("intent-quarantine-delete")
        .await
        .expect("delete Deployment");

    let leftover = store
        .list_quarantined_changes(Some("intent-quarantine-delete"))
        .await
        .expect("list quarantine after delete");
    assert!(
        leftover.is_empty(),
        "delete_deployment must remove poison_quarantine rows (Namespace wipe / Lab remove)"
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
