//! Source / Target engine seam tests for Deployment runtime (issue #156 / #206).
//!
//! These tests exercise Sync→Delivery slices against [`SourceEngine`] /
//! [`TargetEngine`] only so contract / Fake Source and Mongo / Fake Target swap
//! without rewriting orchestration. Helpers here are test-only — not part of the
//! public Deployment runtime surface (#172).
//!
//! #206: Managed validation trusts engine-agnostic column metadata (`supported` +
//! `data_type()`), not Oracle allow-list helpers at the runtime seam.

use migraloop_capture::{
    CapturePosition, FakeSource, FakeSourceTable, InitialLoadChunk, InitialLoadChunkOptions,
    OracleLogMinerSource, OracleSourceConnect, SourceColumn, SourceEngine,
};
use migraloop_delivery::{DeliveryDocument, FakeTarget, ManagedFieldAs, TargetEngine};
use migraloop_platform_store::{
    Deployment, Pipeline, SecretRef, SecretRefKind, SystemConnection, TlsSettings,
};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};

use crate::RuntimeError;

/// One Initial Load chunk → Managed Delivery documents for a Direct-style snapshot.
async fn deliver_initial_load_chunk_via_engines<S: SourceEngine, T: TargetEngine>(
    source: &S,
    target: &T,
    schema: &str,
    table: &str,
    collection: &str,
    chunk_size: usize,
) -> Result<(Vec<SourceColumn>, InitialLoadChunk, usize, Vec<Value>), RuntimeError> {
    source
        .check_prerequisites(&[table.to_string()])
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    let columns = source
        .discover_schema(schema, table)
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    let chunk = source
        .initial_load_chunk(
            schema,
            table,
            None,
            &InitialLoadChunkOptions {
                chunk_size,
                offset: 0,
                established_watermark: None,
            },
        )
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    let mut documents = Vec::with_capacity(chunk.rows.len());
    for row in &chunk.rows {
        let map: Map<String, Value> = row.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        let identity = crate::output_identity_from_row(&map, &chunk.primary_key)?;
        let managed_fields: Map<String, Value> = row
            .iter()
            .filter(|(name, _)| columns.iter().any(|c| c.supported && c.name == **name))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        documents.push(DeliveryDocument {
            identity,
            managed_fields,
            columns: Vec::new(),
            field_as: Default::default(),
        });
    }

    let delivered = target
        .upsert_managed(collection, &documents)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    let listed = target
        .list_documents(collection)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    Ok((columns, chunk, delivered, listed))
}

#[test]
fn production_factories_expose_source_and_target_engine_interfaces() {
    std::env::set_var("ORACLE_PASSWORD", "oracle-secret-value");
    std::env::set_var("MONGO_PASSWORD", "mongo-secret-value");

    let source = crate::source_engine_from_connection(&SystemConnection {
        kind: "oracle".into(),
        host: "stub".into(),
        port: 1521,
        database: "STUB".into(),
        username: "sync_user".into(),
        password_ref: SecretRef {
            kind: SecretRefKind::Env,
            value: "ORACLE_PASSWORD".into(),
        },
        timezone: String::new(),
        tls: TlsSettings::default(),
    })
    .expect("Source factory");
    fn accept_source<S: SourceEngine>(engine: &S) -> &'static str {
        engine.kind_label()
    }
    assert!(accept_source(&source).starts_with("oracle-logminer"));

    let target = crate::target_engine_from_deployment(&Deployment {
        name: "factory-seam".into(),
        source: SystemConnection {
            kind: "oracle".into(),
            host: "stub".into(),
            port: 1521,
            database: "STUB".into(),
            username: "sync_user".into(),
            password_ref: SecretRef {
                kind: SecretRefKind::Env,
                value: "ORACLE_PASSWORD".into(),
            },
            timezone: String::new(),
            tls: TlsSettings::default(),
        },
        target: SystemConnection {
            kind: "mongodb".into(),
            host: "127.0.0.1".into(),
            port: 27017,
            database: "app".into(),
            username: "deliver_user".into(),
            password_ref: SecretRef {
                kind: SecretRefKind::Env,
                value: "MONGO_PASSWORD".into(),
            },
            timezone: String::new(),
            tls: TlsSettings::default(),
        },
    })
    .expect("Target factory");
    fn accept_target<T: TargetEngine>(engine: &T) -> &'static str {
        engine.kind_label()
    }
    assert_eq!(accept_target(&target), "mongodb");
}

fn customers_fake_source() -> FakeSource {
    FakeSource::new().with_table(
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
            rows: vec![{
                let mut row = BTreeMap::new();
                row.insert("ID".into(), json!(1));
                row.insert("NAME".into(), json!("Ada"));
                row
            }],
            low_watermark: CapturePosition(1000),
            changes: vec![],
        },
    )
}

#[tokio::test]
async fn engine_seam_swaps_source_and_target_adapters_without_rewrite() {
    // Same orchestration helper; Fake Source + Fake Target.
    let fake_source = customers_fake_source();
    let fake_target = FakeTarget::new();
    let (cols, chunk, delivered, listed) = deliver_initial_load_chunk_via_engines(
        &fake_source,
        &fake_target,
        "",
        "CUSTOMERS",
        "customers",
        10,
    )
    .await
    .expect("fake→fake seam");
    assert_eq!(fake_source.kind_label(), "fake");
    assert_eq!(fake_target.kind_label(), "fake");
    assert_eq!(cols.len(), 2);
    assert_eq!(chunk.rows.len(), 1);
    assert_eq!(delivered, 1);
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["NAME"], json!("Ada"));

    // Same orchestration helper; Oracle contract Source + Fake Target.
    let mut catalog = migraloop_capture::ContractSourceCatalog::empty();
    catalog.insert(migraloop_capture::snapshot(
        "CUSTOMERS",
        1000,
        &["ID"],
        vec![
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
        vec![{
            let mut row = BTreeMap::new();
            row.insert("ID".into(), json!(2));
            row.insert("NAME".into(), json!("Bob"));
            row
        }],
    ));
    migraloop_capture::set_contract_source_catalog_override(catalog);
    let contract = OracleLogMinerSource::new(
        OracleSourceConnect {
            host: "contract".into(),
            port: 1521,
            database: "X".into(),
            username: "u".into(),
            tls: Default::default(),
        },
        "unused",
    );
    let contract_target = FakeTarget::new();
    let result = deliver_initial_load_chunk_via_engines(
        &contract,
        &contract_target,
        "",
        "CUSTOMERS",
        "customers",
        10,
    )
    .await;
    migraloop_capture::clear_contract_source_catalog_override();
    let (cols2, chunk2, delivered2, listed2) = result.expect("contract→fake seam");
    assert_eq!(contract.kind_label(), "oracle-logminer-contract");
    assert_eq!(cols2.len(), 2);
    assert_eq!(chunk2.rows.len(), 1);
    assert_eq!(delivered2, 1);
    assert_eq!(listed2.len(), 1);
    assert_eq!(listed2[0]["NAME"], json!("Bob"));
}

fn sample_pipeline_with_field(field: &str, mapping: ManagedFieldAs) -> Pipeline {
    let mut field_mappings = BTreeMap::new();
    field_mappings.insert(field.to_string(), mapping);
    Pipeline {
        deployment_name: "seam".into(),
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
        field_mappings,
        output_identity: vec![],
        transform_json: None,
        drift_status: "unknown".into(),
        drift_checked_rows: 0,
        drift_mismatched_rows: 0,
    }
}

/// #206: runtime Managed validation trusts adapter `supported`, not Oracle allow-list.
#[test]
fn managed_validation_trusts_source_supported_flag_not_oracle_allow_list() {
    let pipeline = sample_pipeline_with_field("BIO", ManagedFieldAs::String);
    // Adapter-declared supported — even when the type name would fail Oracle allow-list.
    let columns = vec![SourceColumn {
        name: "BIO".into(),
        oracle_type: "BLOB".into(),
        supported: true,
        precision: None,
        scale: None,
        size: None,
    }];
    let managed = BTreeSet::from(["BIO".to_string()]);
    crate::validate_pipeline_managed_fields(&pipeline, &columns, &managed)
        .expect("supported=true must pass without Oracle allow-list at runtime seam");
}

/// #206: unsupported Managed inputs still fail using engine-agnostic type metadata.
#[test]
fn managed_validation_rejects_unsupported_via_supported_flag_and_data_type() {
    let pipeline = sample_pipeline_with_field("BIO", ManagedFieldAs::String);
    let columns = vec![SourceColumn {
        name: "BIO".into(),
        oracle_type: "BLOB".into(),
        supported: false,
        precision: None,
        scale: None,
        size: None,
    }];
    let managed = BTreeSet::from(["BIO".to_string()]);
    let err = crate::validate_pipeline_managed_fields(&pipeline, &columns, &managed)
        .expect_err("supported=false Managed input must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("unsupported") && msg.contains("BIO") && msg.contains("BLOB"),
        "expected agnostic unsupported message naming field + data_type, got: {msg}"
    );
    assert!(
        !msg.contains("Oracle"),
        "runtime seam error must not brand Oracle, got: {msg}"
    );
}
