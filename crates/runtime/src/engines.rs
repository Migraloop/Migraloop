//! Source / Target engine seams for Deployment runtime (issue #156).
//!
//! Orchestration helpers here are written only against [`SourceEngine`] and
//! [`TargetEngine`] so contract / Fake Source and Mongo / Fake Target swap
//! without rewriting Sync/Delivery control-plane logic.

use migraloop_capture::{
    InitialLoadChunk, InitialLoadChunkOptions, SourceColumn, SourceEngine,
};
use migraloop_delivery::{DeliveryDocument, TargetEngine};
use serde_json::{Map, Value};

use crate::RuntimeError;

/// One Initial Load chunk → Managed Delivery documents for a Direct-style snapshot.
///
/// Used by engine-seam tests to prove the same Sync→Delivery slice works when
/// Source and Target adapters are swapped.
pub async fn deliver_initial_load_chunk_via_engines<S: SourceEngine, T: TargetEngine>(
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
        let identity = identity_from_primary_key(row, &chunk.primary_key)?;
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

fn identity_from_primary_key(
    row: &std::collections::BTreeMap<String, Value>,
    primary_key: &[String],
) -> Result<Value, RuntimeError> {
    let map: Map<String, Value> = row.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    crate::output_identity_from_row(&map, primary_key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use migraloop_capture::{
        FakeSource, FakeSourceTable, OracleLogMinerSource, OracleSourceConnect, SourceColumn,
        CapturePosition,
    };
    use migraloop_delivery::FakeTarget;
    use serde_json::json;
    use std::collections::BTreeMap;

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
        assert_eq!(listed2[0]["NAME"], json!("Bob"));
    }
}
