//! Behaviour seam: store/delivery domain columns use shared ColumnShape (#182);
//! SourceEngine discovered columns expose ColumnShape beside oracle_type (#202).
//!
//! Agreed seams (#168 / #199 Testing Decisions / #182 / #202 AC):
//! - Platform Store / Delivery domain column types no longer expose Oracle-named
//!   fields as the default shape (`data_type` via ColumnShape).
//! - Source adapters still carry engine-specific type metadata (`oracle_type`)
//!   while exposing engine-agnostic ColumnShape / data_type at the seam (expand).
//! - Shared NUMBER classification lives next to ColumnShape (ADR-0023).
//! - Prior-release Platform Store JSON with `oracle_type` still deserializes
//!   (ADR-0014); new writes use `data_type`.
//! - No platform business catalog (ADR-0026); shapes come from Source discovery.

use migraloop_capture::{FakeSource, FakeSourceTable, SourceColumn, SourceEngine};
use migraloop_delivery::DeliveryColumn;
use migraloop_platform_store::{BaseColumn, OmittedColumn};
use migraloop_types::{classify_number, ColumnShape, NumberMongoMapping};

#[test]
fn managed_base_metadata_round_trips_source_store_delivery_through_column_shape() {
    // Independent literals from Source discovery — not a platform catalog.
    let shape = ColumnShape {
        name: "AMOUNT".into(),
        data_type: "NUMBER".into(),
        precision: Some(12),
        scale: Some(2),
    };

    let source = SourceColumn {
        name: "AMOUNT".into(),
        oracle_type: "NUMBER".into(),
        supported: true,
        precision: Some(12),
        scale: Some(2),
        size: None,
    };
    // Engine brand stays on the Source adapter beside the agnostic shape.
    assert_eq!(source.oracle_type, "NUMBER");
    assert_eq!(source.data_type(), "NUMBER");
    assert_eq!(source.column_shape(), shape);
    assert_eq!(ColumnShape::from(source.clone()), shape);

    let base: BaseColumn = source.column_shape();
    assert_eq!(base, shape);
    assert_eq!(base.data_type, "NUMBER");

    let delivery: DeliveryColumn = base;
    assert_eq!(delivery, shape);
    assert_eq!(delivery.data_type, "NUMBER");
}

#[test]
fn source_engine_discovered_columns_expose_column_shape_beside_oracle_type() {
    use migraloop_capture::CapturePosition;
    use serde_json::json;
    use std::collections::BTreeMap;

    let source = FakeSource::new().with_table(
        "CUSTOMERS",
        FakeSourceTable {
            columns: vec![SourceColumn {
                name: "AMOUNT".into(),
                oracle_type: "NUMBER".into(),
                supported: true,
                precision: Some(12),
                scale: Some(2),
                size: None,
            }],
            primary_key: vec!["AMOUNT".into()],
            rows: vec![{
                let mut row = BTreeMap::new();
                row.insert("AMOUNT".into(), json!(1));
                row
            }],
            low_watermark: CapturePosition(1),
            changes: vec![],
        },
    );
    let columns = source.discover_schema("", "CUSTOMERS").unwrap();
    let col = &columns[0];
    assert_eq!(col.oracle_type, "NUMBER");
    assert_eq!(
        ColumnShape::from(col.clone()),
        ColumnShape {
            name: "AMOUNT".into(),
            data_type: "NUMBER".into(),
            precision: Some(12),
            scale: Some(2),
        }
    );
    // Shared NUMBER classify next to ColumnShape (ADR-0023) — Decimal128 for (12,2).
    assert_eq!(
        classify_number(col.precision, col.scale),
        NumberMongoMapping::Decimal128
    );
}

#[test]
fn store_and_delivery_wire_default_to_data_type_and_accept_legacy_oracle_type() {
    let legacy = r#"{"name":"ID","oracle_type":"NUMBER","precision":10,"scale":0}"#;
    let from_legacy: BaseColumn = serde_json::from_str(legacy).unwrap();
    assert_eq!(
        from_legacy,
        ColumnShape {
            name: "ID".into(),
            data_type: "NUMBER".into(),
            precision: Some(10),
            scale: Some(0),
        }
    );

    let shaped = r#"{"name":"ID","data_type":"NUMBER","precision":10,"scale":0}"#;
    let from_shape: BaseColumn = serde_json::from_str(shaped).unwrap();
    assert_eq!(from_shape.data_type, "NUMBER");
    assert_eq!(from_shape, from_legacy);

    let delivery_from_shape: DeliveryColumn = serde_json::from_str(shaped).unwrap();
    assert_eq!(delivery_from_shape.data_type, "NUMBER");
    assert_eq!(
        delivery_from_shape,
        ColumnShape {
            name: "ID".into(),
            data_type: "NUMBER".into(),
            precision: Some(10),
            scale: Some(0),
        }
    );

    let written = serde_json::to_string(&BaseColumn {
        name: "NAME".into(),
        data_type: "VARCHAR2".into(),
        precision: None,
        scale: None,
    })
    .unwrap();
    // Contract: default wire shape is data_type (Oracle-named field dropped).
    assert!(written.contains(r#""data_type":"VARCHAR2""#));
    assert!(!written.contains(r#""oracle_type""#));
}

#[test]
fn omitted_column_wire_defaults_to_data_type_and_accepts_legacy_oracle_type() {
    let legacy = r#"{"name":"PHOTO","oracle_type":"BLOB"}"#;
    let from_legacy: OmittedColumn = serde_json::from_str(legacy).unwrap();
    assert_eq!(from_legacy.name, "PHOTO");
    assert_eq!(from_legacy.data_type, "BLOB");

    let written = serde_json::to_string(&OmittedColumn {
        name: "PHOTO".into(),
        data_type: "BLOB".into(),
    })
    .unwrap();
    assert!(written.contains(r#""data_type":"BLOB""#));
    assert!(!written.contains(r#""oracle_type""#));
}
