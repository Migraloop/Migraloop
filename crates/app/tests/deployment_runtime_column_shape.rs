//! Behaviour seam: Managed/Base column metadata via shared ColumnShape (#181).
//!
//! Agreed seams (#168 Testing Decisions / #181 AC):
//! - Capture → Platform Store → Delivery → runtime consume shared ColumnShape.
//! - Remap pass-throughs go through ColumnShape (not field-by-field oracle copies).
//! - Old Oracle-named fields remain on store/delivery types during expand–contract.
//! - No platform business catalog (ADR-0026); shapes come from Source discovery.

use migraloop_capture::SourceColumn;
use migraloop_delivery::DeliveryColumn;
use migraloop_platform_store::BaseColumn;
use migraloop_types::ColumnShape;

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
    assert_eq!(source.column_shape(), shape);

    let base = BaseColumn::from(source.column_shape());
    assert_eq!(ColumnShape::from(base.clone()), shape);
    // Old field still present until contract (#182).
    assert_eq!(base.oracle_type, "NUMBER");

    let delivery = DeliveryColumn::from(ColumnShape::from(base));
    assert_eq!(ColumnShape::from(delivery.clone()), shape);
    assert_eq!(delivery.oracle_type, "NUMBER");
}

#[test]
fn base_column_wire_keeps_oracle_type_and_accepts_column_shape_data_type() {
    let legacy = r#"{"name":"ID","oracle_type":"NUMBER","precision":10,"scale":0}"#;
    let from_legacy: BaseColumn = serde_json::from_str(legacy).unwrap();
    assert_eq!(
        ColumnShape::from(from_legacy.clone()),
        ColumnShape {
            name: "ID".into(),
            data_type: "NUMBER".into(),
            precision: Some(10),
            scale: Some(0),
        }
    );

    let shaped = r#"{"name":"ID","data_type":"NUMBER","precision":10,"scale":0}"#;
    let from_shape: BaseColumn = serde_json::from_str(shaped).unwrap();
    assert_eq!(from_shape.oracle_type, "NUMBER");
    assert_eq!(ColumnShape::from(from_shape), ColumnShape::from(from_legacy));

    let written = serde_json::to_string(&BaseColumn::from(ColumnShape {
        name: "NAME".into(),
        data_type: "VARCHAR2".into(),
        precision: None,
        scale: None,
    }))
    .unwrap();
    // Migrate batch keeps writing the old wire key so prior-release rows stay coherent.
    assert!(written.contains(r#""oracle_type":"VARCHAR2""#));
    assert!(!written.contains(r#""data_type""#));
}
