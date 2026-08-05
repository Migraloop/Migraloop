//! Delivery of Managed fields to a Target System by Output Identity.
//!
//! v1: MongoDB document Delivery. Updates `$set` only Managed fields so
//! non-Managed keys are left alone (ADR-0002). Inserts are identity + Managed.
//!
//! Conversion is schema-driven (ADR-0018 / ADR-0022 / ADR-0023): NUMBER → Long or
//! Decimal128 via shared [`migraloop_types::classify_number`] next to [`ColumnShape`]
//! (never default IEEE double); temporals → UTC DateTime.

mod engine;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use bson::{doc, Bson, Document};
use chrono::{DateTime, Utc};
use migraloop_types::{classify_number, ColumnShape, NumberMongoMapping, TlsSettings};
use mongodb::options::{ClientOptions, Tls, TlsOptions, UpdateOptions};
use mongodb::{Client, Collection};
use serde_json::Value;
use thiserror::Error;

pub use migraloop_types::ManagedFieldAs;
pub use engine::{
    target_engine_delivery_roundtrip, test_delivery_document, FakeTarget, TargetEngine,
};

/// Module seam marker retained by the single app binary.
pub const SEAM: &str = "delivery";

fn normalize_data_type(data_type: &str) -> String {
    let trimmed = data_type.trim();
    let base = trimmed
        .split_once('(')
        .map(|(head, _)| head)
        .unwrap_or(trimmed)
        .trim();
    base.to_ascii_uppercase()
}

#[derive(Debug, Error)]
pub enum DeliveryError {
    #[error("failed to connect to Target System: {0}")]
    Connect(String),
    #[error("failed to deliver to Target System: {0}")]
    Apply(String),
    #[error("invalid Output Identity: {0}")]
    Identity(String),
    #[error("schema conversion failed for field {field}: {reason}")]
    Conversion { field: String, reason: String },
}

/// Non-secret Target System connection used for Mongo Delivery.
#[derive(Debug, Clone)]
pub struct MongoTargetConnection {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: String,
    pub password: String,
    /// Shared TLS settings; Mongo wire adapters use `ca_file` / skip-verify.
    pub tls: TlsSettings,
}

/// Column schema used for schema-driven BSON conversion.
///
/// Domain metadata is the shared [`ColumnShape`] — Oracle-named fields are not
/// the Delivery domain default (issue #182). NUMBER classification calls through
/// to the shared home next to [`ColumnShape`] (issue #202 / ADR-0023).
pub type DeliveryColumn = ColumnShape;

/// One Output Identity plus the Managed fields Delivery will write.
#[derive(Debug, Clone)]
pub struct DeliveryDocument {
    /// Value stored as Mongo `_id` (Output Identity).
    pub identity: Value,
    /// Managed fields only (may include identity source columns).
    pub managed_fields: serde_json::Map<String, Value>,
    /// Schema for Managed fields (schema-driven conversion).
    pub columns: Vec<DeliveryColumn>,
    /// Per-field mapping overrides (`string` / `omit`).
    pub field_as: BTreeMap<String, ManagedFieldAs>,
}

fn build_uri(target: &MongoTargetConnection) -> String {
    format!(
        "mongodb://{}:{}@{}:{}/{}?authSource=admin",
        urlencoding::encode(&target.username),
        urlencoding::encode(&target.password),
        target.host,
        target.port,
        urlencoding::encode(&target.database),
    )
}

/// Build Mongo `Tls` options when Target TLS is enabled (thin wire adapter).
pub fn mongo_tls_options(tls: &TlsSettings) -> Option<Tls> {
    if !tls.enabled {
        return None;
    }
    let mut options = TlsOptions::default();
    let ca = tls.ca_file.trim();
    if !ca.is_empty() {
        options.ca_file_path = Some(PathBuf::from(ca));
    }
    if tls.insecure_skip_verify {
        options.allow_invalid_certificates = Some(true);
    }
    Some(Tls::Enabled(options))
}

fn map_mongo_connect_error(target: &MongoTargetConnection, err: impl ToString) -> DeliveryError {
    let detail = err.to_string();
    if target.tls.enabled {
        DeliveryError::Connect(format!(
            "Target System TLS was requested but could not be established \
             (host={}, caFile={:?}): {detail} (no silent cleartext fallback)",
            target.host,
            target.tls.ca_file
        ))
    } else {
        DeliveryError::Connect(detail)
    }
}

async fn collection(
    target: &MongoTargetConnection,
    collection_name: &str,
) -> Result<Collection<Document>, DeliveryError> {
    let uri = build_uri(target);
    let mut options = ClientOptions::parse(&uri)
        .await
        .map_err(|err| map_mongo_connect_error(target, err))?;
    options.server_selection_timeout = Some(Duration::from_secs(5));
    options.connect_timeout = Some(Duration::from_secs(5));
    if let Some(tls) = mongo_tls_options(&target.tls) {
        options.tls = Some(tls);
    }
    let client = Client::with_options(options).map_err(|err| map_mongo_connect_error(target, err))?;
    if target.tls.enabled {
        // Force the TLS handshake now so misconfig fails as Connect (not a later Apply),
        // with no silent cleartext fallback.
        client
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .await
            .map_err(|err| map_mongo_connect_error(target, err))?;
    }
    Ok(client
        .database(&target.database)
        .collection::<Document>(collection_name))
}

fn column_map(columns: &[DeliveryColumn]) -> BTreeMap<&str, &DeliveryColumn> {
    columns.iter().map(|c| (c.name.as_str(), c)).collect()
}

fn json_identity_to_bson(
    value: &Value,
    columns: &[DeliveryColumn],
    primary_hint: Option<&DeliveryColumn>,
) -> Result<Bson, DeliveryError> {
    match value {
        Value::Object(map) => {
            let by_name = column_map(columns);
            let mut doc = Document::new();
            for (key, val) in map {
                let col = by_name.get(key.as_str()).copied();
                doc.insert(
                    key,
                    value_to_bson(key, val, col, ManagedFieldAs::Default)?,
                );
            }
            Ok(Bson::Document(doc))
        }
        other => {
            let col = primary_hint;
            value_to_bson("_id", other, col, ManagedFieldAs::Default)
        }
    }
}

/// Convert a platform JSON value to BSON using Oracle column schema.
pub fn value_to_bson(
    field: &str,
    value: &Value,
    column: Option<&DeliveryColumn>,
    mapping: ManagedFieldAs,
) -> Result<Bson, DeliveryError> {
    if matches!(value, Value::Null) {
        return Ok(Bson::Null);
    }
    if mapping == ManagedFieldAs::String {
        return Ok(Bson::String(json_to_plain_string(value)));
    }

    // Nested documents/arrays from Rich Transform (e.g. equiLookup `as`) are not
    // Oracle scalars — convert structurally regardless of column metadata.
    if matches!(value, Value::Array(_) | Value::Object(_)) {
        return nested_json_to_bson(field, value);
    }

    let Some(column) = column else {
        // No schema: preserve non-float JSON numbers as integers when possible.
        return json_fallback_to_bson(field, value);
    };

    let data_type = normalize_data_type(&column.data_type);
    if data_type == "JSON" {
        return nested_json_to_bson(field, value);
    }
    match data_type.as_str() {
        "NUMBER" => number_to_bson(field, value, column.precision, column.scale, mapping),
        "FLOAT" | "BINARY_FLOAT" | "BINARY_DOUBLE" => float_to_bson(field, value),
        "DATE" | "TIMESTAMP" | "TIMESTAMP WITH TIME ZONE" | "TIMESTAMP WITH LOCAL TIME ZONE" => {
            datetime_to_bson(field, value)
        }
        "CHAR" | "NCHAR" | "VARCHAR2" | "NVARCHAR2" | "RAW" => match value {
            Value::String(s) => Ok(Bson::String(s.clone())),
            other => Ok(Bson::String(json_to_plain_string(other))),
        },
        other => Err(DeliveryError::Conversion {
            field: field.to_string(),
            reason: format!("unsupported Source type {other} for Delivery"),
        }),
    }
}

fn number_to_bson(
    field: &str,
    value: &Value,
    precision: Option<i32>,
    scale: Option<i32>,
    mapping: ManagedFieldAs,
) -> Result<Bson, DeliveryError> {
    if mapping == ManagedFieldAs::String {
        return Ok(Bson::String(json_to_plain_string(value)));
    }
    match classify_number(precision, scale) {
        NumberMongoMapping::Long => {
            let n = json_to_i64(value).map_err(|reason| DeliveryError::Conversion {
                field: field.to_string(),
                reason,
            })?;
            Ok(Bson::Int64(n))
        }
        NumberMongoMapping::Decimal128 => {
            let s = json_to_plain_string(value);
            let dec = bson::Decimal128::from_str(&s).map_err(|err| DeliveryError::Conversion {
                field: field.to_string(),
                reason: format!("Decimal128: {err}"),
            })?;
            Ok(Bson::Decimal128(dec))
        }
        NumberMongoMapping::Unsafe => Err(DeliveryError::Conversion {
            field: field.to_string(),
            reason: "unsafe NUMBER must be mapped as string or omit before Delivery".into(),
        }),
    }
}

fn float_to_bson(field: &str, value: &Value) -> Result<Bson, DeliveryError> {
    let n = match value {
        Value::Number(num) => num.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
    .ok_or_else(|| DeliveryError::Conversion {
        field: field.to_string(),
        reason: "expected floating numeric value".into(),
    })?;
    Ok(Bson::Double(n))
}

fn datetime_to_bson(field: &str, value: &Value) -> Result<Bson, DeliveryError> {
    let raw = value.as_str().ok_or_else(|| DeliveryError::Conversion {
        field: field.to_string(),
        reason: "temporal value must be UTC ISO-8601 string".into(),
    })?;
    let dt = DateTime::parse_from_rfc3339(raw)
        .map(|d| d.with_timezone(&Utc))
        .or_else(|_| DateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%z").map(|d| d.with_timezone(&Utc)))
        .map_err(|err| DeliveryError::Conversion {
            field: field.to_string(),
            reason: format!("invalid UTC datetime: {err}"),
        })?;
    Ok(Bson::DateTime(bson::DateTime::from_millis(
        dt.timestamp_millis(),
    )))
}

fn json_to_i64(value: &Value) -> Result<i64, String> {
    match value {
        Value::Number(n) => n
            .as_i64()
            .ok_or_else(|| "NUMBER Long value does not fit i64".into()),
        Value::String(s) => s
            .parse::<i64>()
            .map_err(|err| format!("NUMBER Long parse: {err}")),
        other => Err(format!("expected integer NUMBER, got {other}")),
    }
}

fn json_to_plain_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

fn json_fallback_to_bson(field: &str, value: &Value) -> Result<Bson, DeliveryError> {
    match value {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Bson::Int64(i))
            } else if let Some(u) = n.as_u64() {
                if u <= i64::MAX as u64 {
                    Ok(Bson::Int64(u as i64))
                } else {
                    Err(DeliveryError::Conversion {
                        field: field.to_string(),
                        reason: "unsigned number exceeds i64".into(),
                    })
                }
            } else {
                // Refuse silent IEEE double for bare JSON floats without schema.
                Err(DeliveryError::Conversion {
                    field: field.to_string(),
                    reason: "refusing IEEE double without schema-driven NUMBER mapping".into(),
                })
            }
        }
        Value::Array(_) | Value::Object(_) => nested_json_to_bson(field, value),
        other => bson::to_bson(other).map_err(|err| DeliveryError::Identity(err.to_string())),
    }
}

fn nested_json_to_bson(field: &str, value: &Value) -> Result<Bson, DeliveryError> {
    match value {
        Value::Null => Ok(Bson::Null),
        Value::Bool(b) => Ok(Bson::Boolean(*b)),
        Value::String(s) => Ok(Bson::String(s.clone())),
        Value::Number(_) => json_fallback_to_bson(field, value),
        Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for (index, item) in items.iter().enumerate() {
                out.push(nested_json_to_bson(&format!("{field}[{index}]"), item)?);
            }
            Ok(Bson::Array(out))
        }
        Value::Object(map) => {
            let mut doc = Document::new();
            for (key, val) in map {
                doc.insert(key, nested_json_to_bson(&format!("{field}.{key}"), val)?);
            }
            Ok(Bson::Document(doc))
        }
    }
}

fn managed_fields_to_set_doc(doc_row: &DeliveryDocument) -> Result<Document, DeliveryError> {
    let by_name = column_map(&doc_row.columns);
    let mut set_doc = Document::new();
    for (key, value) in &doc_row.managed_fields {
        let mapping = doc_row
            .field_as
            .get(key)
            .copied()
            .unwrap_or(ManagedFieldAs::Default);
        if mapping == ManagedFieldAs::Omit {
            continue;
        }
        let col = by_name.get(key.as_str()).copied();
        set_doc.insert(key, value_to_bson(key, value, col, mapping)?);
    }
    Ok(set_doc)
}

/// Upsert Managed fields for each Output Identity.
///
/// Uses `$set` of Managed keys only so non-Managed Target fields are not cleared.
/// On insert, the document is identity (`_id`) + Managed fields only.
pub async fn upsert_managed_documents(
    target: &MongoTargetConnection,
    collection_name: &str,
    documents: &[DeliveryDocument],
) -> Result<usize, DeliveryError> {
    let coll = collection(target, collection_name).await?;
    let mut delivered = 0usize;

    for doc_row in documents {
        // Scalar Output Identity must use the matching Managed column's Oracle type
        // (e.g. unwind ORDER_ID as NUMBER), not columns.first() alphabetical luck.
        let primary_hint = match &doc_row.identity {
            Value::Object(_) => doc_row.columns.first(),
            identity_value => doc_row
                .columns
                .iter()
                .find(|col| {
                    doc_row
                        .managed_fields
                        .get(&col.name)
                        .is_some_and(|v| v == identity_value)
                })
                .or_else(|| doc_row.columns.first()),
        };
        let identity = json_identity_to_bson(&doc_row.identity, &doc_row.columns, primary_hint)?;
        let set_doc = managed_fields_to_set_doc(doc_row)?;

        let filter = doc! { "_id": identity };
        let update = doc! { "$set": set_doc };
        let options = UpdateOptions::builder().upsert(true).build();

        coll.update_one(filter, update)
            .with_options(options)
            .await
            .map_err(|err| DeliveryError::Apply(err.to_string()))?;
        delivered += 1;
    }

    Ok(delivered)
}

/// Delete entire Target documents for the given Output Identities.
///
/// When an Output Identity disappears from the platform dataset, Delivery removes
/// the whole Mongo document (ADR-0002). Missing identities are a no-op.
pub async fn delete_documents_by_identity(
    target: &MongoTargetConnection,
    collection_name: &str,
    identities: &[Value],
) -> Result<usize, DeliveryError> {
    let coll = collection(target, collection_name).await?;
    let mut deleted = 0usize;

    for identity_value in identities {
        let identity = match identity_value {
            Value::Number(n) if n.as_i64().is_some() => Bson::Int64(n.as_i64().unwrap()),
            other => bson::to_bson(other).map_err(|err| DeliveryError::Identity(err.to_string()))?,
        };
        let filter = doc! { "_id": identity };
        let result = coll
            .delete_one(filter)
            .await
            .map_err(|err| DeliveryError::Apply(err.to_string()))?;
        deleted += result.deleted_count as usize;
    }

    Ok(deleted)
}

/// Read Target documents for operator-facing inspection (CLI seam).
///
/// Emits Extended JSON so NumberLong / Decimal128 / UTC DateTime are visible.
pub async fn list_target_documents(
    target: &MongoTargetConnection,
    collection_name: &str,
) -> Result<Vec<Value>, DeliveryError> {
    let coll = collection(target, collection_name).await?;
    let mut cursor = coll
        .find(Document::new())
        .await
        .map_err(|err| DeliveryError::Apply(err.to_string()))?;

    let mut out = Vec::new();
    while cursor
        .advance()
        .await
        .map_err(|err| DeliveryError::Apply(err.to_string()))?
    {
        let mongo_doc = cursor
            .deserialize_current()
            .map_err(|err| DeliveryError::Apply(err.to_string()))?;
        // Relaxed Extended JSON keeps ints readable; dates/decimals stay typed.
        out.push(Bson::Document(mongo_doc).into_relaxed_extjson());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mongo_tls_options_none_when_disabled() {
        let tls = TlsSettings::default();
        assert!(mongo_tls_options(&tls).is_none());
    }

    #[test]
    fn mongo_tls_options_enabled_with_ca_file() {
        let tls = TlsSettings {
            enabled: true,
            ca_file: "/etc/certs/mongo-ca.pem".into(),
            wallet_location: String::new(),
            insecure_skip_verify: false,
        };
        match mongo_tls_options(&tls) {
            Some(Tls::Enabled(opts)) => {
                assert_eq!(
                    opts.ca_file_path.as_deref(),
                    Some(std::path::Path::new("/etc/certs/mongo-ca.pem"))
                );
                assert_ne!(opts.allow_invalid_certificates, Some(true));
            }
            other => panic!("expected Enabled TLS options, got {other:?}"),
        }
    }

    #[test]
    fn number_long_not_double() {
        // Construct via shared ColumnShape; NUMBER→Long mapping unchanged (ADR-0023).
        let col = DeliveryColumn {
            name: "ID".into(),
            data_type: "NUMBER".into(),
            precision: Some(10),
            scale: Some(0),
        };
        let bson = value_to_bson(
            "ID",
            &Value::from(1),
            Some(&col),
            ManagedFieldAs::Default,
        )
        .unwrap();
        assert!(matches!(bson, Bson::Int64(1)));
    }

    #[test]
    fn number_decimal128_not_double() {
        let col = DeliveryColumn {
            name: "AMOUNT".into(),
            data_type: "NUMBER".into(),
            precision: Some(12),
            scale: Some(2),
        };
        let bson = value_to_bson(
            "AMOUNT",
            &Value::String("42.50".into()),
            Some(&col),
            ManagedFieldAs::Default,
        )
        .unwrap();
        assert!(matches!(bson, Bson::Decimal128(_)));
    }

    #[test]
    fn unsafe_number_requires_string_map() {
        let col = DeliveryColumn {
            name: "HUGE".into(),
            data_type: "NUMBER".into(),
            precision: Some(38),
            scale: Some(10),
        };
        let err = value_to_bson(
            "HUGE",
            &Value::String("1".into()),
            Some(&col),
            ManagedFieldAs::Default,
        )
        .unwrap_err();
        assert!(err.to_string().contains("unsafe"));
        let ok = value_to_bson(
            "HUGE",
            &Value::String("1".into()),
            Some(&col),
            ManagedFieldAs::String,
        )
        .unwrap();
        assert_eq!(ok, Bson::String("1".into()));
    }
}
