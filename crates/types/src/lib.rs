//! Shared Connection Security, Managed-field, column metadata, NUMBER classification,
//! and Output Identity key types for the apply → Direct Delivery path.
//!
//! Config wire shapes, Platform Store persistence, Source adapters, and Target adapters
//! consume these types (or thin wire adapters from them) so TLS settings, Managed-field
//! mapping, column metadata, NUMBER→Mongo classification (ADR-0023), secret-reference
//! resolution, and Output Identity key encoding do not drift as parallel enums.

use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Canonical string key for one Output Identity value.
///
/// Poison injection matching, Drift Check reconcile, and Delivery delete/upsert
/// identity handling must all use this helper so the same logical identity always
/// serializes the same way (issue #170).
pub fn output_identity_key(identity: &serde_json::Value) -> String {
    serde_json::to_string(identity).unwrap_or_else(|_| identity.to_string())
}

/// Shared Managed / Base column metadata (issues #171 / #181 / #182).
///
/// Source adapters map engine-specific type discovery into [`ColumnShape::data_type`].
/// Runtime, Platform Store, Delivery, and transform consume this shape for Managed/Base
/// column metadata — store/delivery domain types no longer expose Oracle-named fields
/// as the default shape. Prior-release Platform Store JSON may still carry `oracle_type`
/// on read (ADR-0014 alias). Table and column layouts still come from Source schema
/// discovery for Pipeline-referenced tables — not a platform business schema catalog
/// (ADR-0026).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnShape {
    pub name: String,
    /// Source-declared type name at the shared layer (engine brand stays on adapters).
    #[serde(alias = "oracle_type")]
    pub data_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub precision: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale: Option<i32>,
}

/// Decimal128 significant digits (IEEE 754 decimal128).
pub const DECIMAL128_MAX_PRECISION: i32 = 34;

/// Signed Int64 / NumberLong digit budget that always fits.
pub const INT64_SAFE_PRECISION: i32 = 18;

/// How a declared NUMBER(p,s) maps into Mongo numeric types (ADR-0023).
///
/// Lives next to [`ColumnShape`] as the single classification home (issue #207);
/// adapter-private allow-list rules stay elsewhere (ADR-0018).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumberMongoMapping {
    /// scale 0 (or absent scale treated as integer) and precision ≤ 18 → NumberLong
    Long,
    /// precision ≤ 34 and fits Decimal128 → Decimal128 (never IEEE double)
    Decimal128,
    /// declared precision/scale cannot fit safe Mongo numeric types
    Unsafe,
}

/// Classify NUMBER(p,s) for precision-preserving Mongo mapping (ADR-0023).
///
/// Unconstrained / missing precision is unsafe — never default to IEEE double.
pub fn classify_number(precision: Option<i32>, scale: Option<i32>) -> NumberMongoMapping {
    let Some(precision) = precision else {
        return NumberMongoMapping::Unsafe;
    };
    if precision <= 0 || precision > 38 {
        return NumberMongoMapping::Unsafe;
    }
    let scale = scale.unwrap_or(0);
    if scale < 0 || scale > precision {
        return NumberMongoMapping::Unsafe;
    }
    if scale == 0 && precision <= INT64_SAFE_PRECISION {
        return NumberMongoMapping::Long;
    }
    if precision <= DECIMAL128_MAX_PRECISION {
        return NumberMongoMapping::Decimal128;
    }
    NumberMongoMapping::Unsafe
}

/// How a secret is referenced (never stored as plaintext) — ADR-0006.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SecretRefKind {
    Env,
    File,
}

/// Failed to parse a persisted secret-ref kind string.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("unknown secret ref kind: {0}")]
pub struct SecretRefParseError(pub String);

impl SecretRefKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Env => "env",
            Self::File => "file",
        }
    }

    pub fn parse(value: &str) -> Result<Self, SecretRefParseError> {
        match value {
            "env" => Ok(Self::Env),
            "file" => Ok(Self::File),
            other => Err(SecretRefParseError(other.to_string())),
        }
    }
}

/// A named reference to a secret supplied outside config/store rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SecretRef {
    pub kind: SecretRefKind,
    pub value: String,
}

impl SecretRef {
    pub fn display(&self) -> String {
        format!("{}:{}", self.kind.as_str(), self.value)
    }
}

/// Errors from resolving a [`SecretRef`] to a secret value.
///
/// Operator-visible wording matches the historical CLI apply / runtime messages so
/// contract-path twins stay stable.
#[derive(Debug, Error)]
pub enum SecretResolveError {
    #[error("unresolvable secret reference: {field} fromEnv {name} is missing")]
    MissingEnv { field: String, name: String },
    #[error("unresolvable secret reference: {field} {path}: {source}")]
    FileRead {
        field: String,
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("unresolvable secret reference: {field} {path} is empty")]
    EmptyFile { field: String, path: String },
}

/// Resolve a secret reference from the process environment or a mounted file.
///
/// This is the single resolution path for config validation and runtime apply
/// (Docker secrets are collapsed to [`SecretRefKind::File`] at the config wire).
pub fn resolve_secret_ref(reference: &SecretRef, field: &str) -> Result<String, SecretResolveError> {
    match reference.kind {
        SecretRefKind::Env => std::env::var(&reference.value).map_err(|_| {
            SecretResolveError::MissingEnv {
                field: field.to_string(),
                name: reference.value.clone(),
            }
        }),
        SecretRefKind::File => {
            let path = Path::new(&reference.value);
            let contents = fs::read_to_string(path).map_err(|source| {
                SecretResolveError::FileRead {
                    field: field.to_string(),
                    path: reference.value.clone(),
                    source,
                }
            })?;
            let trimmed = contents.trim_end_matches(['\n', '\r']).to_string();
            if trimmed.is_empty() {
                return Err(SecretResolveError::EmptyFile {
                    field: field.to_string(),
                    path: reference.value.clone(),
                });
            }
            Ok(trimmed)
        }
    }
}

/// Non-secret TLS settings for a Source or Target System connection (ADR-0017).
///
/// Paths point at mounted cert/wallet material; PEM bodies and passwords are never
/// stored here (ADR-0006). Engine crates adapt this into wire-specific client options.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct TlsSettings {
    pub enabled: bool,
    /// Filesystem path to a CA certificate (Mongo `tlsCAFile`; optional for Oracle).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ca_file: String,
    /// Oracle Instant Client wallet directory (`MY_WALLET_DIRECTORY`). Empty for Mongo.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub wallet_location: String,
    /// When true, skip certificate verification (dev/lab only; never for production).
    #[serde(default)]
    pub insecure_skip_verify: bool,
}

impl TlsSettings {
    pub fn display_summary(&self) -> String {
        if !self.enabled {
            return "tls=disabled".to_string();
        }
        let mut parts = vec!["tls=enabled".to_string()];
        if !self.ca_file.is_empty() {
            parts.push(format!("caFile={}", self.ca_file));
        }
        if !self.wallet_location.is_empty() {
            parts.push(format!("walletLocation={}", self.wallet_location));
        }
        if self.insecure_skip_verify {
            parts.push("insecureSkipVerify=true".to_string());
        }
        parts.join(" ")
    }
}

/// How a Managed field is mapped for Pipeline apply / Delivery (ADR-0023).
///
/// Config and Platform Store persist only explicit overrides (`string` / `omit`).
/// Absence from a mapping map means schema-driven [`ManagedFieldAs::Default`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ManagedFieldAs {
    /// Default schema-driven mapping.
    #[default]
    Default,
    /// Explicit string map (required for unsafe NUMBER).
    String,
    /// Remove from Managed output (not delivered).
    Omit,
}

impl ManagedFieldAs {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "default" => Some(Self::Default),
            "string" => Some(Self::String),
            "omit" => Some(Self::Omit),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::String => "string",
            Self::Omit => "omit",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn resolve_secret_ref_reads_env() {
        std::env::set_var("MIGRALOOP_TYPES_TEST_SECRET", "env-value");
        let reference = SecretRef {
            kind: SecretRefKind::Env,
            value: "MIGRALOOP_TYPES_TEST_SECRET".into(),
        };
        assert_eq!(
            resolve_secret_ref(&reference, "source.password").unwrap(),
            "env-value"
        );
        std::env::remove_var("MIGRALOOP_TYPES_TEST_SECRET");
    }

    #[test]
    fn resolve_secret_ref_missing_env_keeps_operator_wording() {
        let reference = SecretRef {
            kind: SecretRefKind::Env,
            value: "MIGRALOOP_TYPES_MISSING_ENV".into(),
        };
        let err = resolve_secret_ref(&reference, "source.password").unwrap_err();
        assert_eq!(
            err.to_string(),
            "unresolvable secret reference: source.password fromEnv MIGRALOOP_TYPES_MISSING_ENV is missing"
        );
    }

    #[test]
    fn resolve_secret_ref_reads_file_and_trims_trailing_newlines() {
        let dir = std::env::temp_dir().join(format!(
            "migraloop-types-secret-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("password");
        {
            let mut file = fs::File::create(&path).unwrap();
            write!(file, "file-secret\n").unwrap();
        }
        let reference = SecretRef {
            kind: SecretRefKind::File,
            value: path.display().to_string(),
        };
        assert_eq!(
            resolve_secret_ref(&reference, "target.password").unwrap(),
            "file-secret"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_secret_ref_rejects_empty_file() {
        let dir = std::env::temp_dir().join(format!(
            "migraloop-types-empty-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("empty");
        fs::write(&path, "\n").unwrap();
        let reference = SecretRef {
            kind: SecretRefKind::File,
            value: path.display().to_string(),
        };
        let err = resolve_secret_ref(&reference, "target.password").unwrap_err();
        assert_eq!(
            err.to_string(),
            format!(
                "unresolvable secret reference: target.password {} is empty",
                path.display()
            )
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn tls_settings_display_summary_surfaces_paths_not_pem() {
        let disabled = TlsSettings::default();
        assert_eq!(disabled.display_summary(), "tls=disabled");
        let enabled = TlsSettings {
            enabled: true,
            ca_file: "/etc/certs/ca.pem".into(),
            wallet_location: "/etc/oracle/wallet".into(),
            insecure_skip_verify: true,
        };
        let summary = enabled.display_summary();
        assert!(summary.contains("tls=enabled"));
        assert!(summary.contains("caFile=/etc/certs/ca.pem"));
        assert!(summary.contains("walletLocation=/etc/oracle/wallet"));
        assert!(summary.contains("insecureSkipVerify=true"));
        assert!(!summary.contains("BEGIN CERTIFICATE"));
    }

    #[test]
    fn managed_field_as_round_trips_persisted_overrides() {
        let raw = r#"{"AMOUNT":"string","NOTES":"omit"}"#;
        let map: std::collections::BTreeMap<String, ManagedFieldAs> =
            serde_json::from_str(raw).unwrap();
        assert_eq!(map.get("AMOUNT"), Some(&ManagedFieldAs::String));
        assert_eq!(map.get("NOTES"), Some(&ManagedFieldAs::Omit));
        assert_eq!(
            map.get("OTHER").copied().unwrap_or_default(),
            ManagedFieldAs::Default
        );
    }

    #[test]
    fn secret_ref_kind_parse_accepts_env_and_file_only() {
        assert_eq!(SecretRefKind::parse("env").unwrap(), SecretRefKind::Env);
        assert_eq!(SecretRefKind::parse("file").unwrap(), SecretRefKind::File);
        assert_eq!(
            SecretRefKind::parse("docker").unwrap_err().to_string(),
            "unknown secret ref kind: docker"
        );
    }

    #[test]
    fn output_identity_key_uses_stable_json_encoding() {
        // Independent literals — not recomputed via serde_json::to_string in the assert.
        assert_eq!(output_identity_key(&serde_json::json!(1)), "1");
        assert_eq!(output_identity_key(&serde_json::json!("CUST-1")), "\"CUST-1\"");
        assert_eq!(
            output_identity_key(&serde_json::json!({"ID": 1, "REGION": "APAC"})),
            "{\"ID\":1,\"REGION\":\"APAC\"}"
        );
    }

    #[test]
    fn column_shape_round_trips_managed_base_metadata() {
        // Independent literals — schema comes from Source discovery, not a platform catalog.
        let shape = ColumnShape {
            name: "AMOUNT".into(),
            data_type: "NUMBER".into(),
            precision: Some(10),
            scale: Some(2),
        };
        let json = serde_json::to_string(&shape).unwrap();
        assert_eq!(
            json,
            r#"{"name":"AMOUNT","data_type":"NUMBER","precision":10,"scale":2}"#
        );
        let back: ColumnShape = serde_json::from_str(&json).unwrap();
        assert_eq!(back, shape);
        assert_eq!(back.name, "AMOUNT");
        assert_eq!(back.data_type, "NUMBER");
        assert_eq!(back.precision, Some(10));
        assert_eq!(back.scale, Some(2));
    }

    #[test]
    fn column_shape_accepts_legacy_oracle_type_wire_key() {
        let legacy = r#"{"name":"ID","oracle_type":"NUMBER","precision":10,"scale":0}"#;
        let shape: ColumnShape = serde_json::from_str(legacy).unwrap();
        assert_eq!(shape.data_type, "NUMBER");
        let written = serde_json::to_string(&shape).unwrap();
        assert!(written.contains(r#""data_type":"NUMBER""#));
        assert!(!written.contains(r#""oracle_type""#));
    }

    #[test]
    fn number_classify_next_to_column_shape_never_defaults_to_double() {
        // ADR-0023 worked examples — independent literals, not recomputed from helpers.
        assert_eq!(
            classify_number(Some(10), Some(0)),
            NumberMongoMapping::Long
        );
        assert_eq!(
            classify_number(Some(12), Some(2)),
            NumberMongoMapping::Decimal128
        );
        assert_eq!(classify_number(None, None), NumberMongoMapping::Unsafe);
        assert_eq!(
            classify_number(Some(38), Some(10)),
            NumberMongoMapping::Unsafe
        );
        assert_eq!(INT64_SAFE_PRECISION, 18);
        assert_eq!(DECIMAL128_MAX_PRECISION, 34);
    }
}
