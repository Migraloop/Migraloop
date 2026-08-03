//! Shared Connection Security and Managed-field types for the apply → Direct Delivery path.
//!
//! Config wire shapes, Platform Store persistence, Source adapters, and Target adapters
//! consume these types (or thin wire adapters from them) so TLS settings, Managed-field
//! mapping, and secret-reference resolution do not drift as parallel enums.

use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

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

/// Expand-contract leftover alias for persisted Managed-field overrides.
///
/// Prefer [`ManagedFieldAs`] on the apply → Direct Delivery path. This alias exists
/// only so older call sites can migrate without a second source of truth.
#[deprecated(note = "use ManagedFieldAs — temporary expand-contract leftover")]
pub type FieldMappingAs = ManagedFieldAs;

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
}
