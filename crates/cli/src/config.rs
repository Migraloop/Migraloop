//! Declarative Deployment config (YAML/JSON) with secrets by reference.

use std::fs;
use std::path::Path;

use serde::Deserialize;

use crate::CliError;

const SUPPORTED_API_VERSION: &str = "migraloop.dev/v1";

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentDocument {
    pub api_version: String,
    pub kind: String,
    pub metadata: Metadata,
    pub spec: Spec,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Metadata {
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Spec {
    pub source: SystemSpec,
    pub target: SystemSpec,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SystemSpec {
    pub kind: String,
    pub host: String,
    pub port: i32,
    pub database: String,
    pub username: String,
    pub password: PasswordField,
}

/// Password must be a secret reference — never a plaintext string in config.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum PasswordField {
    FromEnv {
        #[serde(rename = "fromEnv")]
        from_env: String,
    },
    FromFile {
        #[serde(rename = "fromFile")]
        from_file: String,
    },
    /// Catch plaintext strings / unknown shapes so we can emit a clear error.
    Invalid(serde_yaml::Value),
}

pub fn load_deployment_config(path: &Path) -> Result<DeploymentDocument, CliError> {
    let raw = fs::read_to_string(path).map_err(|err| {
        CliError::Failed(format!(
            "failed to read Deployment config {}: {err}",
            path.display()
        ))
    })?;

    let doc = parse_document(path, &raw)?;
    validate_document(&doc)?;
    validate_password_fields(&doc)?;
    Ok(doc)
}

fn parse_document(path: &Path, raw: &str) -> Result<DeploymentDocument, CliError> {
    let extension = path
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();

    let result = match extension.as_str() {
        "json" => serde_json::from_str::<DeploymentDocument>(raw).map_err(|err| err.to_string()),
        "yaml" | "yml" => {
            serde_yaml::from_str::<DeploymentDocument>(raw).map_err(|err| err.to_string())
        }
        _ => {
            // Prefer JSON when it looks like an object; otherwise YAML.
            let trimmed = raw.trim_start();
            if trimmed.starts_with('{') {
                serde_json::from_str::<DeploymentDocument>(raw).map_err(|err| err.to_string())
            } else {
                serde_yaml::from_str::<DeploymentDocument>(raw).map_err(|err| err.to_string())
            }
        }
    };

    result.map_err(|err| {
        // Help operators who put plaintext passwords in config.
        if looks_like_plaintext_password_error(&err, raw) {
            CliError::Failed(
                "invalid Deployment config: password must be a secret reference \
                 (fromEnv or fromFile), not plaintext"
                    .to_string(),
            )
        } else {
            CliError::Failed(format!("invalid Deployment config: {err}"))
        }
    })
}

fn looks_like_plaintext_password_error(err: &str, raw: &str) -> bool {
    let lower = err.to_ascii_lowercase();
    let has_password_context = raw.contains("password:")
        || raw.contains("\"password\"")
        || lower.contains("password");
    has_password_context
        && (lower.contains("did not match")
            || lower.contains("invalid type")
            || lower.contains("expected")
            || lower.contains("fromenv")
            || lower.contains("fromfile"))
}

fn validate_document(doc: &DeploymentDocument) -> Result<(), CliError> {
    if doc.api_version != SUPPORTED_API_VERSION {
        return Err(CliError::Failed(format!(
            "unsupported apiVersion {:?}; expected {SUPPORTED_API_VERSION}",
            doc.api_version
        )));
    }
    if doc.kind != "Deployment" {
        return Err(CliError::Failed(format!(
            "unsupported kind {:?}; expected Deployment",
            doc.kind
        )));
    }
    if doc.metadata.name.trim().is_empty() {
        return Err(CliError::Failed(
            "Deployment metadata.name must not be empty".to_string(),
        ));
    }
    if doc.spec.source.port <= 0 || doc.spec.source.port > u16::MAX as i32 {
        return Err(CliError::Failed(
            "source.port must be a valid TCP port".to_string(),
        ));
    }
    if doc.spec.target.port <= 0 || doc.spec.target.port > u16::MAX as i32 {
        return Err(CliError::Failed(
            "target.port must be a valid TCP port".to_string(),
        ));
    }
    Ok(())
}

fn validate_password_fields(doc: &DeploymentDocument) -> Result<(), CliError> {
    validate_password(&doc.spec.source.password, "source.password")?;
    validate_password(&doc.spec.target.password, "target.password")?;
    Ok(())
}

fn validate_password(password: &PasswordField, field: &str) -> Result<(), CliError> {
    match password {
        PasswordField::FromEnv { from_env } if from_env.trim().is_empty() => Err(CliError::Failed(
            format!("{field}.fromEnv must not be empty"),
        )),
        PasswordField::FromFile { from_file } if from_file.trim().is_empty() => {
            Err(CliError::Failed(format!(
                "{field}.fromFile must not be empty"
            )))
        }
        PasswordField::FromEnv { .. } | PasswordField::FromFile { .. } => Ok(()),
        PasswordField::Invalid(value) => {
            if value.as_str().is_some() {
                Err(CliError::Failed(format!(
                    "{field} must be a secret reference (fromEnv or fromFile), not plaintext"
                )))
            } else {
                Err(CliError::Failed(format!(
                    "{field} must be a secret reference with fromEnv or fromFile"
                )))
            }
        }
    }
}

/// Resolve a secret reference from env or a mounted/Docker secret file.
/// Returns the secret value only for validation; callers must not persist it.
pub fn resolve_secret_ref(password: &PasswordField, field: &str) -> Result<String, CliError> {
    match password {
        PasswordField::FromEnv { from_env } => std::env::var(from_env).map_err(|_| {
            CliError::Failed(format!(
                "unresolvable secret reference: {field} fromEnv {from_env} is missing"
            ))
        }),
        PasswordField::FromFile { from_file } => {
            let contents = fs::read_to_string(from_file).map_err(|err| {
                CliError::Failed(format!(
                    "unresolvable secret reference: {field} fromFile {from_file}: {err}"
                ))
            })?;
            let trimmed = contents.trim_end_matches(['\n', '\r']).to_string();
            if trimmed.is_empty() {
                return Err(CliError::Failed(format!(
                    "unresolvable secret reference: {field} fromFile {from_file} is empty"
                )));
            }
            Ok(trimmed)
        }
        PasswordField::Invalid(_) => Err(CliError::Failed(format!(
            "{field} must be a secret reference (fromEnv or fromFile), not plaintext"
        ))),
    }
}
