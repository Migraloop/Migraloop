//! Declarative Deployment config (YAML/JSON) with secrets by reference.

use std::fs;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::CliError;

const SUPPORTED_API_VERSION: &str = "migraloop.dev/v1";
const V1_SOURCE_KIND: &str = "oracle";
const V1_TARGET_KIND: &str = "mongodb";
const DOCKER_SECRETS_DIR: &str = "/run/secrets";

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeploymentDocument {
    pub api_version: String,
    pub kind: String,
    pub metadata: Metadata,
    pub spec: Spec,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    pub name: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub source: SystemSpec,
    pub target: SystemSpec,
    /// Pipelines hosted by this Deployment. Empty means Deployment-only apply.
    #[serde(default)]
    pub pipelines: Vec<PipelineSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineSpec {
    pub name: String,
    pub mode: String,
    pub source: PipelineSourceSpec,
    /// Target Binding for Delivery. Optional so Deployment/Base-only apply still works.
    #[serde(default)]
    pub target: Option<PipelineTargetSpec>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineSourceSpec {
    pub table: String,
    #[serde(default)]
    pub schema: Option<String>,
}

/// Target Binding: which Target collection receives Managed-field Delivery.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineTargetSpec {
    pub collection: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
    Ref(PasswordRef),
    /// Catch plaintext strings / unknown shapes so we can emit a clear error.
    Invalid(serde_yaml::Value),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PasswordRef {
    #[serde(rename = "fromEnv")]
    from_env: Option<String>,
    #[serde(rename = "fromFile")]
    from_file: Option<String>,
    /// Docker secret name; resolved from `/run/secrets/<name>` (mounted Docker secrets).
    #[serde(rename = "fromDockerSecret")]
    from_docker_secret: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedSecretRef {
    Env(String),
    File(PathBuf),
}

impl PasswordField {
    pub fn validate(&self, field: &str) -> Result<(), CliError> {
        match self {
            Self::Ref(reference) => reference.validate(field),
            Self::Invalid(value) => {
                if value.as_str().is_some() {
                    Err(CliError::Failed(format!(
                        "{field} must be a secret reference \
                         (fromEnv, fromFile, or fromDockerSecret), not plaintext"
                    )))
                } else {
                    Err(CliError::Failed(format!(
                        "{field} must be a secret reference with exactly one of \
                         fromEnv, fromFile, or fromDockerSecret"
                    )))
                }
            }
        }
    }

    pub fn resolved_ref(&self, field: &str) -> Result<ResolvedSecretRef, CliError> {
        match self {
            Self::Ref(reference) => reference.resolved_ref(field),
            Self::Invalid(_) => Err(CliError::Failed(format!(
                "{field} must be a secret reference \
                 (fromEnv, fromFile, or fromDockerSecret), not plaintext"
            ))),
        }
    }

    /// Resolve a secret reference from env, a mounted file, or a Docker secret.
    /// Returns the secret value only for validation; callers must not persist it.
    pub fn resolve(&self, field: &str) -> Result<String, CliError> {
        match self.resolved_ref(field)? {
            ResolvedSecretRef::Env(name) => std::env::var(&name).map_err(|_| {
                CliError::Failed(format!(
                    "unresolvable secret reference: {field} fromEnv {name} is missing"
                ))
            }),
            ResolvedSecretRef::File(path) => {
                let contents = fs::read_to_string(&path).map_err(|err| {
                    CliError::Failed(format!(
                        "unresolvable secret reference: {field} {}: {err}",
                        path.display()
                    ))
                })?;
                let trimmed = contents.trim_end_matches(['\n', '\r']).to_string();
                if trimmed.is_empty() {
                    return Err(CliError::Failed(format!(
                        "unresolvable secret reference: {field} {} is empty",
                        path.display()
                    )));
                }
                Ok(trimmed)
            }
        }
    }
}

impl PasswordRef {
    fn present_keys(&self) -> Vec<&'static str> {
        let mut keys = Vec::new();
        if self.from_env.is_some() {
            keys.push("fromEnv");
        }
        if self.from_file.is_some() {
            keys.push("fromFile");
        }
        if self.from_docker_secret.is_some() {
            keys.push("fromDockerSecret");
        }
        keys
    }

    fn validate(&self, field: &str) -> Result<(), CliError> {
        let keys = self.present_keys();
        if keys.len() != 1 {
            return Err(CliError::Failed(format!(
                "{field} must set exactly one of fromEnv, fromFile, or fromDockerSecret"
            )));
        }
        match (
            self.from_env.as_deref(),
            self.from_file.as_deref(),
            self.from_docker_secret.as_deref(),
        ) {
            (Some(name), None, None) if name.trim().is_empty() => Err(CliError::Failed(format!(
                "{field}.fromEnv must not be empty"
            ))),
            (None, Some(path), None) if path.trim().is_empty() => Err(CliError::Failed(format!(
                "{field}.fromFile must not be empty"
            ))),
            (None, None, Some(name)) if name.trim().is_empty() => Err(CliError::Failed(format!(
                "{field}.fromDockerSecret must not be empty"
            ))),
            (Some(_), None, None) | (None, Some(_), None) | (None, None, Some(_)) => Ok(()),
            _ => Err(CliError::Failed(format!(
                "{field} must set exactly one of fromEnv, fromFile, or fromDockerSecret"
            ))),
        }
    }

    fn resolved_ref(&self, field: &str) -> Result<ResolvedSecretRef, CliError> {
        self.validate(field)?;
        if let Some(name) = &self.from_env {
            return Ok(ResolvedSecretRef::Env(name.clone()));
        }
        if let Some(path) = &self.from_file {
            return Ok(ResolvedSecretRef::File(PathBuf::from(path)));
        }
        if let Some(name) = &self.from_docker_secret {
            return Ok(ResolvedSecretRef::File(
                Path::new(DOCKER_SECRETS_DIR).join(name),
            ));
        }
        Err(CliError::Failed(format!(
            "{field} must set exactly one of fromEnv, fromFile, or fromDockerSecret"
        )))
    }
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
    doc.spec.source.password.validate("source.password")?;
    doc.spec.target.password.validate("target.password")?;
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
                 (fromEnv, fromFile, or fromDockerSecret), not plaintext"
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
            || lower.contains("fromfile")
            || lower.contains("fromdockersecret")
            || lower.contains("unknown field"))
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
    if doc.spec.source.kind != V1_SOURCE_KIND {
        return Err(CliError::Failed(format!(
            "v1 source.kind must be {V1_SOURCE_KIND}, got {:?}",
            doc.spec.source.kind
        )));
    }
    if doc.spec.target.kind != V1_TARGET_KIND {
        return Err(CliError::Failed(format!(
            "v1 target.kind must be {V1_TARGET_KIND}, got {:?}",
            doc.spec.target.kind
        )));
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
    for pipeline in &doc.spec.pipelines {
        validate_pipeline(pipeline)?;
    }
    Ok(())
}

fn validate_pipeline(pipeline: &PipelineSpec) -> Result<(), CliError> {
    if pipeline.name.trim().is_empty() {
        return Err(CliError::Failed(
            "pipeline.name must not be empty".to_string(),
        ));
    }
    if pipeline.mode != "direct" {
        return Err(CliError::Failed(format!(
            "unsupported pipeline.mode {:?}; v1 Initial Load slice supports only \"direct\"",
            pipeline.mode
        )));
    }
    if pipeline.source.table.trim().is_empty() {
        return Err(CliError::Failed(
            "pipeline.source.table must not be empty".to_string(),
        ));
    }
    if let Some(target) = &pipeline.target {
        if target.collection.trim().is_empty() {
            return Err(CliError::Failed(
                "pipeline.target.collection must not be empty".to_string(),
            ));
        }
    }
    Ok(())
}
