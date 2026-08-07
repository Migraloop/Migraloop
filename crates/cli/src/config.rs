//! Declarative Deployment config (YAML/JSON) with secrets by reference.

use std::fs;
use std::path::{Path, PathBuf};

use migraloop_types::{resolve_secret_ref, SecretRef, SecretRefKind, TlsSettings};
use serde::Deserialize;

use crate::CliError;

/// Canonical apiVersion for the current config major line.
const SUPPORTED_API_VERSION: &str = "migraloop.dev/v1";
/// Current config SemVer this binary authors/understands (ADR-0014).
/// Older same-major forms (`v1.0`, `v1.0.0`) still apply; newer minors/patches do not.
const CURRENT_CONFIG_VERSION: (u64, u64, u64) = (1, 0, 0);
const API_VERSION_PREFIX: &str = "migraloop.dev/v";
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
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct PipelineSpec {
    pub name: String,
    pub mode: String,
    /// Optional Operator-facing description (metadata-only; does not rebuild Derived).
    #[serde(default)]
    pub description: Option<String>,
    pub source: PipelineSourceSpec,
    /// Target Binding for Delivery. Optional so Deployment/Base-only apply still works.
    #[serde(default)]
    pub target: Option<PipelineTargetSpec>,
    /// Managed-field mapping overrides for unsafe NUMBER / omit (ADR-0023).
    /// Example: `fields: { HUGE_AMOUNT: { as: string } }`
    #[serde(default)]
    pub fields: std::collections::BTreeMap<String, FieldMappingSpec>,
    /// Required Output Identity field names for Transform Pipelines.
    #[serde(default)]
    pub output_identity: Option<Vec<String>>,
    /// Declarative Rich Transform steps: Aggregation `$…` stages only
    /// (`$project`, `$match`, `$group`, …); classic / SQL-ish aliases are rejected (ADR-0030).
    /// Rejected for Direct.
    #[serde(default)]
    pub transform: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum FieldMappingAsSpec {
    String,
    Omit,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldMappingSpec {
    /// `string` (map to string) or `omit` (remove from Managed output).
    #[serde(rename = "as")]
    pub map_as: FieldMappingAsSpec,
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
    /// IANA name or Oracle-style offset (`±HH:MM`) for naive DATE/TIMESTAMP when
    /// Source DB timezone is unreadable.
    #[serde(default)]
    pub timezone: Option<String>,
    /// Optional TLS settings (ADR-0017). Omitted / disabled keeps cleartext allowed.
    #[serde(default)]
    pub tls: Option<TlsSpec>,
}

/// Operator-facing TLS block on `spec.source` / `spec.target`.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase", default)]
pub struct TlsSpec {
    /// When true, the product path must establish TLS (no silent cleartext fallback).
    pub enabled: bool,
    /// Path to a CA certificate file (Mongo). Optional for Oracle when using a wallet.
    pub ca_file: Option<String>,
    /// Oracle Instant Client wallet directory. Invalid on Mongo Target.
    pub wallet_location: Option<String>,
    /// Skip certificate verification (dev/lab only). Default false.
    pub insecure_skip_verify: bool,
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
    ///
    /// Uses the shared [`resolve_secret_ref`] path so config parse and runtime apply
    /// do not fork PasswordField / SecretRef logic.
    pub fn resolve(&self, field: &str) -> Result<String, CliError> {
        let reference = secret_ref_from_resolved(self.resolved_ref(field)?);
        resolve_secret_ref(&reference, field).map_err(|err| CliError::Failed(err.to_string()))
    }
}

/// Collapse config wire resolution (`ResolvedSecretRef`) into the shared [`SecretRef`].
///
/// Docker secrets are already file paths here; store persistence never sees a third kind.
pub fn secret_ref_from_resolved(resolved: ResolvedSecretRef) -> SecretRef {
    match resolved {
        ResolvedSecretRef::Env(name) => SecretRef {
            kind: SecretRefKind::Env,
            value: name,
        },
        ResolvedSecretRef::File(path) => SecretRef {
            kind: SecretRefKind::File,
            value: path.display().to_string(),
        },
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
    validate_source_timezone(doc.spec.source.timezone.as_deref())?;
    doc.spec.source.password.validate("source.password")?;
    doc.spec.target.password.validate("target.password")?;
    validate_tls_spec("source", &doc.spec.source)?;
    validate_tls_spec("target", &doc.spec.target)?;
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
    // Only rewrite when the parse error itself is about password — not every
    // unknown-field failure in a document that happens to contain password refs.
    let error_about_password = lower.contains("password");
    let has_password_context = raw.contains("password:")
        || raw.contains("\"password\"")
        || error_about_password;
    error_about_password
        && has_password_context
        && (lower.contains("did not match")
            || lower.contains("invalid type")
            || lower.contains("expected")
            || lower.contains("fromenv")
            || lower.contains("fromfile")
            || lower.contains("fromdockersecret")
            || lower.contains("unknown field"))
}

/// Parse `migraloop.dev/v{major}` / `v{major}.{minor}` / `v{major}.{minor}.{patch}`.
fn parse_api_version_semver(api_version: &str) -> Result<(u64, u64, u64), CliError> {
    let Some(rest) = api_version.strip_prefix(API_VERSION_PREFIX) else {
        return Err(CliError::Failed(format!(
            "unsupported apiVersion {api_version:?}; expected SemVer-compatible \
             {SUPPORTED_API_VERSION} (also accepts migraloop.dev/v1.0 or migraloop.dev/v1.0.0)"
        )));
    };
    if rest.is_empty() {
        return Err(CliError::Failed(format!(
            "unsupported apiVersion {api_version:?}; missing SemVer after {API_VERSION_PREFIX}"
        )));
    }
    let parts: Vec<&str> = rest.split('.').collect();
    if parts.len() > 3 {
        return Err(CliError::Failed(format!(
            "unsupported apiVersion {api_version:?}; expected SemVer-compatible \
             {SUPPORTED_API_VERSION} (major[.minor[.patch]])"
        )));
    }
    let mut nums = [0u64; 3];
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() || !part.chars().all(|c| c.is_ascii_digit()) {
            return Err(CliError::Failed(format!(
                "unsupported apiVersion {api_version:?}; SemVer components must be non-negative integers"
            )));
        }
        // Reject leading zeros like "01" except a lone "0".
        if part.len() > 1 && part.starts_with('0') {
            return Err(CliError::Failed(format!(
                "unsupported apiVersion {api_version:?}; SemVer components must not have leading zeros"
            )));
        }
        nums[i] = part.parse().map_err(|_| {
            CliError::Failed(format!(
                "unsupported apiVersion {api_version:?}; invalid SemVer component"
            ))
        })?;
    }
    Ok((nums[0], nums[1], nums[2]))
}

fn validate_api_version(api_version: &str) -> Result<(), CliError> {
    let (major, minor, patch) = parse_api_version_semver(api_version)?;
    let (cur_major, cur_minor, cur_patch) = CURRENT_CONFIG_VERSION;
    if major != cur_major {
        return Err(CliError::Failed(format!(
            "unsupported apiVersion {api_version:?}; this app accepts SemVer-compatible \
             major {cur_major} ({SUPPORTED_API_VERSION}, migraloop.dev/v1.0, \
             migraloop.dev/v1.0.0) — incompatible major requires an explicit upgrade path"
        )));
    }
    // Older-or-equal within the current major only (not forward-compat with newer apps).
    let newer_than_this_app =
        (minor, patch) > (cur_minor, cur_patch);
    if newer_than_this_app {
        return Err(CliError::Failed(format!(
            "unsupported apiVersion {api_version:?}; this app reads older-or-equal \
             SemVer within major {cur_major} up to {cur_major}.{cur_minor}.{cur_patch} \
             (canonical {SUPPORTED_API_VERSION})"
        )));
    }
    Ok(())
}

fn validate_document(doc: &DeploymentDocument) -> Result<(), CliError> {
    validate_api_version(&doc.api_version)?;
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
    if pipeline.mode != "direct" && pipeline.mode != "transform" {
        return Err(CliError::Failed(format!(
            "unsupported pipeline.mode {:?}; expected \"direct\" or \"transform\"",
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
    for (field, _mapping) in &pipeline.fields {
        if field.trim().is_empty() {
            return Err(CliError::Failed(
                "pipeline.fields keys must not be empty".to_string(),
            ));
        }
    }

    if pipeline.mode == "direct" {
        if pipeline.transform.is_some() {
            return Err(CliError::Failed(
                "Direct Pipeline must not declare transform; use mode: transform".to_string(),
            ));
        }
        // Direct Output Identity defaults from the source primary key; ignore any
        // explicit outputIdentity rather than failing apply.
        return Ok(());
    }

    // Transform Pipeline: Output Identity required; declarative analyzable ops only.
    match &pipeline.output_identity {
        None => {
            return Err(CliError::Failed(format!(
                "Transform Pipeline {} requires outputIdentity before it can run",
                pipeline.name
            )));
        }
        Some(identity) if identity.is_empty() => {
            return Err(CliError::Failed(format!(
                "Transform Pipeline {} requires a non-empty outputIdentity",
                pipeline.name
            )));
        }
        Some(identity) => {
            for field in identity {
                if field.trim().is_empty() {
                    return Err(CliError::Failed(format!(
                        "Transform Pipeline {} outputIdentity entries must not be empty",
                        pipeline.name
                    )));
                }
            }
        }
    }

    let Some(steps) = &pipeline.transform else {
        return Err(CliError::Failed(format!(
            "Transform Pipeline {} requires a declarative transform",
            pipeline.name
        )));
    };
    if steps.is_empty() {
        return Err(CliError::Failed(format!(
            "Transform Pipeline {} transform must declare at least one operator",
            pipeline.name
        )));
    }

    validate_transform_steps(pipeline, steps)?;

    if pipeline.target.is_none() {
        return Err(CliError::Failed(format!(
            "Transform Pipeline {} requires target.collection for Delivery",
            pipeline.name
        )));
    }

    Ok(())
}

fn validate_transform_steps(
    pipeline: &PipelineSpec,
    steps: &[serde_json::Value],
) -> Result<(), CliError> {
    migraloop_transform::parse_transform_steps(steps).map_err(|err| {
        CliError::Failed(format!("Transform Pipeline {}: {err}", pipeline.name))
    })?;
    Ok(())
}

/// Validate source.timezone when present (IANA name or Oracle-style `±HH:MM`).
pub fn validate_source_timezone(timezone: Option<&str>) -> Result<(), CliError> {
    let Some(tz) = timezone.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(());
    };
    // Same acceptance rules as Capture temporal resolution (ADR-0022 / handbook).
    if migraloop_capture::resolve_temporal_timezone(None, Some(tz)).is_err() {
        return Err(CliError::Failed(format!(
            "source.timezone {tz:?} is not a valid IANA timezone or Oracle-style offset (±HH:MM)"
        )));
    }
    Ok(())
}

fn validate_tls_spec(system: &str, spec: &SystemSpec) -> Result<(), CliError> {
    let Some(tls) = &spec.tls else {
        return Ok(());
    };
    if let Some(ca) = tls.ca_file.as_deref() {
        if ca.trim().is_empty() {
            return Err(CliError::Failed(format!(
                "{system}.tls.caFile must not be empty when set"
            )));
        }
        if system == "source" || spec.kind == V1_SOURCE_KIND {
            return Err(CliError::Failed(
                "source.tls.caFile is not used for Oracle TCPS; set \
                 source.tls.walletLocation to an Instant Client wallet directory \
                 (MongoDB Target uses tls.caFile)"
                    .to_string(),
            ));
        }
    }
    if let Some(wallet) = tls.wallet_location.as_deref() {
        if wallet.trim().is_empty() {
            return Err(CliError::Failed(format!(
                "{system}.tls.walletLocation must not be empty when set"
            )));
        }
        if system == "target" || spec.kind == V1_TARGET_KIND {
            return Err(CliError::Failed(
                "target.tls.walletLocation is only valid for Oracle Source; \
                 MongoDB Target uses tls.caFile"
                    .to_string(),
            ));
        }
    }
    Ok(())
}

/// Resolve TLS settings from config wire (`TlsSpec`) into the shared [`TlsSettings`].
pub fn resolve_tls_settings(
    field_prefix: &str,
    tls: Option<&TlsSpec>,
) -> Result<TlsSettings, CliError> {
    let Some(tls) = tls else {
        return Ok(TlsSettings::default());
    };
    let ca_file = tls
        .ca_file
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("")
        .to_string();
    let wallet_location = tls
        .wallet_location
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("")
        .to_string();

    if tls.enabled {
        if !ca_file.is_empty() {
            let path = Path::new(&ca_file);
            if !path.is_file() {
                return Err(CliError::Failed(format!(
                    "{field_prefix}.tls.caFile {ca_file:?} does not exist or is not a file; \
                     TLS was requested and will not silently fall back to cleartext"
                )));
            }
        }
        if !wallet_location.is_empty() {
            let path = Path::new(&wallet_location);
            if !path.is_dir() {
                return Err(CliError::Failed(format!(
                    "{field_prefix}.tls.walletLocation {wallet_location:?} does not exist \
                     or is not a directory; TLS was requested and will not silently \
                     fall back to cleartext"
                )));
            }
        }
    }

    Ok(TlsSettings {
        enabled: tls.enabled,
        ca_file,
        wallet_location,
        insecure_skip_verify: tls.insecure_skip_verify,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_timezone_accepts_iana_name() {
        validate_source_timezone(Some("Asia/Taipei")).expect("IANA name");
    }

    #[test]
    fn source_timezone_accepts_oracle_style_offset() {
        validate_source_timezone(Some("+09:00")).expect("Oracle-style offset");
        validate_source_timezone(Some("-05:00")).expect("negative offset");
    }

    #[test]
    fn source_timezone_rejects_neither_iana_nor_offset() {
        let err = validate_source_timezone(Some("Not/AZone"))
            .expect_err("invalid timezone must fail apply");
        let msg = err.to_string();
        assert!(
            msg.contains("source.timezone \"Not/AZone\""),
            "error should name the field and value, got: {msg}"
        );
        assert!(
            msg.contains("IANA") && msg.contains("offset"),
            "error should explain accepted forms, got: {msg}"
        );
    }

    #[test]
    fn source_timezone_empty_or_absent_ok() {
        validate_source_timezone(None).expect("absent");
        validate_source_timezone(Some("")).expect("empty");
        validate_source_timezone(Some("   ")).expect("whitespace");
    }
}
