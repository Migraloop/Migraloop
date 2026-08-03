//! Platform Store: dedicated PostgreSQL data plane for the platform.

mod guardrails;

pub use guardrails::{
    check_store_settings, disk_warn_message, probe_store_resources, probe_store_settings,
    GuardrailError, PlatformStoreResourceStatus, PlatformStoreSettings, DISK_FREE_WARN_BYTES,
    MIN_MAINTENANCE_WORK_MEM_BYTES, MIN_MAX_CONNECTIONS, MIN_SHARED_BUFFERS_BYTES,
    MIN_WORK_MEM_BYTES,
};

use std::borrow::Cow;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use thiserror::Error;

/// Prior-release Platform Store schema cut for upgrade-smoke verification.
///
/// Migrations `1..=4` cover bootstrap through Delivery binding — a store shape
/// that pre-dates Incremental Sync Health / lag / quarantine columns. Newer apps
/// must migrate forward from this cut without wiping Deployment data (ADR-0014).
pub const PRIOR_RELEASE_SCHEMA_VERSION: i64 = 4;

#[derive(Debug, Error)]
pub enum PlatformStoreError {
    #[error("failed to connect to Platform Store: {0}")]
    Connect(#[source] sqlx::Error),
    #[error("failed to migrate Platform Store: {0}")]
    Migrate(#[source] sqlx::migrate::MigrateError),
    #[error("unknown Platform Store migration version: {0}")]
    UnknownMigrationVersion(i64),
    #[error("failed to persist Deployment: {0}")]
    Persist(#[source] sqlx::Error),
    #[error("failed to load Deployments: {0}")]
    Load(#[source] sqlx::Error),
    #[error("{0}")]
    NotFound(String),
    #[error("invalid stored JSON: {0}")]
    InvalidJson(#[source] serde_json::Error),
}

/// How a secret is referenced (never stored as plaintext).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SecretRefKind {
    Env,
    File,
}

impl SecretRefKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Env => "env",
            Self::File => "file",
        }
    }

    pub fn parse(value: &str) -> Result<Self, PlatformStoreError> {
        match value {
            "env" => Ok(Self::Env),
            "file" => Ok(Self::File),
            other => Err(PlatformStoreError::Load(sqlx::Error::Protocol(format!(
                "unknown secret ref kind: {other}"
            )))),
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

/// Non-secret TLS settings for a Source or Target System connection (ADR-0017).
///
/// Paths point at mounted cert/wallet material; PEM bodies and passwords are never
/// stored here (ADR-0006).
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

/// Non-secret connection configuration for a Source or Target System.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemConnection {
    pub kind: String,
    pub host: String,
    pub port: i32,
    pub database: String,
    pub username: String,
    pub password_ref: SecretRef,
    /// IANA timezone for naive DATE/TIMESTAMP when Source DB timezone is unreadable.
    /// Empty means unset (ADR-0022).
    #[serde(default)]
    pub timezone: String,
    /// TLS mode and non-secret cert/wallet paths (ADR-0017).
    #[serde(default)]
    pub tls: TlsSettings,
}

/// Durable Deployment pairing one Source System with one Target System.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deployment {
    pub name: String,
    pub source: SystemConnection,
    pub target: SystemConnection,
}

/// Explicit Managed-field mapping override for Pipeline apply (ADR-0023).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldMappingAs {
    String,
    Omit,
}

impl FieldMappingAs {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Omit => "omit",
        }
    }
}

/// A Pipeline declared inside a Deployment.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Pipeline {
    pub deployment_name: String,
    pub name: String,
    pub mode: String,
    pub source_table: String,
    pub source_schema: String,
    /// Target collection for Delivery; empty means Delivery not configured.
    pub target_collection: String,
    /// Operator-visible Delivery progress: not_configured | pending | delivered.
    pub delivery_status: String,
    /// Count of Output Identity Delivery applies (upserts + deletes) for progress.
    pub delivery_applied_changes: i32,
    /// Remaining pending Delivery work in the current Incremental window (ADR-0020).
    #[serde(default)]
    pub delivery_lag: i32,
    /// Durable Operator pause (ADR-0007): when true, skip Delivery/processing.
    #[serde(default)]
    pub paused: bool,
    /// Optional Operator-facing description (metadata-only; ADR-0007 / issue #21).
    #[serde(default)]
    pub description: String,
    /// Per-field Managed mapping overrides (`string` / `omit`) keyed by column name.
    #[serde(default)]
    pub field_mappings: std::collections::BTreeMap<String, FieldMappingAs>,
    /// Transform Pipeline Output Identity field names (empty for Direct).
    #[serde(default)]
    pub output_identity: Vec<String>,
    /// Declarative Rich Transform JSON (null/empty for Direct).
    #[serde(default)]
    pub transform_json: Option<serde_json::Value>,
    /// Drift Check result: unknown | ok | partial (issue #25).
    /// `partial` means the last check was resource-gated (budget truncated).
    #[serde(default = "default_drift_unknown")]
    pub drift_status: String,
    /// Output Identities compared against Target in the last Drift Check.
    #[serde(default)]
    pub drift_checked_rows: i32,
    /// Managed-field mismatches detected in the last Drift Check (before repair).
    #[serde(default)]
    pub drift_mismatched_rows: i32,
}

fn default_drift_unknown() -> String {
    "unknown".to_string()
}

/// Platform-managed Derived Dataset for one Transform Pipeline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedDataset {
    pub deployment_name: String,
    pub pipeline_name: String,
    pub status: String,
    pub output_identity: Vec<String>,
    pub columns: Vec<BaseColumn>,
    pub row_count: i32,
}

/// One row stored in a Derived Dataset.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DerivedRow {
    pub row_ordinal: i32,
    pub data: serde_json::Map<String, serde_json::Value>,
}

/// Supported column kept in a Base Dataset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseColumn {
    pub name: String,
    pub oracle_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub precision: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale: Option<i32>,
}

/// Unsupported Source column omitted from the Base Dataset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OmittedColumn {
    pub name: String,
    pub oracle_type: String,
}

/// Platform-managed Base Dataset for one Source table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseDataset {
    pub deployment_name: String,
    pub source_table: String,
    pub source_schema: String,
    pub status: String,
    /// Source primary-key column names used as Direct Pipeline Output Identity.
    pub primary_key: Vec<String>,
    pub columns: Vec<BaseColumn>,
    pub omitted_columns: Vec<OmittedColumn>,
    pub row_count: i32,
    /// Count of Incremental Capture changes applied into this Base Dataset.
    pub sync_applied_changes: i32,
    /// Operator-visible Sync Health for this Base: unknown | ok.
    pub sync_health: String,
    /// Low-watermark capture position established before Initial Load (ADR-0004).
    /// Required before Incremental Capture may start.
    pub capture_low_watermark: Option<i64>,
    /// Highest capture position successfully applied into this Base (checkpoint).
    pub capture_checkpoint: Option<i64>,
    /// Remaining unapplied Incremental Capture changes after the last sync (lag).
    /// Durable so status after process restart stays coherent without local-only state.
    pub sync_lag: i32,
    /// Source Alignment Check result: unknown | aligned | partial (issue #24).
    /// `partial` means the last check was resource-gated (budget truncated).
    pub source_alignment: String,
    /// Rows compared against Source in the last Source Alignment Check.
    pub source_alignment_checked_rows: i32,
    /// Mismatches detected in the last Source Alignment Check (before repair).
    pub source_alignment_mismatched_rows: i32,
    /// Keyset cursor for resumable chunked Initial Load (issue #124).
    /// JSON array of last-persisted primary-key values; `None` when complete/idle.
    pub initial_load_cursor: Option<Vec<serde_json::Value>>,
}

/// One row stored in a Base Dataset (supported columns only).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BaseRow {
    pub row_ordinal: i32,
    pub data: serde_json::Map<String, serde_json::Value>,
}

/// Health of the Platform Store as observed by operators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlatformStoreHealth {
    Healthy { schema_version: i64 },
    Unhealthy { reason: String },
    Unreachable { reason: String },
}

pub(crate) async fn connect(database_url: &str) -> Result<PgPool, PlatformStoreError> {
    PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(3))
        .connect(database_url)
        .await
        .map_err(|err| map_store_connect_error(database_url, err))
}

/// True when the Platform Store URL explicitly requests TLS (no cleartext fallback).
pub fn platform_store_url_requires_tls(database_url: &str) -> bool {
    database_url
        .split(['?', '&'])
        .skip(1)
        .filter_map(|pair| pair.split_once('='))
        .any(|(key, value)| {
            key.eq_ignore_ascii_case("sslmode")
                && matches!(
                    value.to_ascii_lowercase().as_str(),
                    "require" | "verify-ca" | "verify-full"
                )
        })
}

fn map_store_connect_error(database_url: &str, err: sqlx::Error) -> PlatformStoreError {
    let detail = err.to_string();
    if platform_store_url_requires_tls(database_url) {
        // Keep the underlying cause (handshake, auth, DNS, …) but make the
        // required-TLS contract visible — no silent cleartext fallback.
        PlatformStoreError::Connect(sqlx::Error::Protocol(format!(
            "Platform Store URL requires TLS (sslmode=require|verify-ca|verify-full); \
             connection failed with no cleartext fallback: {detail}"
        )))
    } else {
        PlatformStoreError::Connect(err)
    }
}

/// Embedded Platform Store migrator (all versioned schema migrations).
fn store_migrator() -> sqlx::migrate::Migrator {
    sqlx::migrate!("./migrations")
}

/// Latest schema migration version shipped with this app binary.
pub fn latest_migration_version() -> i64 {
    store_migrator()
        .iter()
        .map(|m| m.version)
        .max()
        .unwrap_or(0)
}

/// Apply versioned Platform Store schema migrations.
pub async fn migrate(database_url: &str) -> Result<(), PlatformStoreError> {
    let pool = connect(database_url).await?;
    store_migrator()
        .run(&pool)
        .await
        .map_err(PlatformStoreError::Migrate)?;
    Ok(())
}

/// Apply only migrations with version `<= through_version` (inclusive).
///
/// Upgrade-smoke / CI helper for seeding a prior-release Platform Store schema.
/// Production operators use [`migrate`] (or `migraloop run` / `migraloop migrate`),
/// which always applies every pending migration.
#[doc(hidden)]
pub async fn migrate_through(
    database_url: &str,
    through_version: i64,
) -> Result<(), PlatformStoreError> {
    let full = store_migrator();
    if !full.version_exists(through_version) {
        return Err(PlatformStoreError::UnknownMigrationVersion(through_version));
    }

    let subset: Vec<_> = full
        .iter()
        .filter(|m| m.version <= through_version)
        .cloned()
        .collect();
    let partial = sqlx::migrate::Migrator {
        migrations: Cow::Owned(subset),
        ignore_missing: full.ignore_missing,
        locking: full.locking,
        no_tx: full.no_tx,
    };

    let pool = connect(database_url).await?;
    partial
        .run(&pool)
        .await
        .map_err(PlatformStoreError::Migrate)?;
    Ok(())
}

/// Check whether the Platform Store is reachable and migrated.
pub async fn health(database_url: &str) -> PlatformStoreHealth {
    let pool = match connect(database_url).await {
        Ok(pool) => pool,
        Err(err) => {
            return PlatformStoreHealth::Unreachable {
                reason: err.to_string(),
            };
        }
    };

    if let Err(err) = sqlx::query("SELECT 1").execute(&pool).await {
        return PlatformStoreHealth::Unreachable {
            reason: err.to_string(),
        };
    }

    let version = sqlx::query_scalar::<_, i64>(
        "SELECT version FROM _sqlx_migrations WHERE success = true ORDER BY version DESC LIMIT 1",
    )
    .fetch_optional(&pool)
    .await;

    match version {
        Ok(Some(schema_version)) => PlatformStoreHealth::Healthy { schema_version },
        Ok(None) => PlatformStoreHealth::Unhealthy {
            reason: "schema migrations have not been applied".to_string(),
        },
        Err(_) => PlatformStoreHealth::Unhealthy {
            reason: "schema migrations have not been applied".to_string(),
        },
    }
}

/// Delete a Deployment and cascaded Platform Store state (Pipelines, Bases, Derived).
///
/// Idempotent: missing Deployments are a no-op success so Lab Namespace cleanup
/// and re-run wipe can call this unconditionally.
pub async fn delete_deployment(
    database_url: &str,
    deployment_name: &str,
) -> Result<(), PlatformStoreError> {
    let pool = connect(database_url).await?;
    sqlx::query("DELETE FROM deployments WHERE name = $1")
        .bind(deployment_name)
        .execute(&pool)
        .await
        .map_err(PlatformStoreError::Persist)?;
    Ok(())
}

/// Create or update a Deployment. Secrets are stored only as references.
pub async fn upsert_deployment(
    database_url: &str,
    deployment: &Deployment,
) -> Result<(), PlatformStoreError> {
    let pool = connect(database_url).await?;
    sqlx::query(
        r#"
        INSERT INTO deployments (
            name,
            source_kind, source_host, source_port, source_database, source_username,
            source_password_ref_kind, source_password_ref_value, source_timezone,
            source_tls_json,
            target_kind, target_host, target_port, target_database, target_username,
            target_password_ref_kind, target_password_ref_value,
            target_tls_json,
            applied_at
        ) VALUES (
            $1,
            $2, $3, $4, $5, $6,
            $7, $8, $9,
            $10,
            $11, $12, $13, $14, $15,
            $16, $17,
            $18,
            now()
        )
        ON CONFLICT (name) DO UPDATE SET
            source_kind = EXCLUDED.source_kind,
            source_host = EXCLUDED.source_host,
            source_port = EXCLUDED.source_port,
            source_database = EXCLUDED.source_database,
            source_username = EXCLUDED.source_username,
            source_password_ref_kind = EXCLUDED.source_password_ref_kind,
            source_password_ref_value = EXCLUDED.source_password_ref_value,
            source_timezone = EXCLUDED.source_timezone,
            source_tls_json = EXCLUDED.source_tls_json,
            target_kind = EXCLUDED.target_kind,
            target_host = EXCLUDED.target_host,
            target_port = EXCLUDED.target_port,
            target_database = EXCLUDED.target_database,
            target_username = EXCLUDED.target_username,
            target_password_ref_kind = EXCLUDED.target_password_ref_kind,
            target_password_ref_value = EXCLUDED.target_password_ref_value,
            target_tls_json = EXCLUDED.target_tls_json,
            applied_at = now()
        "#,
    )
    .bind(&deployment.name)
    .bind(&deployment.source.kind)
    .bind(&deployment.source.host)
    .bind(deployment.source.port)
    .bind(&deployment.source.database)
    .bind(&deployment.source.username)
    .bind(deployment.source.password_ref.kind.as_str())
    .bind(&deployment.source.password_ref.value)
    .bind(&deployment.source.timezone)
    .bind(tls_settings_to_json(&deployment.source.tls)?)
    .bind(&deployment.target.kind)
    .bind(&deployment.target.host)
    .bind(deployment.target.port)
    .bind(&deployment.target.database)
    .bind(&deployment.target.username)
    .bind(deployment.target.password_ref.kind.as_str())
    .bind(&deployment.target.password_ref.value)
    .bind(tls_settings_to_json(&deployment.target.tls)?)
    .execute(&pool)
    .await
    .map_err(PlatformStoreError::Persist)?;
    Ok(())
}

fn tls_settings_to_json(tls: &TlsSettings) -> Result<String, PlatformStoreError> {
    serde_json::to_string(tls).map_err(PlatformStoreError::InvalidJson)
}

fn tls_settings_from_json(raw: &str) -> Result<TlsSettings, PlatformStoreError> {
    if raw.trim().is_empty() || raw.trim() == "{}" {
        return Ok(TlsSettings::default());
    }
    serde_json::from_str(raw).map_err(PlatformStoreError::InvalidJson)
}

/// Replace all Pipelines for a Deployment with the provided set.
pub async fn replace_pipelines(
    database_url: &str,
    deployment_name: &str,
    pipelines: &[Pipeline],
) -> Result<(), PlatformStoreError> {
    let pool = connect(database_url).await?;
    let mut tx = pool.begin().await.map_err(PlatformStoreError::Persist)?;

    sqlx::query("DELETE FROM pipelines WHERE deployment_name = $1")
        .bind(deployment_name)
        .execute(&mut *tx)
        .await
        .map_err(PlatformStoreError::Persist)?;

    for pipeline in pipelines {
        let field_mappings_json = serde_json::to_string(&pipeline.field_mappings)
            .map_err(PlatformStoreError::InvalidJson)?;
        let output_identity_json = serde_json::to_string(&pipeline.output_identity)
            .map_err(PlatformStoreError::InvalidJson)?;
        let transform_json = match &pipeline.transform_json {
            Some(value) => {
                serde_json::to_string(value).map_err(PlatformStoreError::InvalidJson)?
            }
            None => "null".to_string(),
        };
        sqlx::query(
            r#"
            INSERT INTO pipelines (
                deployment_name, name, mode, source_table, source_schema,
                target_collection, delivery_status, delivery_applied_changes, delivery_lag,
                paused, description, field_mappings_json, output_identity_json, transform_json,
                drift_status, drift_checked_rows, drift_mismatched_rows, applied_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, now())
            "#,
        )
        .bind(&pipeline.deployment_name)
        .bind(&pipeline.name)
        .bind(&pipeline.mode)
        .bind(&pipeline.source_table)
        .bind(&pipeline.source_schema)
        .bind(&pipeline.target_collection)
        .bind(&pipeline.delivery_status)
        .bind(pipeline.delivery_applied_changes)
        .bind(pipeline.delivery_lag)
        .bind(pipeline.paused)
        .bind(&pipeline.description)
        .bind(&field_mappings_json)
        .bind(&output_identity_json)
        .bind(&transform_json)
        .bind(&pipeline.drift_status)
        .bind(pipeline.drift_checked_rows)
        .bind(pipeline.drift_mismatched_rows)
        .execute(&mut *tx)
        .await
        .map_err(PlatformStoreError::Persist)?;
    }

    tx.commit().await.map_err(PlatformStoreError::Persist)?;
    Ok(())
}

/// Persist a Base Dataset snapshot (metadata + full supported-type rows).
pub async fn replace_base_dataset(
    database_url: &str,
    dataset: &BaseDataset,
    rows: &[serde_json::Map<String, serde_json::Value>],
) -> Result<(), PlatformStoreError> {
    let pool = connect(database_url).await?;
    let mut tx = pool.begin().await.map_err(PlatformStoreError::Persist)?;

    sqlx::query(
        r#"
        DELETE FROM base_rows
        WHERE deployment_name = $1 AND source_schema = $2 AND source_table = $3
        "#,
    )
    .bind(&dataset.deployment_name)
    .bind(&dataset.source_schema)
    .bind(&dataset.source_table)
    .execute(&mut *tx)
    .await
    .map_err(PlatformStoreError::Persist)?;

    let columns_json =
        serde_json::to_string(&dataset.columns).map_err(PlatformStoreError::InvalidJson)?;
    let omitted_json =
        serde_json::to_string(&dataset.omitted_columns).map_err(PlatformStoreError::InvalidJson)?;
    let primary_key_json =
        serde_json::to_string(&dataset.primary_key).map_err(PlatformStoreError::InvalidJson)?;
    let cursor_json = match &dataset.initial_load_cursor {
        Some(cursor) => Some(serde_json::to_string(cursor).map_err(PlatformStoreError::InvalidJson)?),
        None => None,
    };

    sqlx::query(
        r#"
        INSERT INTO base_datasets (
            deployment_name, source_table, source_schema, status,
            primary_key_json, columns_json, omitted_columns_json, row_count,
            sync_applied_changes, sync_health,
            capture_low_watermark, capture_checkpoint, sync_lag,
            source_alignment, source_alignment_checked_rows,
            source_alignment_mismatched_rows, initial_load_cursor_json, loaded_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, now()
        )
        ON CONFLICT (deployment_name, source_schema, source_table) DO UPDATE SET
            status = EXCLUDED.status,
            primary_key_json = EXCLUDED.primary_key_json,
            columns_json = EXCLUDED.columns_json,
            omitted_columns_json = EXCLUDED.omitted_columns_json,
            row_count = EXCLUDED.row_count,
            sync_applied_changes = EXCLUDED.sync_applied_changes,
            sync_health = EXCLUDED.sync_health,
            capture_low_watermark = EXCLUDED.capture_low_watermark,
            capture_checkpoint = EXCLUDED.capture_checkpoint,
            sync_lag = EXCLUDED.sync_lag,
            source_alignment = EXCLUDED.source_alignment,
            source_alignment_checked_rows = EXCLUDED.source_alignment_checked_rows,
            source_alignment_mismatched_rows = EXCLUDED.source_alignment_mismatched_rows,
            initial_load_cursor_json = EXCLUDED.initial_load_cursor_json,
            loaded_at = now()
        "#,
    )
    .bind(&dataset.deployment_name)
    .bind(&dataset.source_table)
    .bind(&dataset.source_schema)
    .bind(&dataset.status)
    .bind(&primary_key_json)
    .bind(&columns_json)
    .bind(&omitted_json)
    .bind(dataset.row_count)
    .bind(dataset.sync_applied_changes)
    .bind(&dataset.sync_health)
    .bind(dataset.capture_low_watermark)
    .bind(dataset.capture_checkpoint)
    .bind(dataset.sync_lag)
    .bind(&dataset.source_alignment)
    .bind(dataset.source_alignment_checked_rows)
    .bind(dataset.source_alignment_mismatched_rows)
    .bind(&cursor_json)
    .execute(&mut *tx)
    .await
    .map_err(PlatformStoreError::Persist)?;

    for (ordinal, row) in rows.iter().enumerate() {
        let row_json = serde_json::to_string(row).map_err(PlatformStoreError::InvalidJson)?;
        sqlx::query(
            r#"
            INSERT INTO base_rows (
                deployment_name, source_schema, source_table, row_ordinal, row_json
            ) VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(&dataset.deployment_name)
        .bind(&dataset.source_schema)
        .bind(&dataset.source_table)
        .bind(ordinal as i32)
        .bind(&row_json)
        .execute(&mut *tx)
        .await
        .map_err(PlatformStoreError::Persist)?;
    }

    tx.commit().await.map_err(PlatformStoreError::Persist)?;
    Ok(())
}

/// Append one Initial Load chunk into an existing (or new) Base Dataset.
///
/// Does **not** delete prior rows — used for chunked / pausable Initial Load
/// (issue #124). `dataset.row_count` must be the new total after this chunk.
/// `start_ordinal` is the first `row_ordinal` for the appended rows.
pub async fn append_base_dataset_chunk(
    database_url: &str,
    dataset: &BaseDataset,
    rows: &[serde_json::Map<String, serde_json::Value>],
    start_ordinal: i32,
) -> Result<(), PlatformStoreError> {
    let pool = connect(database_url).await?;
    let mut tx = pool.begin().await.map_err(PlatformStoreError::Persist)?;

    let columns_json =
        serde_json::to_string(&dataset.columns).map_err(PlatformStoreError::InvalidJson)?;
    let omitted_json =
        serde_json::to_string(&dataset.omitted_columns).map_err(PlatformStoreError::InvalidJson)?;
    let primary_key_json =
        serde_json::to_string(&dataset.primary_key).map_err(PlatformStoreError::InvalidJson)?;
    let cursor_json = match &dataset.initial_load_cursor {
        Some(cursor) => Some(serde_json::to_string(cursor).map_err(PlatformStoreError::InvalidJson)?),
        None => None,
    };

    sqlx::query(
        r#"
        INSERT INTO base_datasets (
            deployment_name, source_table, source_schema, status,
            primary_key_json, columns_json, omitted_columns_json, row_count,
            sync_applied_changes, sync_health,
            capture_low_watermark, capture_checkpoint, sync_lag,
            source_alignment, source_alignment_checked_rows,
            source_alignment_mismatched_rows, initial_load_cursor_json, loaded_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, now()
        )
        ON CONFLICT (deployment_name, source_schema, source_table) DO UPDATE SET
            status = EXCLUDED.status,
            primary_key_json = EXCLUDED.primary_key_json,
            columns_json = EXCLUDED.columns_json,
            omitted_columns_json = EXCLUDED.omitted_columns_json,
            row_count = EXCLUDED.row_count,
            sync_applied_changes = EXCLUDED.sync_applied_changes,
            sync_health = EXCLUDED.sync_health,
            capture_low_watermark = EXCLUDED.capture_low_watermark,
            capture_checkpoint = EXCLUDED.capture_checkpoint,
            sync_lag = EXCLUDED.sync_lag,
            source_alignment = EXCLUDED.source_alignment,
            source_alignment_checked_rows = EXCLUDED.source_alignment_checked_rows,
            source_alignment_mismatched_rows = EXCLUDED.source_alignment_mismatched_rows,
            initial_load_cursor_json = EXCLUDED.initial_load_cursor_json,
            loaded_at = now()
        "#,
    )
    .bind(&dataset.deployment_name)
    .bind(&dataset.source_table)
    .bind(&dataset.source_schema)
    .bind(&dataset.status)
    .bind(&primary_key_json)
    .bind(&columns_json)
    .bind(&omitted_json)
    .bind(dataset.row_count)
    .bind(dataset.sync_applied_changes)
    .bind(&dataset.sync_health)
    .bind(dataset.capture_low_watermark)
    .bind(dataset.capture_checkpoint)
    .bind(dataset.sync_lag)
    .bind(&dataset.source_alignment)
    .bind(dataset.source_alignment_checked_rows)
    .bind(dataset.source_alignment_mismatched_rows)
    .bind(&cursor_json)
    .execute(&mut *tx)
    .await
    .map_err(PlatformStoreError::Persist)?;

    for (offset, row) in rows.iter().enumerate() {
        let row_json = serde_json::to_string(row).map_err(PlatformStoreError::InvalidJson)?;
        sqlx::query(
            r#"
            INSERT INTO base_rows (
                deployment_name, source_schema, source_table, row_ordinal, row_json
            ) VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(&dataset.deployment_name)
        .bind(&dataset.source_schema)
        .bind(&dataset.source_table)
        .bind(start_ordinal + offset as i32)
        .bind(&row_json)
        .execute(&mut *tx)
        .await
        .map_err(PlatformStoreError::Persist)?;
    }

    tx.commit().await.map_err(PlatformStoreError::Persist)?;
    Ok(())
}

/// List applied Deployments ordered by name.
pub async fn list_deployments(
    database_url: &str,
) -> Result<Vec<Deployment>, PlatformStoreError> {
    let pool = connect(database_url).await?;
    let rows = sqlx::query_as::<_, DeploymentRow>(
        r#"
        SELECT
            name,
            source_kind, source_host, source_port, source_database, source_username,
            source_password_ref_kind, source_password_ref_value, source_timezone,
            source_tls_json,
            target_kind, target_host, target_port, target_database, target_username,
            target_password_ref_kind, target_password_ref_value,
            target_tls_json
        FROM deployments
        ORDER BY name
        "#,
    )
    .fetch_all(&pool)
    .await
    .map_err(PlatformStoreError::Load)?;

    rows.into_iter().map(DeploymentRow::into_deployment).collect()
}

/// List Pipelines for all Deployments, ordered by deployment then name.
pub async fn list_pipelines(database_url: &str) -> Result<Vec<Pipeline>, PlatformStoreError> {
    let pool = connect(database_url).await?;
    let rows = sqlx::query_as::<_, PipelineRow>(
        r#"
        SELECT deployment_name, name, mode, source_table, source_schema,
               target_collection, delivery_status, delivery_applied_changes, delivery_lag,
               paused, description, field_mappings_json, output_identity_json, transform_json,
               drift_status, drift_checked_rows, drift_mismatched_rows
        FROM pipelines
        ORDER BY deployment_name, name
        "#,
    )
    .fetch_all(&pool)
    .await
    .map_err(PlatformStoreError::Load)?;

    rows.into_iter().map(PipelineRow::into_pipeline).collect()
}

/// Set durable Operator pause for one Pipeline (ADR-0007 / issue #19).
pub async fn set_pipeline_paused(
    database_url: &str,
    deployment_name: &str,
    pipeline_name: &str,
    paused: bool,
) -> Result<(), PlatformStoreError> {
    let pool = connect(database_url).await?;
    let result = sqlx::query(
        r#"
        UPDATE pipelines
        SET paused = $3
        WHERE deployment_name = $1 AND name = $2
        "#,
    )
    .bind(deployment_name)
    .bind(pipeline_name)
    .bind(paused)
    .execute(&pool)
    .await
    .map_err(PlatformStoreError::Persist)?;

    if result.rows_affected() == 0 {
        return Err(PlatformStoreError::NotFound(format!(
            "Pipeline {pipeline_name} not found in Deployment {deployment_name}"
        )));
    }
    Ok(())
}

/// Delete one Pipeline (Derived Dataset CASCADE). Deployment and shared Bases stay
/// until callers prune unreferenced Bases (ADR-0007 / issue #20).
pub async fn delete_pipeline(
    database_url: &str,
    deployment_name: &str,
    pipeline_name: &str,
) -> Result<(), PlatformStoreError> {
    let pool = connect(database_url).await?;
    let result = sqlx::query(
        r#"
        DELETE FROM pipelines
        WHERE deployment_name = $1 AND name = $2
        "#,
    )
    .bind(deployment_name)
    .bind(pipeline_name)
    .execute(&pool)
    .await
    .map_err(PlatformStoreError::Persist)?;

    if result.rows_affected() == 0 {
        return Err(PlatformStoreError::NotFound(format!(
            "Pipeline {pipeline_name} not found in Deployment {deployment_name}"
        )));
    }
    Ok(())
}

/// Update Delivery status for one Pipeline.
pub async fn update_pipeline_delivery_status(
    database_url: &str,
    deployment_name: &str,
    pipeline_name: &str,
    delivery_status: &str,
) -> Result<(), PlatformStoreError> {
    update_pipeline_delivery_progress(
        database_url,
        deployment_name,
        pipeline_name,
        delivery_status,
        None,
    )
    .await
}

/// Persist Drift Check status for one Pipeline (issue #25).
pub async fn update_pipeline_drift_status(
    database_url: &str,
    deployment_name: &str,
    pipeline_name: &str,
    drift_status: &str,
    drift_checked_rows: i32,
    drift_mismatched_rows: i32,
) -> Result<(), PlatformStoreError> {
    let pool = connect(database_url).await?;
    let result = sqlx::query(
        r#"
        UPDATE pipelines
        SET drift_status = $3,
            drift_checked_rows = $4,
            drift_mismatched_rows = $5
        WHERE deployment_name = $1 AND name = $2
        "#,
    )
    .bind(deployment_name)
    .bind(pipeline_name)
    .bind(drift_status)
    .bind(drift_checked_rows)
    .bind(drift_mismatched_rows)
    .execute(&pool)
    .await
    .map_err(PlatformStoreError::Persist)?;

    if result.rows_affected() == 0 {
        return Err(PlatformStoreError::NotFound(format!(
            "Pipeline {pipeline_name} not found in Deployment {deployment_name}"
        )));
    }
    Ok(())
}

/// Update Delivery status and optionally accumulate applied Output Identity changes.
pub async fn update_pipeline_delivery_progress(
    database_url: &str,
    deployment_name: &str,
    pipeline_name: &str,
    delivery_status: &str,
    additional_applied_changes: Option<i32>,
) -> Result<(), PlatformStoreError> {
    update_pipeline_delivery_progress_with_lag(
        database_url,
        deployment_name,
        pipeline_name,
        delivery_status,
        additional_applied_changes,
        None,
    )
    .await
}

/// Persist Delivery Health lag (remaining pending Delivery work; ADR-0020 / issue #26).
pub async fn update_pipeline_delivery_lag(
    database_url: &str,
    deployment_name: &str,
    pipeline_name: &str,
    delivery_lag: i32,
) -> Result<(), PlatformStoreError> {
    let pool = connect(database_url).await?;
    let result = sqlx::query(
        r#"
        UPDATE pipelines
        SET delivery_lag = $3
        WHERE deployment_name = $1 AND name = $2
        "#,
    )
    .bind(deployment_name)
    .bind(pipeline_name)
    .bind(delivery_lag)
    .execute(&pool)
    .await
    .map_err(PlatformStoreError::Persist)?;

    if result.rows_affected() == 0 {
        return Err(PlatformStoreError::NotFound(format!(
            "Pipeline {pipeline_name} not found in Deployment {deployment_name}"
        )));
    }
    Ok(())
}

/// Update Delivery status, optional applied-count delta, and optional Delivery lag.
pub async fn update_pipeline_delivery_progress_with_lag(
    database_url: &str,
    deployment_name: &str,
    pipeline_name: &str,
    delivery_status: &str,
    additional_applied_changes: Option<i32>,
    delivery_lag: Option<i32>,
) -> Result<(), PlatformStoreError> {
    let pool = connect(database_url).await?;
    let result = match (additional_applied_changes, delivery_lag) {
        (Some(additional), Some(lag)) => {
            sqlx::query(
                r#"
                UPDATE pipelines
                SET delivery_status = $3,
                    delivery_applied_changes = delivery_applied_changes + $4,
                    delivery_lag = $5
                WHERE deployment_name = $1 AND name = $2
                "#,
            )
            .bind(deployment_name)
            .bind(pipeline_name)
            .bind(delivery_status)
            .bind(additional)
            .bind(lag)
            .execute(&pool)
            .await
            .map_err(PlatformStoreError::Persist)?
        }
        (Some(additional), None) => {
            sqlx::query(
                r#"
                UPDATE pipelines
                SET delivery_status = $3,
                    delivery_applied_changes = delivery_applied_changes + $4
                WHERE deployment_name = $1 AND name = $2
                "#,
            )
            .bind(deployment_name)
            .bind(pipeline_name)
            .bind(delivery_status)
            .bind(additional)
            .execute(&pool)
            .await
            .map_err(PlatformStoreError::Persist)?
        }
        (None, Some(lag)) => {
            sqlx::query(
                r#"
                UPDATE pipelines
                SET delivery_status = $3,
                    delivery_lag = $4
                WHERE deployment_name = $1 AND name = $2
                "#,
            )
            .bind(deployment_name)
            .bind(pipeline_name)
            .bind(delivery_status)
            .bind(lag)
            .execute(&pool)
            .await
            .map_err(PlatformStoreError::Persist)?
        }
        (None, None) => {
            sqlx::query(
                r#"
                UPDATE pipelines
                SET delivery_status = $3
                WHERE deployment_name = $1 AND name = $2
                "#,
            )
            .bind(deployment_name)
            .bind(pipeline_name)
            .bind(delivery_status)
            .execute(&pool)
            .await
            .map_err(PlatformStoreError::Persist)?
        }
    };

    if result.rows_affected() == 0 {
        return Err(PlatformStoreError::NotFound(format!(
            "Pipeline {pipeline_name} not found in Deployment {deployment_name}"
        )));
    }
    Ok(())
}

/// List Base Datasets for all Deployments.
pub async fn list_base_datasets(
    database_url: &str,
) -> Result<Vec<BaseDataset>, PlatformStoreError> {
    let pool = connect(database_url).await?;
    let rows = sqlx::query_as::<_, BaseDatasetRow>(
        r#"
        SELECT
            deployment_name, source_table, source_schema, status,
            primary_key_json, columns_json, omitted_columns_json, row_count,
            sync_applied_changes, sync_health,
            capture_low_watermark, capture_checkpoint, sync_lag,
            source_alignment, source_alignment_checked_rows,
            source_alignment_mismatched_rows,
            initial_load_cursor_json
        FROM base_datasets
        ORDER BY deployment_name, source_schema, source_table
        "#,
    )
    .fetch_all(&pool)
    .await
    .map_err(PlatformStoreError::Load)?;

    rows.into_iter()
        .map(BaseDatasetRow::into_base_dataset)
        .collect()
}

/// Load Base Dataset rows for a Source table (operator-facing inspect).
///
/// When `deployment_name` is `None`, exactly one matching Base Dataset must exist.
pub async fn get_base_rows(
    database_url: &str,
    table: &str,
    deployment_name: Option<&str>,
) -> Result<(BaseDataset, Vec<BaseRow>), PlatformStoreError> {
    let pool = connect(database_url).await?;
    let dataset_rows = if let Some(deployment_name) = deployment_name {
        sqlx::query_as::<_, BaseDatasetRow>(
            r#"
            SELECT
                deployment_name, source_table, source_schema, status,
                primary_key_json, columns_json, omitted_columns_json, row_count,
                sync_applied_changes, sync_health,
                capture_low_watermark, capture_checkpoint, sync_lag,
                source_alignment, source_alignment_checked_rows,
                source_alignment_mismatched_rows,
                initial_load_cursor_json
            FROM base_datasets
            WHERE source_table = $1 AND deployment_name = $2
            ORDER BY source_schema
            "#,
        )
        .bind(table)
        .bind(deployment_name)
        .fetch_all(&pool)
        .await
        .map_err(PlatformStoreError::Load)?
    } else {
        sqlx::query_as::<_, BaseDatasetRow>(
            r#"
            SELECT
                deployment_name, source_table, source_schema, status,
                primary_key_json, columns_json, omitted_columns_json, row_count,
                sync_applied_changes, sync_health,
                capture_low_watermark, capture_checkpoint, sync_lag,
                source_alignment, source_alignment_checked_rows,
                source_alignment_mismatched_rows,
                initial_load_cursor_json
            FROM base_datasets
            WHERE source_table = $1
            ORDER BY deployment_name, source_schema
            "#,
        )
        .bind(table)
        .fetch_all(&pool)
        .await
        .map_err(PlatformStoreError::Load)?
    };

    let dataset_row = match dataset_rows.as_slice() {
        [] => {
            return Err(PlatformStoreError::NotFound(format!(
                "no Base Dataset found for table {table}"
            )));
        }
        [only] => only.clone(),
        many => {
            return Err(PlatformStoreError::NotFound(format!(
                "multiple Base Datasets found for table {table} across Deployments {}; \
                 pass --deployment to disambiguate",
                many.iter()
                    .map(|row| row.deployment_name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
    };
    let dataset = dataset_row.into_base_dataset()?;

    let rows = sqlx::query_as::<_, BaseRowDb>(
        r#"
        SELECT row_ordinal, row_json
        FROM base_rows
        WHERE deployment_name = $1 AND source_schema = $2 AND source_table = $3
        ORDER BY row_ordinal
        "#,
    )
    .bind(&dataset.deployment_name)
    .bind(&dataset.source_schema)
    .bind(&dataset.source_table)
    .fetch_all(&pool)
    .await
    .map_err(PlatformStoreError::Load)?;

    let base_rows = rows
        .into_iter()
        .map(BaseRowDb::into_base_row)
        .collect::<Result<Vec<_>, _>>()?;
    Ok((dataset, base_rows))
}

#[derive(Debug, sqlx::FromRow)]
struct DeploymentRow {
    name: String,
    source_kind: String,
    source_host: String,
    source_port: i32,
    source_database: String,
    source_username: String,
    source_password_ref_kind: String,
    source_password_ref_value: String,
    source_timezone: String,
    source_tls_json: String,
    target_kind: String,
    target_host: String,
    target_port: i32,
    target_database: String,
    target_username: String,
    target_password_ref_kind: String,
    target_password_ref_value: String,
    target_tls_json: String,
}

impl DeploymentRow {
    fn into_deployment(self) -> Result<Deployment, PlatformStoreError> {
        Ok(Deployment {
            name: self.name,
            source: SystemConnection {
                kind: self.source_kind,
                host: self.source_host,
                port: self.source_port,
                database: self.source_database,
                username: self.source_username,
                password_ref: SecretRef {
                    kind: SecretRefKind::parse(&self.source_password_ref_kind)?,
                    value: self.source_password_ref_value,
                },
                timezone: self.source_timezone,
                tls: tls_settings_from_json(&self.source_tls_json)?,
            },
            target: SystemConnection {
                kind: self.target_kind,
                host: self.target_host,
                port: self.target_port,
                database: self.target_database,
                username: self.target_username,
                password_ref: SecretRef {
                    kind: SecretRefKind::parse(&self.target_password_ref_kind)?,
                    value: self.target_password_ref_value,
                },
                timezone: String::new(),
                tls: tls_settings_from_json(&self.target_tls_json)?,
            },
        })
    }
}

#[derive(Debug, sqlx::FromRow)]
struct PipelineRow {
    deployment_name: String,
    name: String,
    mode: String,
    source_table: String,
    source_schema: String,
    target_collection: String,
    delivery_status: String,
    delivery_applied_changes: i32,
    delivery_lag: i32,
    paused: bool,
    description: String,
    field_mappings_json: String,
    output_identity_json: String,
    transform_json: String,
    drift_status: String,
    drift_checked_rows: i32,
    drift_mismatched_rows: i32,
}

impl PipelineRow {
    fn into_pipeline(self) -> Result<Pipeline, PlatformStoreError> {
        let field_mappings = serde_json::from_str(&self.field_mappings_json)
            .map_err(PlatformStoreError::InvalidJson)?;
        let output_identity = serde_json::from_str(&self.output_identity_json)
            .map_err(PlatformStoreError::InvalidJson)?;
        let transform_value: serde_json::Value = serde_json::from_str(&self.transform_json)
            .map_err(PlatformStoreError::InvalidJson)?;
        let transform_json = if transform_value.is_null() {
            None
        } else {
            Some(transform_value)
        };
        Ok(Pipeline {
            deployment_name: self.deployment_name,
            name: self.name,
            mode: self.mode,
            source_table: self.source_table,
            source_schema: self.source_schema,
            target_collection: self.target_collection,
            delivery_status: self.delivery_status,
            delivery_applied_changes: self.delivery_applied_changes,
            delivery_lag: self.delivery_lag,
            paused: self.paused,
            description: self.description,
            field_mappings,
            output_identity,
            transform_json,
            drift_status: self.drift_status,
            drift_checked_rows: self.drift_checked_rows,
            drift_mismatched_rows: self.drift_mismatched_rows,
        })
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
struct BaseDatasetRow {
    deployment_name: String,
    source_table: String,
    source_schema: String,
    status: String,
    primary_key_json: String,
    columns_json: String,
    omitted_columns_json: String,
    row_count: i32,
    sync_applied_changes: i32,
    sync_health: String,
    capture_low_watermark: Option<i64>,
    capture_checkpoint: Option<i64>,
    sync_lag: i32,
    source_alignment: String,
    source_alignment_checked_rows: i32,
    source_alignment_mismatched_rows: i32,
    initial_load_cursor_json: Option<String>,
}

/// Delete Base Datasets (and rows) for a Deployment whose tables are not in `keep_tables`.
pub async fn delete_base_datasets_not_in(
    database_url: &str,
    deployment_name: &str,
    keep_tables: &[(String, String)],
) -> Result<(), PlatformStoreError> {
    let pool = connect(database_url).await?;
    let existing = sqlx::query_as::<_, (String, String)>(
        r#"
        SELECT source_schema, source_table
        FROM base_datasets
        WHERE deployment_name = $1
        "#,
    )
    .bind(deployment_name)
    .fetch_all(&pool)
    .await
    .map_err(PlatformStoreError::Persist)?;

    for (schema, table) in existing {
        let keep = keep_tables
            .iter()
            .any(|(s, t)| s == &schema && t == &table);
        if keep {
            continue;
        }
        sqlx::query(
            r#"
            DELETE FROM base_rows
            WHERE deployment_name = $1 AND source_schema = $2 AND source_table = $3
            "#,
        )
        .bind(deployment_name)
        .bind(&schema)
        .bind(&table)
        .execute(&pool)
        .await
        .map_err(PlatformStoreError::Persist)?;
        sqlx::query(
            r#"
            DELETE FROM base_datasets
            WHERE deployment_name = $1 AND source_schema = $2 AND source_table = $3
            "#,
        )
        .bind(deployment_name)
        .bind(&schema)
        .bind(&table)
        .execute(&pool)
        .await
        .map_err(PlatformStoreError::Persist)?;
    }
    Ok(())
}

/// Whether a Base Dataset already exists for the given Deployment table.
pub async fn base_dataset_exists(
    database_url: &str,
    deployment_name: &str,
    source_schema: &str,
    source_table: &str,
) -> Result<bool, PlatformStoreError> {
    let pool = connect(database_url).await?;
    let found = sqlx::query_scalar::<_, i32>(
        r#"
        SELECT 1
        FROM base_datasets
        WHERE deployment_name = $1 AND source_schema = $2 AND source_table = $3
        LIMIT 1
        "#,
    )
    .bind(deployment_name)
    .bind(source_schema)
    .bind(source_table)
    .fetch_optional(&pool)
    .await
    .map_err(PlatformStoreError::Load)?;
    Ok(found.is_some())
}

/// Backfill Output Identity source primary-key metadata without reloading Base rows.
pub async fn update_base_primary_key(
    database_url: &str,
    deployment_name: &str,
    source_schema: &str,
    source_table: &str,
    primary_key: &[String],
) -> Result<(), PlatformStoreError> {
    let pool = connect(database_url).await?;
    let primary_key_json =
        serde_json::to_string(primary_key).map_err(PlatformStoreError::InvalidJson)?;
    let result = sqlx::query(
        r#"
        UPDATE base_datasets
        SET primary_key_json = $4
        WHERE deployment_name = $1 AND source_schema = $2 AND source_table = $3
        "#,
    )
    .bind(deployment_name)
    .bind(source_schema)
    .bind(source_table)
    .bind(&primary_key_json)
    .execute(&pool)
    .await
    .map_err(PlatformStoreError::Persist)?;

    if result.rows_affected() == 0 {
        return Err(PlatformStoreError::NotFound(format!(
            "no Base Dataset found for table {source_table}"
        )));
    }
    Ok(())
}

impl BaseDatasetRow {
    fn into_base_dataset(self) -> Result<BaseDataset, PlatformStoreError> {
        Ok(BaseDataset {
            deployment_name: self.deployment_name,
            source_table: self.source_table,
            source_schema: self.source_schema,
            status: self.status,
            primary_key: serde_json::from_str(&self.primary_key_json)
                .map_err(PlatformStoreError::InvalidJson)?,
            columns: serde_json::from_str(&self.columns_json)
                .map_err(PlatformStoreError::InvalidJson)?,
            omitted_columns: serde_json::from_str(&self.omitted_columns_json)
                .map_err(PlatformStoreError::InvalidJson)?,
            row_count: self.row_count,
            sync_applied_changes: self.sync_applied_changes,
            sync_health: self.sync_health,
            capture_low_watermark: self.capture_low_watermark,
            capture_checkpoint: self.capture_checkpoint,
            sync_lag: self.sync_lag,
            source_alignment: self.source_alignment,
            source_alignment_checked_rows: self.source_alignment_checked_rows,
            source_alignment_mismatched_rows: self.source_alignment_mismatched_rows,
            initial_load_cursor: match self.initial_load_cursor_json.as_deref() {
                None | Some("") => None,
                Some(raw) => Some(
                    serde_json::from_str(raw).map_err(PlatformStoreError::InvalidJson)?,
                ),
            },
        })
    }
}

/// List applied source change ids at or after `from_position` for resume-safe
/// same-SCN Incremental windows (issue #143).
pub async fn list_applied_change_ids_from_position(
    database_url: &str,
    deployment_name: &str,
    source_schema: &str,
    source_table: &str,
    from_position: i64,
) -> Result<Vec<String>, PlatformStoreError> {
    let pool = connect(database_url).await?;
    sqlx::query_scalar::<_, String>(
        r#"
        SELECT change_id
        FROM applied_source_changes
        WHERE deployment_name = $1
          AND source_schema = $2
          AND source_table = $3
          AND position >= $4
        ORDER BY position ASC, change_id ASC
        "#,
    )
    .bind(deployment_name)
    .bind(source_schema)
    .bind(source_table)
    .bind(from_position)
    .fetch_all(&pool)
    .await
    .map_err(PlatformStoreError::Load)
}

/// Filter `change_ids` down to those not yet applied into this Base Dataset.
pub async fn filter_unapplied_change_ids(
    database_url: &str,
    deployment_name: &str,
    source_schema: &str,
    source_table: &str,
    change_ids: &[String],
) -> Result<Vec<String>, PlatformStoreError> {
    if change_ids.is_empty() {
        return Ok(Vec::new());
    }
    let pool = connect(database_url).await?;
    let existing = sqlx::query_scalar::<_, String>(
        r#"
        SELECT change_id
        FROM applied_source_changes
        WHERE deployment_name = $1
          AND source_schema = $2
          AND source_table = $3
          AND change_id = ANY($4)
        "#,
    )
    .bind(deployment_name)
    .bind(source_schema)
    .bind(source_table)
    .bind(change_ids)
    .fetch_all(&pool)
    .await
    .map_err(PlatformStoreError::Load)?;

    let existing_set: std::collections::BTreeSet<_> = existing.into_iter().collect();
    Ok(change_ids
        .iter()
        .filter(|id| !existing_set.contains(*id))
        .cloned()
        .collect())
}

/// Record applied source change ids for cutover/replay dedupe.
pub async fn record_applied_source_changes(
    database_url: &str,
    deployment_name: &str,
    source_schema: &str,
    source_table: &str,
    changes: &[(String, i64)],
) -> Result<(), PlatformStoreError> {
    if changes.is_empty() {
        return Ok(());
    }
    let pool = connect(database_url).await?;
    let mut tx = pool.begin().await.map_err(PlatformStoreError::Persist)?;
    for (change_id, position) in changes {
        sqlx::query(
            r#"
            INSERT INTO applied_source_changes (
                deployment_name, source_schema, source_table, change_id, position
            ) VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (deployment_name, source_schema, source_table, change_id) DO NOTHING
            "#,
        )
        .bind(deployment_name)
        .bind(source_schema)
        .bind(source_table)
        .bind(change_id)
        .bind(position)
        .execute(&mut *tx)
        .await
        .map_err(PlatformStoreError::Persist)?;
    }
    tx.commit().await.map_err(PlatformStoreError::Persist)?;
    Ok(())
}

#[derive(Debug, sqlx::FromRow)]
struct BaseRowDb {
    row_ordinal: i32,
    row_json: String,
}

impl BaseRowDb {
    fn into_base_row(self) -> Result<BaseRow, PlatformStoreError> {
        let value: serde_json::Value =
            serde_json::from_str(&self.row_json).map_err(PlatformStoreError::InvalidJson)?;
        let data = value
            .as_object()
            .cloned()
            .ok_or_else(|| {
                PlatformStoreError::NotFound("stored Base row is not a JSON object".to_string())
            })?;
        Ok(BaseRow {
            row_ordinal: self.row_ordinal,
            data,
        })
    }
}

/// Persist a Derived Dataset snapshot (metadata + rows) for a Transform Pipeline.
pub async fn replace_derived_dataset(
    database_url: &str,
    dataset: &DerivedDataset,
    rows: &[serde_json::Map<String, serde_json::Value>],
) -> Result<(), PlatformStoreError> {
    let pool = connect(database_url).await?;
    let mut tx = pool.begin().await.map_err(PlatformStoreError::Persist)?;

    sqlx::query(
        r#"
        DELETE FROM derived_rows
        WHERE deployment_name = $1 AND pipeline_name = $2
        "#,
    )
    .bind(&dataset.deployment_name)
    .bind(&dataset.pipeline_name)
    .execute(&mut *tx)
    .await
    .map_err(PlatformStoreError::Persist)?;

    let output_identity_json = serde_json::to_string(&dataset.output_identity)
        .map_err(PlatformStoreError::InvalidJson)?;
    let columns_json =
        serde_json::to_string(&dataset.columns).map_err(PlatformStoreError::InvalidJson)?;

    sqlx::query(
        r#"
        INSERT INTO derived_datasets (
            deployment_name, pipeline_name, status,
            output_identity_json, columns_json, row_count, materialized_at
        ) VALUES ($1, $2, $3, $4, $5, $6, now())
        ON CONFLICT (deployment_name, pipeline_name) DO UPDATE SET
            status = EXCLUDED.status,
            output_identity_json = EXCLUDED.output_identity_json,
            columns_json = EXCLUDED.columns_json,
            row_count = EXCLUDED.row_count,
            materialized_at = now()
        "#,
    )
    .bind(&dataset.deployment_name)
    .bind(&dataset.pipeline_name)
    .bind(&dataset.status)
    .bind(&output_identity_json)
    .bind(&columns_json)
    .bind(dataset.row_count)
    .execute(&mut *tx)
    .await
    .map_err(PlatformStoreError::Persist)?;

    for (ordinal, row) in rows.iter().enumerate() {
        let row_json = serde_json::to_string(row).map_err(PlatformStoreError::InvalidJson)?;
        sqlx::query(
            r#"
            INSERT INTO derived_rows (
                deployment_name, pipeline_name, row_ordinal, row_json
            ) VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(&dataset.deployment_name)
        .bind(&dataset.pipeline_name)
        .bind(ordinal as i32)
        .bind(&row_json)
        .execute(&mut *tx)
        .await
        .map_err(PlatformStoreError::Persist)?;
    }

    tx.commit().await.map_err(PlatformStoreError::Persist)?;
    Ok(())
}

/// List Derived Datasets ordered by deployment then Pipeline name.
pub async fn list_derived_datasets(
    database_url: &str,
) -> Result<Vec<DerivedDataset>, PlatformStoreError> {
    let pool = connect(database_url).await?;
    let rows = sqlx::query_as::<_, DerivedDatasetRow>(
        r#"
        SELECT deployment_name, pipeline_name, status,
               output_identity_json, columns_json, row_count
        FROM derived_datasets
        ORDER BY deployment_name, pipeline_name
        "#,
    )
    .fetch_all(&pool)
    .await
    .map_err(PlatformStoreError::Load)?;

    rows.into_iter()
        .map(DerivedDatasetRow::into_derived_dataset)
        .collect()
}

/// Load Derived Dataset rows for one Pipeline.
pub async fn get_derived_rows(
    database_url: &str,
    pipeline_name: &str,
    deployment_name: Option<&str>,
) -> Result<(DerivedDataset, Vec<DerivedRow>), PlatformStoreError> {
    let pool = connect(database_url).await?;

    let dataset_row = if let Some(deployment) = deployment_name {
        sqlx::query_as::<_, DerivedDatasetRow>(
            r#"
            SELECT deployment_name, pipeline_name, status,
                   output_identity_json, columns_json, row_count
            FROM derived_datasets
            WHERE deployment_name = $1 AND pipeline_name = $2
            "#,
        )
        .bind(deployment)
        .bind(pipeline_name)
        .fetch_optional(&pool)
        .await
        .map_err(PlatformStoreError::Load)?
    } else {
        let matches = sqlx::query_as::<_, DerivedDatasetRow>(
            r#"
            SELECT deployment_name, pipeline_name, status,
                   output_identity_json, columns_json, row_count
            FROM derived_datasets
            WHERE pipeline_name = $1
            ORDER BY deployment_name
            "#,
        )
        .bind(pipeline_name)
        .fetch_all(&pool)
        .await
        .map_err(PlatformStoreError::Load)?;
        match matches.len() {
            0 => None,
            1 => matches.into_iter().next(),
            _ => {
                return Err(PlatformStoreError::NotFound(format!(
                    "multiple Derived Datasets named {pipeline_name}; pass deployment to disambiguate"
                )));
            }
        }
    };

    let dataset_row = dataset_row.ok_or_else(|| {
        PlatformStoreError::NotFound(format!(
            "no Derived Dataset found for Pipeline {pipeline_name}"
        ))
    })?;
    let dataset = dataset_row.into_derived_dataset()?;

    let row_dbs = sqlx::query_as::<_, DerivedRowDb>(
        r#"
        SELECT row_ordinal, row_json
        FROM derived_rows
        WHERE deployment_name = $1 AND pipeline_name = $2
        ORDER BY row_ordinal
        "#,
    )
    .bind(&dataset.deployment_name)
    .bind(&dataset.pipeline_name)
    .fetch_all(&pool)
    .await
    .map_err(PlatformStoreError::Load)?;

    let rows = row_dbs
        .into_iter()
        .map(DerivedRowDb::into_derived_row)
        .collect::<Result<Vec<_>, _>>()?;
    Ok((dataset, rows))
}

/// Persist Maintenance State JSON for a Transform Pipeline (distinct/addToSet).
///
/// Callers serialize the transform-crate `MaintenanceState`. Pipelines that do not
/// require Maintenance State should call [`delete_maintenance_state`] instead.
pub async fn replace_maintenance_state(
    database_url: &str,
    deployment_name: &str,
    pipeline_name: &str,
    state_json: &str,
) -> Result<(), PlatformStoreError> {
    let pool = connect(database_url).await?;
    sqlx::query(
        r#"
        INSERT INTO maintenance_states (
            deployment_name, pipeline_name, state_json, updated_at
        ) VALUES ($1, $2, $3, now())
        ON CONFLICT (deployment_name, pipeline_name) DO UPDATE SET
            state_json = EXCLUDED.state_json,
            updated_at = now()
        "#,
    )
    .bind(deployment_name)
    .bind(pipeline_name)
    .bind(state_json)
    .execute(&pool)
    .await
    .map_err(PlatformStoreError::Persist)?;
    Ok(())
}

/// Load Maintenance State JSON for a Pipeline, if present.
pub async fn get_maintenance_state_json(
    database_url: &str,
    deployment_name: &str,
    pipeline_name: &str,
) -> Result<Option<String>, PlatformStoreError> {
    let pool = connect(database_url).await?;
    let row: Option<(String,)> = sqlx::query_as(
        r#"
        SELECT state_json
        FROM maintenance_states
        WHERE deployment_name = $1 AND pipeline_name = $2
        "#,
    )
    .bind(deployment_name)
    .bind(pipeline_name)
    .fetch_optional(&pool)
    .await
    .map_err(PlatformStoreError::Load)?;
    Ok(row.map(|(json,)| json))
}

/// Remove Maintenance State for a Pipeline (no-op when absent).
pub async fn delete_maintenance_state(
    database_url: &str,
    deployment_name: &str,
    pipeline_name: &str,
) -> Result<(), PlatformStoreError> {
    let pool = connect(database_url).await?;
    sqlx::query(
        r#"
        DELETE FROM maintenance_states
        WHERE deployment_name = $1 AND pipeline_name = $2
        "#,
    )
    .bind(deployment_name)
    .bind(pipeline_name)
    .execute(&pool)
    .await
    .map_err(PlatformStoreError::Persist)?;
    Ok(())
}

#[derive(Debug, sqlx::FromRow)]
struct DerivedDatasetRow {
    deployment_name: String,
    pipeline_name: String,
    status: String,
    output_identity_json: String,
    columns_json: String,
    row_count: i32,
}

impl DerivedDatasetRow {
    fn into_derived_dataset(self) -> Result<DerivedDataset, PlatformStoreError> {
        Ok(DerivedDataset {
            deployment_name: self.deployment_name,
            pipeline_name: self.pipeline_name,
            status: self.status,
            output_identity: serde_json::from_str(&self.output_identity_json)
                .map_err(PlatformStoreError::InvalidJson)?,
            columns: serde_json::from_str(&self.columns_json)
                .map_err(PlatformStoreError::InvalidJson)?,
            row_count: self.row_count,
        })
    }
}

#[derive(Debug, sqlx::FromRow)]
struct DerivedRowDb {
    row_ordinal: i32,
    row_json: String,
}

impl DerivedRowDb {
    fn into_derived_row(self) -> Result<DerivedRow, PlatformStoreError> {
        let value: serde_json::Value =
            serde_json::from_str(&self.row_json).map_err(PlatformStoreError::InvalidJson)?;
        let data = value.as_object().cloned().ok_or_else(|| {
            PlatformStoreError::NotFound("stored Derived row is not a JSON object".to_string())
        })?;
        Ok(DerivedRow {
            row_ordinal: self.row_ordinal,
            data,
        })
    }
}

/// Durable Poison Change quarantine record (ADR-0015 / issue #22).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QuarantinedChange {
    pub deployment_name: String,
    pub pipeline_name: String,
    pub source_schema: String,
    pub source_table: String,
    pub change_id: String,
    pub capture_position: i64,
    pub output_identity: serde_json::Value,
    pub stage: String,
    pub attempts: i32,
    pub last_error: String,
    pub status: String,
}

#[derive(Debug, sqlx::FromRow)]
struct QuarantinedChangeRow {
    deployment_name: String,
    pipeline_name: String,
    source_schema: String,
    source_table: String,
    change_id: String,
    capture_position: i64,
    output_identity_json: String,
    stage: String,
    attempts: i32,
    last_error: String,
    status: String,
}

impl QuarantinedChangeRow {
    fn into_quarantined_change(self) -> Result<QuarantinedChange, PlatformStoreError> {
        Ok(QuarantinedChange {
            deployment_name: self.deployment_name,
            pipeline_name: self.pipeline_name,
            source_schema: self.source_schema,
            source_table: self.source_table,
            change_id: self.change_id,
            capture_position: self.capture_position,
            output_identity: serde_json::from_str(&self.output_identity_json)
                .map_err(PlatformStoreError::InvalidJson)?,
            stage: self.stage,
            attempts: self.attempts,
            last_error: self.last_error,
            status: self.status,
        })
    }
}

/// Persist or refresh a Poison Change quarantine (ADR-0015).
pub async fn upsert_quarantined_change(
    database_url: &str,
    record: &QuarantinedChange,
) -> Result<(), PlatformStoreError> {
    let pool = connect(database_url).await?;
    let identity_json = serde_json::to_string(&record.output_identity)
        .map_err(PlatformStoreError::InvalidJson)?;
    sqlx::query(
        r#"
        INSERT INTO poison_quarantine (
            deployment_name, pipeline_name, source_schema, source_table,
            change_id, capture_position, output_identity_json, stage,
            attempts, last_error, status
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11)
        ON CONFLICT (deployment_name, pipeline_name, change_id) DO UPDATE SET
            source_schema = EXCLUDED.source_schema,
            source_table = EXCLUDED.source_table,
            capture_position = EXCLUDED.capture_position,
            output_identity_json = EXCLUDED.output_identity_json,
            stage = EXCLUDED.stage,
            attempts = EXCLUDED.attempts,
            last_error = EXCLUDED.last_error,
            status = EXCLUDED.status,
            quarantined_at = NOW()
        "#,
    )
    .bind(&record.deployment_name)
    .bind(&record.pipeline_name)
    .bind(&record.source_schema)
    .bind(&record.source_table)
    .bind(&record.change_id)
    .bind(record.capture_position)
    .bind(identity_json)
    .bind(&record.stage)
    .bind(record.attempts)
    .bind(&record.last_error)
    .bind(&record.status)
    .execute(&pool)
    .await
    .map_err(PlatformStoreError::Persist)?;
    Ok(())
}

/// List active (status=quarantined) Poison Change records, optionally scoped.
pub async fn list_quarantined_changes(
    database_url: &str,
    deployment_name: Option<&str>,
) -> Result<Vec<QuarantinedChange>, PlatformStoreError> {
    let pool = connect(database_url).await?;
    // Optional deployment filter: empty string means "all Deployments".
    let deployment_filter = deployment_name.unwrap_or("");
    let rows = sqlx::query_as::<_, QuarantinedChangeRow>(
        r#"
        SELECT deployment_name, pipeline_name, source_schema, source_table,
               change_id, capture_position, output_identity_json::text AS output_identity_json,
               stage, attempts, last_error, status
        FROM poison_quarantine
        WHERE status = 'quarantined'
          AND ($1 = '' OR deployment_name = $1)
        ORDER BY deployment_name, pipeline_name, quarantined_at, change_id
        "#,
    )
    .bind(deployment_filter)
    .fetch_all(&pool)
    .await
    .map_err(PlatformStoreError::Load)?;

    rows.into_iter()
        .map(QuarantinedChangeRow::into_quarantined_change)
        .collect()
}

/// Count active quarantines for one Pipeline (Operator-visible Delivery Health).
pub async fn count_active_quarantines(
    database_url: &str,
    deployment_name: &str,
    pipeline_name: &str,
) -> Result<i64, PlatformStoreError> {
    let pool = connect(database_url).await?;
    let count = sqlx::query_scalar::<_, i64>(
        r#"
        SELECT COUNT(*)::bigint
        FROM poison_quarantine
        WHERE deployment_name = $1
          AND pipeline_name = $2
          AND status = 'quarantined'
        "#,
    )
    .bind(deployment_name)
    .bind(pipeline_name)
    .fetch_one(&pool)
    .await
    .map_err(PlatformStoreError::Load)?;
    Ok(count)
}

/// Durable Schema Change impact record (ADR-0009 / issue #23).
///
/// Blocking DDL warns and pauses affected Pipelines. Distinct from
/// [`QuarantinedChange`] (poison rows keep the Pipeline running).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SchemaChangeImpact {
    pub deployment_name: String,
    pub pipeline_name: String,
    pub source_schema: String,
    pub source_table: String,
    pub change_id: String,
    pub capture_position: i64,
    pub ddl_summary: String,
    pub impact: String,
    pub status: String,
}

#[derive(Debug, sqlx::FromRow)]
struct SchemaChangeImpactRow {
    deployment_name: String,
    pipeline_name: String,
    source_schema: String,
    source_table: String,
    change_id: String,
    capture_position: i64,
    ddl_summary: String,
    impact: String,
    status: String,
}

impl SchemaChangeImpactRow {
    fn into_impact(self) -> SchemaChangeImpact {
        SchemaChangeImpact {
            deployment_name: self.deployment_name,
            pipeline_name: self.pipeline_name,
            source_schema: self.source_schema,
            source_table: self.source_table,
            change_id: self.change_id,
            capture_position: self.capture_position,
            ddl_summary: self.ddl_summary,
            impact: self.impact,
            status: self.status,
        }
    }
}

/// Persist or refresh a Schema Change impact (ADR-0009).
pub async fn upsert_schema_change_impact(
    database_url: &str,
    record: &SchemaChangeImpact,
) -> Result<(), PlatformStoreError> {
    let pool = connect(database_url).await?;
    sqlx::query(
        r#"
        INSERT INTO schema_change_impacts (
            deployment_name, pipeline_name, source_schema, source_table,
            change_id, capture_position, ddl_summary, impact, status
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (deployment_name, pipeline_name, change_id) DO UPDATE SET
            source_schema = EXCLUDED.source_schema,
            source_table = EXCLUDED.source_table,
            capture_position = EXCLUDED.capture_position,
            ddl_summary = EXCLUDED.ddl_summary,
            impact = EXCLUDED.impact,
            status = EXCLUDED.status,
            warned_at = NOW()
        "#,
    )
    .bind(&record.deployment_name)
    .bind(&record.pipeline_name)
    .bind(&record.source_schema)
    .bind(&record.source_table)
    .bind(&record.change_id)
    .bind(record.capture_position)
    .bind(&record.ddl_summary)
    .bind(&record.impact)
    .bind(&record.status)
    .execute(&pool)
    .await
    .map_err(PlatformStoreError::Persist)?;
    Ok(())
}

/// List active Schema Change impacts, optionally scoped to one Deployment.
pub async fn list_schema_change_impacts(
    database_url: &str,
    deployment_name: Option<&str>,
) -> Result<Vec<SchemaChangeImpact>, PlatformStoreError> {
    let pool = connect(database_url).await?;
    let deployment_filter = deployment_name.unwrap_or("");
    let rows = sqlx::query_as::<_, SchemaChangeImpactRow>(
        r#"
        SELECT deployment_name, pipeline_name, source_schema, source_table,
               change_id, capture_position, ddl_summary, impact, status
        FROM schema_change_impacts
        WHERE status = 'active'
          AND ($1 = '' OR deployment_name = $1)
        ORDER BY deployment_name, pipeline_name, warned_at, change_id
        "#,
    )
    .bind(deployment_filter)
    .fetch_all(&pool)
    .await
    .map_err(PlatformStoreError::Load)?;

    Ok(rows.into_iter().map(SchemaChangeImpactRow::into_impact).collect())
}

/// Clear active Schema Change impacts for one Pipeline (e.g. on Operator resume).
pub async fn clear_schema_change_impacts(
    database_url: &str,
    deployment_name: &str,
    pipeline_name: &str,
) -> Result<u64, PlatformStoreError> {
    let pool = connect(database_url).await?;
    let result = sqlx::query(
        r#"
        UPDATE schema_change_impacts
        SET status = 'cleared'
        WHERE deployment_name = $1
          AND pipeline_name = $2
          AND status = 'active'
        "#,
    )
    .bind(deployment_name)
    .bind(pipeline_name)
    .execute(&pool)
    .await
    .map_err(PlatformStoreError::Persist)?;
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn platform_store_url_requires_tls_detects_explicit_modes() {
        assert!(platform_store_url_requires_tls(
            "postgres://u:p@h:5432/db?sslmode=require"
        ));
        assert!(platform_store_url_requires_tls(
            "postgres://u:p@h:5432/db?sslmode=verify-full"
        ));
        assert!(platform_store_url_requires_tls(
            "postgres://u:p@h:5432/db?connect_timeout=3&sslmode=verify-ca"
        ));
        assert!(!platform_store_url_requires_tls(
            "postgres://u:p@h:5432/db"
        ));
        assert!(!platform_store_url_requires_tls(
            "postgres://u:p@h:5432/db?sslmode=prefer"
        ));
        assert!(!platform_store_url_requires_tls(
            "postgres://u:p@h:5432/db?sslmode=disable"
        ));
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
}
