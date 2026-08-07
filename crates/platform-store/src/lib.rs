//! Platform Store: dedicated PostgreSQL data plane for the platform.

mod guardrails;

pub use guardrails::{
    check_store_settings, disk_warn_message, probe_store_resources, GuardrailError,
    PlatformStoreResourceStatus, PlatformStoreSettings, DISK_FREE_WARN_BYTES,
    MIN_MAINTENANCE_WORK_MEM_BYTES, MIN_MAX_CONNECTIONS, MIN_SHARED_BUFFERS_BYTES,
    MIN_WORK_MEM_BYTES,
};

use std::borrow::Cow;
use std::time::Duration;

use migraloop_types::ColumnShape;
use serde::{Deserialize, Serialize};
use sqlx::pool::PoolConnection;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Postgres};
use thiserror::Error;

/// Session advisory-lock key serializing Incremental Capture cycles (ADR-0005 single writer).
///
/// Held for one `sync` / continuous-run cycle so one-shot catch-up and the long-running
/// app instance do not multi-write Base/Delivery state concurrently.
const INCREMENTAL_SYNC_ADVISORY_LOCK_KEY: i64 = 0x4D47_5F53_594E_4301; // "MG_SYNC\x01"

/// One Base Dataset row mutation for Incremental Sync persist without rewriting peers.
///
/// Used by [`PlatformStore::record_sync_row_progress`] so Incremental Capture can
/// advance one source key (Base primary-key identity) without DELETE+reinsert of all
/// `base_rows` (issue #230 / ADR-0029 throughput path). This is the Base source-key
/// seam — not Target Output Identity.
#[derive(Debug, Clone, Copy)]
pub enum BaseRowMutation<'a> {
    /// Insert or replace the Base row matching `identity` (JSON containment on PK fields).
    Upsert {
        identity: &'a serde_json::Map<String, serde_json::Value>,
        row: &'a serde_json::Map<String, serde_json::Value>,
    },
    /// Delete the Base row matching `identity`.
    Delete {
        identity: &'a serde_json::Map<String, serde_json::Value>,
    },
}

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

// Shared apply-path types live in `migraloop-types`. Re-export so store callers keep
// a single import surface while enums stop drifting across crates.
pub use migraloop_types::{ManagedFieldAs, SecretRef, SecretRefKind, TlsSettings};

/// Parse a persisted secret-ref kind, mapping unknown values into store Load errors.
pub fn parse_secret_ref_kind(value: &str) -> Result<SecretRefKind, PlatformStoreError> {
    SecretRefKind::parse(value)
        .map_err(|err| PlatformStoreError::Load(sqlx::Error::Protocol(err.to_string())))
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
    /// IANA name or Oracle-style offset (`±HH:MM`) for naive DATE/TIMESTAMP when
    /// Source DB timezone is unreadable.
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
    /// Uses shared [`ManagedFieldAs`]; only explicit overrides are persisted.
    #[serde(default)]
    pub field_mappings: std::collections::BTreeMap<String, ManagedFieldAs>,
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
///
/// Domain metadata is the shared [`ColumnShape`] — Oracle-named fields are not
/// the store domain default (issue #182). Prior-release JSON may still carry
/// `oracle_type` on read via [`ColumnShape`]'s serde alias (ADR-0014).
pub type BaseColumn = ColumnShape;

/// Unsupported Source column omitted from the Base Dataset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OmittedColumn {
    pub name: String,
    /// Source-declared type name; accepts legacy `oracle_type` on read (ADR-0014).
    #[serde(alias = "oracle_type")]
    pub data_type: String,
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
    /// Operator-visible Sync Health for this Base: unknown | ok | lagging | failed.
    /// Typed assembly lives in the Deployment runtime Observability Surface.
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

/// Opened Platform Store session: one pool reused across Deployment persistence verbs.
///
/// Postgres remains the only store engine (ADR-0001). Open once per process flow and
/// call session verbs instead of reconnecting on every table-shaped CRUD call.
/// Sync / Delivery progress, quarantine, schema-impact, Source Alignment, Drift, and
/// Pipeline lifecycle intents use
/// [`PlatformStore::record_sync_window_progress`], [`PlatformStore::record_sync_row_progress`],
/// [`PlatformStore::record_delivery_progress`],
/// [`PlatformStore::quarantine_change`], [`PlatformStore::mark_schema_impact`],
/// [`PlatformStore::record_source_alignment_progress`], [`PlatformStore::record_drift_outcome`],
/// [`PlatformStore::resume_pipeline`], and [`PlatformStore::remove_pipeline`].
///
/// [`Clone`] is cheap (shared [`PgPool`]) so Deployment runtime supervise can hand a
/// session handle to panic-isolated continuous Sync workers without reopening by URL.
#[derive(Clone)]
pub struct PlatformStore {
    pool: PgPool,
}

impl PlatformStore {
    /// Open a Platform Store session against the given Postgres URL.
    pub async fn open(database_url: &str) -> Result<Self, PlatformStoreError> {
        Ok(Self {
            pool: connect(database_url).await?,
        })
    }

    /// Probe Platform Store Guardrails settings via this session's pool.
    pub async fn probe_settings(&self) -> Result<PlatformStoreSettings, PlatformStoreError> {
        guardrails::probe_store_settings_on_pool(&self.pool).await
    }

    /// Probe warn-only resource signals (disk; ADR-0010).
    ///
    /// Free-disk observation is process/env based (not a pool query); this method
    /// exists so apply can keep one session handle for guardrails + persistence.
    pub async fn probe_resources(&self) -> Result<PlatformStoreResourceStatus, PlatformStoreError> {
        let _ = &self.pool;
        probe_store_resources("unused").await
    }

    /// Acquire the Incremental Capture single-writer lock (blocks until available).
    ///
    /// The guard closes its backend session on drop (not pool-return) so the
    /// session advisory lock is released even when the Platform Store pool is
    /// reused across continuous Sync cycles.
    pub async fn acquire_incremental_sync_lock(
        &self,
    ) -> Result<IncrementalSyncLock, PlatformStoreError> {
        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(PlatformStoreError::Connect)?;
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(INCREMENTAL_SYNC_ADVISORY_LOCK_KEY)
            .execute(&mut *conn)
            .await
            .map_err(PlatformStoreError::Persist)?;
        // Returning the connection to the pool would keep the PostgreSQL session
        // (and this advisory lock) alive. Close-on-drop ends the session instead.
        conn.close_on_drop();
        Ok(IncrementalSyncLock { _conn: conn })
    }

    /// Apply versioned Platform Store schema migrations.
    pub async fn migrate(&self) -> Result<(), PlatformStoreError> {
        let pool = &self.pool;
        store_migrator()
            .run(pool)
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
    pub async fn migrate_through(&self, through_version: i64) -> Result<(), PlatformStoreError> {
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

        let pool = &self.pool;
        partial
            .run(pool)
            .await
            .map_err(PlatformStoreError::Migrate)?;
        Ok(())
    }

    /// Check whether the Platform Store is reachable and migrated.
    pub async fn health(&self) -> PlatformStoreHealth {
        let pool = &self.pool;

        if let Err(err) = sqlx::query("SELECT 1").execute(pool).await {
            return PlatformStoreHealth::Unreachable {
                reason: err.to_string(),
            };
        }

        let version = sqlx::query_scalar::<_, i64>(
            "SELECT version FROM _sqlx_migrations WHERE success = true ORDER BY version DESC LIMIT 1",
        )
        .fetch_optional(pool)
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
    ///
    /// `poison_quarantine` cascades via FK (migration 0022). An explicit delete remains
    /// so older stores that have not migrated yet still clear quarantine on Namespace wipe.
    pub async fn delete_deployment(&self, deployment_name: &str) -> Result<(), PlatformStoreError> {
        let pool = &self.pool;
        sqlx::query("DELETE FROM poison_quarantine WHERE deployment_name = $1")
            .bind(deployment_name)
            .execute(pool)
            .await
            .map_err(PlatformStoreError::Persist)?;
        sqlx::query("DELETE FROM deployments WHERE name = $1")
            .bind(deployment_name)
            .execute(pool)
            .await
            .map_err(PlatformStoreError::Persist)?;
        Ok(())
    }

    /// Create or update a Deployment. Secrets are stored only as references.
    pub async fn upsert_deployment(
        &self,
        deployment: &Deployment,
    ) -> Result<(), PlatformStoreError> {
        let pool = &self.pool;
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
        .execute(pool)
        .await
        .map_err(PlatformStoreError::Persist)?;
        Ok(())
    }

    /// Replace all Pipelines for a Deployment with the provided set.
    pub async fn replace_pipelines(
        &self,
        deployment_name: &str,
        pipelines: &[Pipeline],
    ) -> Result<(), PlatformStoreError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(PlatformStoreError::Persist)?;

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
        &self,
        dataset: &BaseDataset,
        rows: &[serde_json::Map<String, serde_json::Value>],
    ) -> Result<(), PlatformStoreError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(PlatformStoreError::Persist)?;
        replace_base_dataset_in_tx(&mut tx, dataset, rows).await?;
        tx.commit().await.map_err(PlatformStoreError::Persist)?;
        Ok(())
    }

    /// Record Incremental Sync window progress: Base snapshot + applied change ids.
    ///
    /// Deployment-intent verb — Runtime Sync paths call this instead of sequencing
    /// [`replace_base_dataset`] and [`record_applied_source_changes`]. Prefer
    /// [`record_sync_row_progress`] for ordinary per-change Incremental Capture so
    /// untouched Base rows are not rewritten (issue #230).
    pub async fn record_sync_window_progress(
        &self,
        dataset: &BaseDataset,
        rows: &[serde_json::Map<String, serde_json::Value>],
        applied_changes: &[(String, i64)],
    ) -> Result<(), PlatformStoreError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(PlatformStoreError::Persist)?;
        replace_base_dataset_in_tx(&mut tx, dataset, rows).await?;
        record_applied_source_changes_in_tx(
            &mut tx,
            &dataset.deployment_name,
            &dataset.source_schema,
            &dataset.source_table,
            applied_changes,
        )
        .await?;
        tx.commit().await.map_err(PlatformStoreError::Persist)?;
        Ok(())
    }

    /// Record one Incremental Capture Base mutation + applied change ids.
    ///
    /// Updates `base_datasets` Sync fields and upserts/deletes only the touched
    /// `base_rows` identity — peers stay durable. Preserves Deliver-before-checkpoint
    /// semantics when Runtime calls this after Target Delivery (issue #230).
    pub async fn record_sync_row_progress(
        &self,
        dataset: &BaseDataset,
        mutation: BaseRowMutation<'_>,
        applied_changes: &[(String, i64)],
    ) -> Result<(), PlatformStoreError> {
        self.record_sync_rows_progress(dataset, &[mutation], applied_changes)
            .await
    }

    /// Record many Incremental Capture Base mutations + applied change ids in one TX.
    ///
    /// Window-batch Direct Incremental path (issue #252): Deliver-before-checkpoint
    /// still holds when Runtime flushes Target Delivery for the window first, then
    /// calls this once for all collapsed Base identity mutations and change ids.
    /// Untouched peers are never rewritten.
    pub async fn record_sync_rows_progress(
        &self,
        dataset: &BaseDataset,
        mutations: &[BaseRowMutation<'_>],
        applied_changes: &[(String, i64)],
    ) -> Result<(), PlatformStoreError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(PlatformStoreError::Persist)?;
        upsert_base_dataset_metadata_in_tx(&mut tx, dataset).await?;
        for mutation in mutations {
            match mutation {
                BaseRowMutation::Upsert { identity, row } => {
                    upsert_base_row_by_identity_in_tx(
                        &mut tx,
                        &dataset.deployment_name,
                        &dataset.source_schema,
                        &dataset.source_table,
                        identity,
                        row,
                    )
                    .await?;
                }
                BaseRowMutation::Delete { identity } => {
                    delete_base_row_by_identity_in_tx(
                        &mut tx,
                        &dataset.deployment_name,
                        &dataset.source_schema,
                        &dataset.source_table,
                        identity,
                    )
                    .await?;
                }
            }
        }
        record_applied_source_changes_in_tx(
            &mut tx,
            &dataset.deployment_name,
            &dataset.source_schema,
            &dataset.source_table,
            applied_changes,
        )
        .await?;
        tx.commit().await.map_err(PlatformStoreError::Persist)?;
        Ok(())
    }

    /// Persist Sync Health / checkpoint / lag / row_count without rewriting `base_rows`.
    ///
    /// Empty Incremental windows (caught-up) must not DELETE+reinsert Base rows from a
    /// possibly stale in-memory snapshot — that races concurrent Initial Load and
    /// shrinks durable `row_count`.
    pub async fn persist_base_dataset_sync_fields(
        &self,
        dataset: &BaseDataset,
    ) -> Result<(), PlatformStoreError> {
        let pool = &self.pool;
        let columns_json =
            serde_json::to_string(&dataset.columns).map_err(PlatformStoreError::InvalidJson)?;
        let omitted_json = serde_json::to_string(&dataset.omitted_columns)
            .map_err(PlatformStoreError::InvalidJson)?;
        let primary_key_json =
            serde_json::to_string(&dataset.primary_key).map_err(PlatformStoreError::InvalidJson)?;
        let cursor_json = match &dataset.initial_load_cursor {
            Some(cursor) => {
                Some(serde_json::to_string(cursor).map_err(PlatformStoreError::InvalidJson)?)
            }
            None => None,
        };
        let result = sqlx::query(
            r#"
            UPDATE base_datasets SET
                status = $4,
                primary_key_json = $5,
                columns_json = $6,
                omitted_columns_json = $7,
                row_count = $8,
                sync_applied_changes = $9,
                sync_health = $10,
                capture_low_watermark = $11,
                capture_checkpoint = $12,
                sync_lag = $13,
                source_alignment = $14,
                source_alignment_checked_rows = $15,
                source_alignment_mismatched_rows = $16,
                initial_load_cursor_json = $17,
                loaded_at = now()
            WHERE deployment_name = $1 AND source_schema = $2 AND source_table = $3
            "#,
        )
        .bind(&dataset.deployment_name)
        .bind(&dataset.source_schema)
        .bind(&dataset.source_table)
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
        .bind(cursor_json)
        .execute(pool)
        .await
        .map_err(PlatformStoreError::Persist)?;
        if result.rows_affected() == 0 {
            return Err(PlatformStoreError::NotFound(format!(
                "Base Dataset {}.{} not found for Deployment {}",
                dataset.source_schema, dataset.source_table, dataset.deployment_name
            )));
        }
        Ok(())
    }

    /// Record Source Alignment Check progress: repaired Base snapshot + alignment fields.
    ///
    /// Deployment-intent verb — Runtime Alignment paths call this instead of a
    /// hand-built [`replace_base_dataset`] for alignment repair persistence.
    pub async fn record_source_alignment_progress(
        &self,
        dataset: &BaseDataset,
        rows: &[serde_json::Map<String, serde_json::Value>],
    ) -> Result<(), PlatformStoreError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(PlatformStoreError::Persist)?;
        replace_base_dataset_in_tx(&mut tx, dataset, rows).await?;
        tx.commit().await.map_err(PlatformStoreError::Persist)?;
        Ok(())
    }

    /// Append one Initial Load chunk into an existing (or new) Base Dataset.
    ///
    /// Does **not** delete prior rows — used for chunked / pausable Initial Load
    /// (issue #124). `dataset.row_count` must be the new total after this chunk.
    /// `start_ordinal` is the first `row_ordinal` for the appended rows.
    pub async fn append_base_dataset_chunk(
        &self,
        dataset: &BaseDataset,
        rows: &[serde_json::Map<String, serde_json::Value>],
        start_ordinal: i32,
    ) -> Result<(), PlatformStoreError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(PlatformStoreError::Persist)?;

        let columns_json =
            serde_json::to_string(&dataset.columns).map_err(PlatformStoreError::InvalidJson)?;
        let omitted_json = serde_json::to_string(&dataset.omitted_columns)
            .map_err(PlatformStoreError::InvalidJson)?;
        let primary_key_json =
            serde_json::to_string(&dataset.primary_key).map_err(PlatformStoreError::InvalidJson)?;
        let cursor_json = match &dataset.initial_load_cursor {
            Some(cursor) => {
                Some(serde_json::to_string(cursor).map_err(PlatformStoreError::InvalidJson)?)
            }
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

        insert_base_rows_bulk_in_tx(
            &mut tx,
            &dataset.deployment_name,
            &dataset.source_schema,
            &dataset.source_table,
            rows,
            start_ordinal,
        )
        .await?;

        tx.commit().await.map_err(PlatformStoreError::Persist)?;
        Ok(())
    }

    /// List applied Deployments ordered by name.
    pub async fn list_deployments(&self) -> Result<Vec<Deployment>, PlatformStoreError> {
        let pool = &self.pool;
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
        .fetch_all(pool)
        .await
        .map_err(PlatformStoreError::Load)?;

        rows.into_iter()
            .map(DeploymentRow::into_deployment)
            .collect()
    }

    /// List Pipelines for all Deployments, ordered by deployment then name.
    pub async fn list_pipelines(&self) -> Result<Vec<Pipeline>, PlatformStoreError> {
        let pool = &self.pool;
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
        .fetch_all(pool)
        .await
        .map_err(PlatformStoreError::Load)?;

        rows.into_iter().map(PipelineRow::into_pipeline).collect()
    }

    /// Set durable Operator pause for one Pipeline (ADR-0007 / issue #19).
    pub async fn set_pipeline_paused(
        &self,
        deployment_name: &str,
        pipeline_name: &str,
        paused: bool,
    ) -> Result<(), PlatformStoreError> {
        let pool = &self.pool;
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
        .execute(pool)
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
        &self,
        deployment_name: &str,
        pipeline_name: &str,
    ) -> Result<(), PlatformStoreError> {
        let pool = &self.pool;
        let result = sqlx::query(
            r#"
            DELETE FROM pipelines
            WHERE deployment_name = $1 AND name = $2
            "#,
        )
        .bind(deployment_name)
        .bind(pipeline_name)
        .execute(pool)
        .await
        .map_err(PlatformStoreError::Persist)?;

        if result.rows_affected() == 0 {
            return Err(PlatformStoreError::NotFound(format!(
                "Pipeline {pipeline_name} not found in Deployment {deployment_name}"
            )));
        }
        Ok(())
    }

    /// Record Drift Check outcome for one Pipeline (status + checked/mismatched counts).
    ///
    /// Deployment-intent verb — Runtime Drift paths call this instead of a
    /// table-shaped drift-column update.
    pub async fn record_drift_outcome(
        &self,
        deployment_name: &str,
        pipeline_name: &str,
        drift_status: &str,
        drift_checked_rows: i32,
        drift_mismatched_rows: i32,
    ) -> Result<(), PlatformStoreError> {
        let pool = &self.pool;
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
        .execute(pool)
        .await
        .map_err(PlatformStoreError::Persist)?;

        if result.rows_affected() == 0 {
            return Err(PlatformStoreError::NotFound(format!(
                "Pipeline {pipeline_name} not found in Deployment {deployment_name}"
            )));
        }
        Ok(())
    }

    /// Record Delivery progress for one Pipeline (status, applied delta, and/or lag).
    ///
    /// Deployment-intent verb — Runtime Delivery paths call this instead of
    /// sequencing fine-grained status / applied-count / lag column updates.
    /// Pass `delivery_status: None` for lag-only (or applied-only) updates that
    /// must leave the existing Delivery status untouched.
    pub async fn record_delivery_progress(
        &self,
        deployment_name: &str,
        pipeline_name: &str,
        delivery_status: Option<&str>,
        additional_applied_changes: Option<i32>,
        delivery_lag: Option<i32>,
    ) -> Result<(), PlatformStoreError> {
        let pool = &self.pool;
        let result = match (
            delivery_status,
            additional_applied_changes,
            delivery_lag,
        ) {
            (Some(status), Some(additional), Some(lag)) => sqlx::query(
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
            .bind(status)
            .bind(additional)
            .bind(lag)
            .execute(pool)
            .await
            .map_err(PlatformStoreError::Persist)?,
            (Some(status), Some(additional), None) => sqlx::query(
                r#"
                    UPDATE pipelines
                    SET delivery_status = $3,
                        delivery_applied_changes = delivery_applied_changes + $4
                    WHERE deployment_name = $1 AND name = $2
                    "#,
            )
            .bind(deployment_name)
            .bind(pipeline_name)
            .bind(status)
            .bind(additional)
            .execute(pool)
            .await
            .map_err(PlatformStoreError::Persist)?,
            (Some(status), None, Some(lag)) => sqlx::query(
                r#"
                    UPDATE pipelines
                    SET delivery_status = $3,
                        delivery_lag = $4
                    WHERE deployment_name = $1 AND name = $2
                    "#,
            )
            .bind(deployment_name)
            .bind(pipeline_name)
            .bind(status)
            .bind(lag)
            .execute(pool)
            .await
            .map_err(PlatformStoreError::Persist)?,
            (Some(status), None, None) => sqlx::query(
                r#"
                    UPDATE pipelines
                    SET delivery_status = $3
                    WHERE deployment_name = $1 AND name = $2
                    "#,
            )
            .bind(deployment_name)
            .bind(pipeline_name)
            .bind(status)
            .execute(pool)
            .await
            .map_err(PlatformStoreError::Persist)?,
            (None, Some(additional), Some(lag)) => sqlx::query(
                r#"
                    UPDATE pipelines
                    SET delivery_applied_changes = delivery_applied_changes + $3,
                        delivery_lag = $4
                    WHERE deployment_name = $1 AND name = $2
                    "#,
            )
            .bind(deployment_name)
            .bind(pipeline_name)
            .bind(additional)
            .bind(lag)
            .execute(pool)
            .await
            .map_err(PlatformStoreError::Persist)?,
            (None, Some(additional), None) => sqlx::query(
                r#"
                    UPDATE pipelines
                    SET delivery_applied_changes = delivery_applied_changes + $3
                    WHERE deployment_name = $1 AND name = $2
                    "#,
            )
            .bind(deployment_name)
            .bind(pipeline_name)
            .bind(additional)
            .execute(pool)
            .await
            .map_err(PlatformStoreError::Persist)?,
            (None, None, Some(lag)) => sqlx::query(
                r#"
                    UPDATE pipelines
                    SET delivery_lag = $3
                    WHERE deployment_name = $1 AND name = $2
                    "#,
            )
            .bind(deployment_name)
            .bind(pipeline_name)
            .bind(lag)
            .execute(pool)
            .await
            .map_err(PlatformStoreError::Persist)?,
            (None, None, None) => return Ok(()),
        };

        if result.rows_affected() == 0 {
            return Err(PlatformStoreError::NotFound(format!(
                "Pipeline {pipeline_name} not found in Deployment {deployment_name}"
            )));
        }
        Ok(())
    }

    /// List Base Datasets for all Deployments.
    pub async fn list_base_datasets(&self) -> Result<Vec<BaseDataset>, PlatformStoreError> {
        let pool = &self.pool;
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
        .fetch_all(pool)
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
        &self,
        table: &str,
        deployment_name: Option<&str>,
    ) -> Result<(BaseDataset, Vec<BaseRow>), PlatformStoreError> {
        let pool = &self.pool;
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
            .fetch_all(pool)
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
            .fetch_all(pool)
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
        .fetch_all(pool)
        .await
        .map_err(PlatformStoreError::Load)?;

        let base_rows = rows
            .into_iter()
            .map(BaseRowDb::into_base_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((dataset, base_rows))
    }

    /// Delete Base Datasets (and rows) for a Deployment whose tables are not in `keep_tables`.
    pub async fn delete_base_datasets_not_in(
        &self,
        deployment_name: &str,
        keep_tables: &[(String, String)],
    ) -> Result<(), PlatformStoreError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(PlatformStoreError::Persist)?;
        delete_base_datasets_not_in_tx(&mut tx, deployment_name, keep_tables).await?;
        tx.commit().await.map_err(PlatformStoreError::Persist)?;
        Ok(())
    }

    /// Whether a Base Dataset already exists for the given Deployment table.
    pub async fn base_dataset_exists(
        &self,
        deployment_name: &str,
        source_schema: &str,
        source_table: &str,
    ) -> Result<bool, PlatformStoreError> {
        let pool = &self.pool;
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
        .fetch_optional(pool)
        .await
        .map_err(PlatformStoreError::Load)?;
        Ok(found.is_some())
    }

    /// Backfill Output Identity source primary-key metadata without reloading Base rows.
    pub async fn update_base_primary_key(
        &self,
        deployment_name: &str,
        source_schema: &str,
        source_table: &str,
        primary_key: &[String],
    ) -> Result<(), PlatformStoreError> {
        let pool = &self.pool;
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
        .execute(pool)
        .await
        .map_err(PlatformStoreError::Persist)?;

        if result.rows_affected() == 0 {
            return Err(PlatformStoreError::NotFound(format!(
                "no Base Dataset found for table {source_table}"
            )));
        }
        Ok(())
    }

    /// List applied source change ids at or after `from_position` for resume-safe
    /// same-SCN Incremental windows (issue #143).
    pub async fn list_applied_change_ids_from_position(
        &self,
        deployment_name: &str,
        source_schema: &str,
        source_table: &str,
        from_position: i64,
    ) -> Result<Vec<String>, PlatformStoreError> {
        let pool = &self.pool;
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
        .fetch_all(pool)
        .await
        .map_err(PlatformStoreError::Load)
    }

    /// Filter `change_ids` down to those not yet applied into this Base Dataset.
    pub async fn filter_unapplied_change_ids(
        &self,
        deployment_name: &str,
        source_schema: &str,
        source_table: &str,
        change_ids: &[String],
    ) -> Result<Vec<String>, PlatformStoreError> {
        if change_ids.is_empty() {
            return Ok(Vec::new());
        }
        let pool = &self.pool;
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
        .fetch_all(pool)
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
        &self,
        deployment_name: &str,
        source_schema: &str,
        source_table: &str,
        changes: &[(String, i64)],
    ) -> Result<(), PlatformStoreError> {
        if changes.is_empty() {
            return Ok(());
        }
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(PlatformStoreError::Persist)?;
        record_applied_source_changes_in_tx(
            &mut tx,
            deployment_name,
            source_schema,
            source_table,
            changes,
        )
        .await?;
        tx.commit().await.map_err(PlatformStoreError::Persist)?;
        Ok(())
    }

    /// Persist a Derived Dataset snapshot (metadata + rows) for a Transform Pipeline.
    ///
    /// Full-snapshot path (Initial Load / reconcile). Prefer
    /// [`apply_derived_identity_changes`] for ordinary Incremental Affect recompute
    /// so untouched Derived peers are not rewritten (issue #231).
    pub async fn replace_derived_dataset(
        &self,
        dataset: &DerivedDataset,
        rows: &[serde_json::Map<String, serde_json::Value>],
    ) -> Result<(), PlatformStoreError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(PlatformStoreError::Persist)?;

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

        upsert_derived_dataset_metadata_in_tx(&mut tx, dataset).await?;
        insert_derived_rows_bulk_in_tx(
            &mut tx,
            &dataset.deployment_name,
            &dataset.pipeline_name,
            rows,
            0,
        )
        .await?;

        tx.commit().await.map_err(PlatformStoreError::Persist)?;
        Ok(())
    }

    /// Apply Incremental Transform Derived identity changes without rewriting peers.
    ///
    /// Deletes every Derived row matching any `remove_identities` containment key,
    /// then bulk-inserts `upsert_rows`. Metadata `row_count` is set from the durable
    /// row count after those mutations (caller's `dataset.row_count` is overwritten).
    /// Issue #231 / ADR-0029 Transform path.
    pub async fn apply_derived_identity_changes(
        &self,
        dataset: &DerivedDataset,
        remove_identities: &[serde_json::Map<String, serde_json::Value>],
        upsert_rows: &[serde_json::Map<String, serde_json::Value>],
    ) -> Result<(), PlatformStoreError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(PlatformStoreError::Persist)?;

        for identity in remove_identities {
            delete_derived_rows_by_identity_in_tx(
                &mut tx,
                &dataset.deployment_name,
                &dataset.pipeline_name,
                identity,
            )
            .await?;
        }

        let next_ordinal: i32 = sqlx::query_scalar(
            r#"
            SELECT COALESCE(MAX(row_ordinal), -1) + 1
            FROM derived_rows
            WHERE deployment_name = $1 AND pipeline_name = $2
            "#,
        )
        .bind(&dataset.deployment_name)
        .bind(&dataset.pipeline_name)
        .fetch_one(&mut *tx)
        .await
        .map_err(PlatformStoreError::Persist)?;

        insert_derived_rows_bulk_in_tx(
            &mut tx,
            &dataset.deployment_name,
            &dataset.pipeline_name,
            upsert_rows,
            next_ordinal,
        )
        .await?;

        let durable_count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)::bigint
            FROM derived_rows
            WHERE deployment_name = $1 AND pipeline_name = $2
            "#,
        )
        .bind(&dataset.deployment_name)
        .bind(&dataset.pipeline_name)
        .fetch_one(&mut *tx)
        .await
        .map_err(PlatformStoreError::Persist)?;

        let mut metadata = dataset.clone();
        metadata.row_count = durable_count as i32;
        upsert_derived_dataset_metadata_in_tx(&mut tx, &metadata).await?;

        tx.commit().await.map_err(PlatformStoreError::Persist)?;
        Ok(())
    }

    /// List Derived Datasets ordered by deployment then Pipeline name.
    pub async fn list_derived_datasets(&self) -> Result<Vec<DerivedDataset>, PlatformStoreError> {
        let pool = &self.pool;
        let rows = sqlx::query_as::<_, DerivedDatasetRow>(
            r#"
            SELECT deployment_name, pipeline_name, status,
                   output_identity_json, columns_json, row_count
            FROM derived_datasets
            ORDER BY deployment_name, pipeline_name
            "#,
        )
        .fetch_all(pool)
        .await
        .map_err(PlatformStoreError::Load)?;

        rows.into_iter()
            .map(DerivedDatasetRow::into_derived_dataset)
            .collect()
    }

    /// Load Derived Dataset rows for one Pipeline.
    pub async fn get_derived_rows(
        &self,
        pipeline_name: &str,
        deployment_name: Option<&str>,
    ) -> Result<(DerivedDataset, Vec<DerivedRow>), PlatformStoreError> {
        let pool = &self.pool;

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
            .fetch_optional(pool)
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
            .fetch_all(pool)
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
        .fetch_all(pool)
        .await
        .map_err(PlatformStoreError::Load)?;

        let rows = row_dbs
            .into_iter()
            .map(DerivedRowDb::into_derived_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((dataset, rows))
    }

    /// Persist opaque Maintenance State JSON for a Transform Pipeline.
    ///
    /// The blob is produced by the transform Affect Analysis interface; the store does
    /// not interpret its contents. Pipelines that do not require Maintenance State should
    /// call [`delete_maintenance_state`] instead.
    pub async fn replace_maintenance_state(
        &self,
        deployment_name: &str,
        pipeline_name: &str,
        state_json: &str,
    ) -> Result<(), PlatformStoreError> {
        let pool = &self.pool;
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
        .execute(pool)
        .await
        .map_err(PlatformStoreError::Persist)?;
        Ok(())
    }

    /// Load Maintenance State JSON for a Pipeline, if present.
    pub async fn get_maintenance_state_json(
        &self,
        deployment_name: &str,
        pipeline_name: &str,
    ) -> Result<Option<String>, PlatformStoreError> {
        let pool = &self.pool;
        let row: Option<(String,)> = sqlx::query_as(
            r#"
            SELECT state_json
            FROM maintenance_states
            WHERE deployment_name = $1 AND pipeline_name = $2
            "#,
        )
        .bind(deployment_name)
        .bind(pipeline_name)
        .fetch_optional(pool)
        .await
        .map_err(PlatformStoreError::Load)?;
        Ok(row.map(|(json,)| json))
    }

    /// Remove Maintenance State for a Pipeline (no-op when absent).
    pub async fn delete_maintenance_state(
        &self,
        deployment_name: &str,
        pipeline_name: &str,
    ) -> Result<(), PlatformStoreError> {
        let pool = &self.pool;
        sqlx::query(
            r#"
            DELETE FROM maintenance_states
            WHERE deployment_name = $1 AND pipeline_name = $2
            "#,
        )
        .bind(deployment_name)
        .bind(pipeline_name)
        .execute(pool)
        .await
        .map_err(PlatformStoreError::Persist)?;
        Ok(())
    }

    /// Quarantine a Poison Change (ADR-0015).
    ///
    /// Deployment-intent verb — Runtime poison paths call this instead of a
    /// table-shaped upsert helper name.
    pub async fn quarantine_change(
        &self,
        record: &QuarantinedChange,
    ) -> Result<(), PlatformStoreError> {
        let pool = &self.pool;
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
        .execute(pool)
        .await
        .map_err(PlatformStoreError::Persist)?;
        Ok(())
    }

    /// List active (status=quarantined) Poison Change records, optionally scoped.
    pub async fn list_quarantined_changes(
        &self,
        deployment_name: Option<&str>,
    ) -> Result<Vec<QuarantinedChange>, PlatformStoreError> {
        let pool = &self.pool;
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
        .fetch_all(pool)
        .await
        .map_err(PlatformStoreError::Load)?;

        rows.into_iter()
            .map(QuarantinedChangeRow::into_quarantined_change)
            .collect()
    }

    /// Count active quarantines for one Pipeline (Operator-visible Delivery Health).
    pub async fn count_active_quarantines(
        &self,
        deployment_name: &str,
        pipeline_name: &str,
    ) -> Result<i64, PlatformStoreError> {
        let pool = &self.pool;
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
        .fetch_one(pool)
        .await
        .map_err(PlatformStoreError::Load)?;
        Ok(count)
    }

    /// Mark a blocking Schema Change impact and pause the affected Pipeline (ADR-0009).
    ///
    /// Deployment-intent verb — Runtime Schema Change paths call this instead of
    /// sequencing pause + impact-row column updates.
    pub async fn mark_schema_impact(
        &self,
        record: &SchemaChangeImpact,
    ) -> Result<(), PlatformStoreError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(PlatformStoreError::Persist)?;

        let paused = sqlx::query(
            r#"
            UPDATE pipelines
            SET paused = true
            WHERE deployment_name = $1 AND name = $2
            "#,
        )
        .bind(&record.deployment_name)
        .bind(&record.pipeline_name)
        .execute(&mut *tx)
        .await
        .map_err(PlatformStoreError::Persist)?;
        if paused.rows_affected() == 0 {
            return Err(PlatformStoreError::NotFound(format!(
                "Pipeline {} not found in Deployment {}",
                record.pipeline_name, record.deployment_name
            )));
        }

        upsert_schema_change_impact_in_tx(&mut tx, record).await?;
        tx.commit().await.map_err(PlatformStoreError::Persist)?;
        Ok(())
    }

    /// List active Schema Change impacts, optionally scoped to one Deployment.
    pub async fn list_schema_change_impacts(
        &self,
        deployment_name: Option<&str>,
    ) -> Result<Vec<SchemaChangeImpact>, PlatformStoreError> {
        let pool = &self.pool;
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
        .fetch_all(pool)
        .await
        .map_err(PlatformStoreError::Load)?;

        Ok(rows
            .into_iter()
            .map(SchemaChangeImpactRow::into_impact)
            .collect())
    }

    /// Clear active Schema Change impacts for one Pipeline (e.g. on Operator resume).
    pub async fn clear_schema_change_impacts(
        &self,
        deployment_name: &str,
        pipeline_name: &str,
    ) -> Result<u64, PlatformStoreError> {
        let pool = &self.pool;
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
        .execute(pool)
        .await
        .map_err(PlatformStoreError::Persist)?;
        Ok(result.rows_affected())
    }

    /// Resume a paused Pipeline's durable pause and clear active schema impacts.
    ///
    /// Deployment-intent verb — Runtime Operator resume paths call this instead of
    /// sequencing [`set_pipeline_paused`]`(false)` + [`clear_schema_change_impacts`].
    /// Delivery catch-up stays in Runtime.
    pub async fn resume_pipeline(
        &self,
        deployment_name: &str,
        pipeline_name: &str,
    ) -> Result<(), PlatformStoreError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(PlatformStoreError::Persist)?;

        let paused = sqlx::query(
            r#"
            UPDATE pipelines
            SET paused = false
            WHERE deployment_name = $1 AND name = $2
            "#,
        )
        .bind(deployment_name)
        .bind(pipeline_name)
        .execute(&mut *tx)
        .await
        .map_err(PlatformStoreError::Persist)?;
        if paused.rows_affected() == 0 {
            return Err(PlatformStoreError::NotFound(format!(
                "Pipeline {pipeline_name} not found in Deployment {deployment_name}"
            )));
        }

        sqlx::query(
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
        .execute(&mut *tx)
        .await
        .map_err(PlatformStoreError::Persist)?;

        tx.commit().await.map_err(PlatformStoreError::Persist)?;
        Ok(())
    }

    /// Remove one Pipeline and prune Base Datasets outside `keep_tables`.
    ///
    /// Deployment-intent verb — Runtime remove paths call this instead of sequencing
    /// [`delete_pipeline`] + [`delete_base_datasets_not_in`]. Callers compute
    /// `keep_tables` from remaining Pipeline Base refs (ADR-0007 / ADR-0019).
    pub async fn remove_pipeline(
        &self,
        deployment_name: &str,
        pipeline_name: &str,
        keep_tables: &[(String, String)],
    ) -> Result<(), PlatformStoreError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(PlatformStoreError::Persist)?;

        let deleted = sqlx::query(
            r#"
            DELETE FROM pipelines
            WHERE deployment_name = $1 AND name = $2
            "#,
        )
        .bind(deployment_name)
        .bind(pipeline_name)
        .execute(&mut *tx)
        .await
        .map_err(PlatformStoreError::Persist)?;
        if deleted.rows_affected() == 0 {
            return Err(PlatformStoreError::NotFound(format!(
                "Pipeline {pipeline_name} not found in Deployment {deployment_name}"
            )));
        }

        delete_base_datasets_not_in_tx(&mut tx, deployment_name, keep_tables).await?;
        tx.commit().await.map_err(PlatformStoreError::Persist)?;
        Ok(())
    }
}

async fn upsert_base_dataset_metadata_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    dataset: &BaseDataset,
) -> Result<(), PlatformStoreError> {
    let columns_json =
        serde_json::to_string(&dataset.columns).map_err(PlatformStoreError::InvalidJson)?;
    let omitted_json = serde_json::to_string(&dataset.omitted_columns)
        .map_err(PlatformStoreError::InvalidJson)?;
    let primary_key_json =
        serde_json::to_string(&dataset.primary_key).map_err(PlatformStoreError::InvalidJson)?;
    let cursor_json = match &dataset.initial_load_cursor {
        Some(cursor) => {
            Some(serde_json::to_string(cursor).map_err(PlatformStoreError::InvalidJson)?)
        }
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
    .execute(&mut **tx)
    .await
    .map_err(PlatformStoreError::Persist)?;
    Ok(())
}

/// Insert `derived_rows` in one statement via UNNEST (IL + Incremental identity upserts).
async fn insert_derived_rows_bulk_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    deployment_name: &str,
    pipeline_name: &str,
    rows: &[serde_json::Map<String, serde_json::Value>],
    start_ordinal: i32,
) -> Result<(), PlatformStoreError> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut ordinals = Vec::with_capacity(rows.len());
    let mut row_jsons = Vec::with_capacity(rows.len());
    for (offset, row) in rows.iter().enumerate() {
        ordinals.push(start_ordinal + offset as i32);
        row_jsons.push(serde_json::to_string(row).map_err(PlatformStoreError::InvalidJson)?);
    }
    let deployments = vec![deployment_name.to_string(); rows.len()];
    let pipelines = vec![pipeline_name.to_string(); rows.len()];
    sqlx::query(
        r#"
            INSERT INTO derived_rows (
                deployment_name, pipeline_name, row_ordinal, row_json
            )
            SELECT * FROM UNNEST(
                $1::text[], $2::text[], $3::int4[], $4::text[]
            )
            "#,
    )
    .bind(&deployments)
    .bind(&pipelines)
    .bind(&ordinals)
    .bind(&row_jsons)
    .execute(&mut **tx)
    .await
    .map_err(PlatformStoreError::Persist)?;
    Ok(())
}

async fn upsert_derived_dataset_metadata_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    dataset: &DerivedDataset,
) -> Result<(), PlatformStoreError> {
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
    .execute(&mut **tx)
    .await
    .map_err(PlatformStoreError::Persist)?;
    Ok(())
}

async fn delete_derived_rows_by_identity_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    deployment_name: &str,
    pipeline_name: &str,
    identity: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), PlatformStoreError> {
    let identity_json = identity_json_for_containment(identity)?;
    // Delete every match (not LIMIT 1): Output Identity / group keys must cover the
    // Derived grain, but unwind fan-out historically could share partial keys.
    sqlx::query(
        r#"
            DELETE FROM derived_rows
            WHERE deployment_name = $1
              AND pipeline_name = $2
              AND row_json::jsonb @> $3::jsonb
            "#,
    )
    .bind(deployment_name)
    .bind(pipeline_name)
    .bind(&identity_json)
    .execute(&mut **tx)
    .await
    .map_err(PlatformStoreError::Persist)?;
    Ok(())
}

/// Insert `base_rows` in one statement via UNNEST (IL + full-snapshot Sync paths).
async fn insert_base_rows_bulk_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    deployment_name: &str,
    source_schema: &str,
    source_table: &str,
    rows: &[serde_json::Map<String, serde_json::Value>],
    start_ordinal: i32,
) -> Result<(), PlatformStoreError> {
    if rows.is_empty() {
        return Ok(());
    }
    let mut ordinals = Vec::with_capacity(rows.len());
    let mut row_jsons = Vec::with_capacity(rows.len());
    for (offset, row) in rows.iter().enumerate() {
        ordinals.push(start_ordinal + offset as i32);
        row_jsons.push(serde_json::to_string(row).map_err(PlatformStoreError::InvalidJson)?);
    }
    let deployments = vec![deployment_name.to_string(); rows.len()];
    let schemas = vec![source_schema.to_string(); rows.len()];
    let tables = vec![source_table.to_string(); rows.len()];
    sqlx::query(
        r#"
            INSERT INTO base_rows (
                deployment_name, source_schema, source_table, row_ordinal, row_json
            )
            SELECT * FROM UNNEST(
                $1::text[], $2::text[], $3::text[], $4::int4[], $5::text[]
            )
            "#,
    )
    .bind(&deployments)
    .bind(&schemas)
    .bind(&tables)
    .bind(&ordinals)
    .bind(&row_jsons)
    .execute(&mut **tx)
    .await
    .map_err(PlatformStoreError::Persist)?;
    Ok(())
}

fn identity_json_for_containment(
    identity: &serde_json::Map<String, serde_json::Value>,
) -> Result<String, PlatformStoreError> {
    serde_json::to_string(identity).map_err(PlatformStoreError::InvalidJson)
}

async fn upsert_base_row_by_identity_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    deployment_name: &str,
    source_schema: &str,
    source_table: &str,
    identity: &serde_json::Map<String, serde_json::Value>,
    row: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), PlatformStoreError> {
    let identity_json = identity_json_for_containment(identity)?;
    let row_json = serde_json::to_string(row).map_err(PlatformStoreError::InvalidJson)?;
    // Match by PK containment; `ctid` LIMIT 1 keeps a single ordinal if peers somehow
    // share a partial identity (full Primary Key identity should be unique).
    let updated = sqlx::query(
        r#"
            UPDATE base_rows
            SET row_json = $5
            WHERE ctid = (
                SELECT ctid FROM base_rows
                WHERE deployment_name = $1
                  AND source_schema = $2
                  AND source_table = $3
                  AND row_json::jsonb @> $4::jsonb
                ORDER BY row_ordinal
                LIMIT 1
            )
            "#,
    )
    .bind(deployment_name)
    .bind(source_schema)
    .bind(source_table)
    .bind(&identity_json)
    .bind(&row_json)
    .execute(&mut **tx)
    .await
    .map_err(PlatformStoreError::Persist)?;
    if updated.rows_affected() > 0 {
        return Ok(());
    }
    let next_ordinal: i32 = sqlx::query_scalar(
        r#"
            SELECT COALESCE(MAX(row_ordinal), -1) + 1
            FROM base_rows
            WHERE deployment_name = $1 AND source_schema = $2 AND source_table = $3
            "#,
    )
    .bind(deployment_name)
    .bind(source_schema)
    .bind(source_table)
    .fetch_one(&mut **tx)
    .await
    .map_err(PlatformStoreError::Persist)?;
    sqlx::query(
        r#"
            INSERT INTO base_rows (
                deployment_name, source_schema, source_table, row_ordinal, row_json
            ) VALUES ($1, $2, $3, $4, $5)
            "#,
    )
    .bind(deployment_name)
    .bind(source_schema)
    .bind(source_table)
    .bind(next_ordinal)
    .bind(&row_json)
    .execute(&mut **tx)
    .await
    .map_err(PlatformStoreError::Persist)?;
    Ok(())
}

async fn delete_base_row_by_identity_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    deployment_name: &str,
    source_schema: &str,
    source_table: &str,
    identity: &serde_json::Map<String, serde_json::Value>,
) -> Result<(), PlatformStoreError> {
    let identity_json = identity_json_for_containment(identity)?;
    sqlx::query(
        r#"
            DELETE FROM base_rows
            WHERE ctid = (
                SELECT ctid FROM base_rows
                WHERE deployment_name = $1
                  AND source_schema = $2
                  AND source_table = $3
                  AND row_json::jsonb @> $4::jsonb
                ORDER BY row_ordinal
                LIMIT 1
            )
            "#,
    )
    .bind(deployment_name)
    .bind(source_schema)
    .bind(source_table)
    .bind(&identity_json)
    .execute(&mut **tx)
    .await
    .map_err(PlatformStoreError::Persist)?;
    Ok(())
}

async fn replace_base_dataset_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    dataset: &BaseDataset,
    rows: &[serde_json::Map<String, serde_json::Value>],
) -> Result<(), PlatformStoreError> {
    sqlx::query(
        r#"
            DELETE FROM base_rows
            WHERE deployment_name = $1 AND source_schema = $2 AND source_table = $3
            "#,
    )
    .bind(&dataset.deployment_name)
    .bind(&dataset.source_schema)
    .bind(&dataset.source_table)
    .execute(&mut **tx)
    .await
    .map_err(PlatformStoreError::Persist)?;

    upsert_base_dataset_metadata_in_tx(tx, dataset).await?;
    insert_base_rows_bulk_in_tx(
        tx,
        &dataset.deployment_name,
        &dataset.source_schema,
        &dataset.source_table,
        rows,
        0,
    )
    .await?;
    Ok(())
}

async fn delete_base_datasets_not_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    deployment_name: &str,
    keep_tables: &[(String, String)],
) -> Result<(), PlatformStoreError> {
    let existing = sqlx::query_as::<_, (String, String)>(
        r#"
            SELECT source_schema, source_table
            FROM base_datasets
            WHERE deployment_name = $1
            "#,
    )
    .bind(deployment_name)
    .fetch_all(&mut **tx)
    .await
    .map_err(PlatformStoreError::Persist)?;

    for (schema, table) in existing {
        let keep = keep_tables.iter().any(|(s, t)| s == &schema && t == &table);
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
        .execute(&mut **tx)
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
        .execute(&mut **tx)
        .await
        .map_err(PlatformStoreError::Persist)?;
    }
    Ok(())
}

async fn record_applied_source_changes_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    deployment_name: &str,
    source_schema: &str,
    source_table: &str,
    changes: &[(String, i64)],
) -> Result<(), PlatformStoreError> {
    if changes.is_empty() {
        return Ok(());
    }
    // Bulk UNNEST insert for Direct Incremental window batches (#252).
    let change_ids: Vec<&str> = changes.iter().map(|(id, _)| id.as_str()).collect();
    let positions: Vec<i64> = changes.iter().map(|(_, pos)| *pos).collect();
    let deployment_names = vec![deployment_name; changes.len()];
    let source_schemas = vec![source_schema; changes.len()];
    let source_tables = vec![source_table; changes.len()];
    sqlx::query(
        r#"
            INSERT INTO applied_source_changes (
                deployment_name, source_schema, source_table, change_id, position
            )
            SELECT * FROM UNNEST(
                $1::text[], $2::text[], $3::text[], $4::text[], $5::bigint[]
            )
            ON CONFLICT (deployment_name, source_schema, source_table, change_id) DO NOTHING
            "#,
    )
    .bind(&deployment_names)
    .bind(&source_schemas)
    .bind(&source_tables)
    .bind(&change_ids)
    .bind(&positions)
    .execute(&mut **tx)
    .await
    .map_err(PlatformStoreError::Persist)?;
    Ok(())
}

async fn upsert_schema_change_impact_in_tx(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    record: &SchemaChangeImpact,
) -> Result<(), PlatformStoreError> {
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
    .execute(&mut **tx)
    .await
    .map_err(PlatformStoreError::Persist)?;
    Ok(())
}

/// Holds a Platform Store session advisory lock for one Incremental Capture cycle.
///
/// Dropping the guard closes the backend connection (via [`PoolConnection::close_on_drop`])
/// so PostgreSQL releases the session advisory lock. Do not return the locked
/// connection to a shared pool — that would leave the lock held across cycles.
pub struct IncrementalSyncLock {
    _conn: PoolConnection<Postgres>,
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


/// Apply only migrations with version `<= through_version` (inclusive).
///
/// Upgrade-smoke / CI helper for seeding a prior-release Platform Store schema.
/// Production operators use `PlatformStore::migrate` (or `migraloop run` / `migraloop migrate`),
/// which always applies every pending migration.
#[doc(hidden)]
pub async fn migrate_through(
    database_url: &str,
    through_version: i64,
) -> Result<(), PlatformStoreError> {
    let store = PlatformStore::open(database_url).await?;
    store.migrate_through(through_version).await
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
                    kind: parse_secret_ref_kind(&self.source_password_ref_kind)?,
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
                    kind: parse_secret_ref_kind(&self.target_password_ref_kind)?,
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
        let transform_value: serde_json::Value =
            serde_json::from_str(&self.transform_json).map_err(PlatformStoreError::InvalidJson)?;
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
                Some(raw) => {
                    Some(serde_json::from_str(raw).map_err(PlatformStoreError::InvalidJson)?)
                }
            },
        })
    }
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
        let data = value.as_object().cloned().ok_or_else(|| {
            PlatformStoreError::NotFound("stored Base row is not a JSON object".to_string())
        })?;
        Ok(BaseRow {
            row_ordinal: self.row_ordinal,
            data,
        })
    }
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




#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_column_is_shared_column_shape() {
        let shape = ColumnShape {
            name: "ID".into(),
            data_type: "NUMBER".into(),
            precision: Some(10),
            scale: Some(0),
        };
        let column: BaseColumn = shape.clone();
        assert_eq!(column.data_type, "NUMBER");
        assert_eq!(column, shape);
    }

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
        assert!(!platform_store_url_requires_tls("postgres://u:p@h:5432/db"));
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
