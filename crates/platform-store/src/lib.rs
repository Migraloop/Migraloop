//! Platform Store: dedicated PostgreSQL data plane for the platform.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PlatformStoreError {
    #[error("failed to connect to Platform Store: {0}")]
    Connect(#[source] sqlx::Error),
    #[error("failed to migrate Platform Store: {0}")]
    Migrate(#[source] sqlx::migrate::MigrateError),
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

/// Non-secret connection configuration for a Source or Target System.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemConnection {
    pub kind: String,
    pub host: String,
    pub port: i32,
    pub database: String,
    pub username: String,
    pub password_ref: SecretRef,
}

/// Durable Deployment pairing one Source System with one Target System.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Deployment {
    pub name: String,
    pub source: SystemConnection,
    pub target: SystemConnection,
}

/// A Pipeline declared inside a Deployment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
}

/// Supported column kept in a Base Dataset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BaseColumn {
    pub name: String,
    pub oracle_type: String,
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

async fn connect(database_url: &str) -> Result<PgPool, PlatformStoreError> {
    PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(Duration::from_secs(3))
        .connect(database_url)
        .await
        .map_err(PlatformStoreError::Connect)
}

/// Apply versioned Platform Store schema migrations.
pub async fn migrate(database_url: &str) -> Result<(), PlatformStoreError> {
    let pool = connect(database_url).await?;
    sqlx::migrate!("./migrations")
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
            source_password_ref_kind, source_password_ref_value,
            target_kind, target_host, target_port, target_database, target_username,
            target_password_ref_kind, target_password_ref_value,
            applied_at
        ) VALUES (
            $1,
            $2, $3, $4, $5, $6,
            $7, $8,
            $9, $10, $11, $12, $13,
            $14, $15,
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
            target_kind = EXCLUDED.target_kind,
            target_host = EXCLUDED.target_host,
            target_port = EXCLUDED.target_port,
            target_database = EXCLUDED.target_database,
            target_username = EXCLUDED.target_username,
            target_password_ref_kind = EXCLUDED.target_password_ref_kind,
            target_password_ref_value = EXCLUDED.target_password_ref_value,
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
    .bind(&deployment.target.kind)
    .bind(&deployment.target.host)
    .bind(deployment.target.port)
    .bind(&deployment.target.database)
    .bind(&deployment.target.username)
    .bind(deployment.target.password_ref.kind.as_str())
    .bind(&deployment.target.password_ref.value)
    .execute(&pool)
    .await
    .map_err(PlatformStoreError::Persist)?;
    Ok(())
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
        sqlx::query(
            r#"
            INSERT INTO pipelines (
                deployment_name, name, mode, source_table, source_schema,
                target_collection, delivery_status, delivery_applied_changes, applied_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now())
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

    sqlx::query(
        r#"
        INSERT INTO base_datasets (
            deployment_name, source_table, source_schema, status,
            primary_key_json, columns_json, omitted_columns_json, row_count,
            sync_applied_changes, sync_health,
            capture_low_watermark, capture_checkpoint, loaded_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, now())
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
            source_password_ref_kind, source_password_ref_value,
            target_kind, target_host, target_port, target_database, target_username,
            target_password_ref_kind, target_password_ref_value
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
               target_collection, delivery_status, delivery_applied_changes
        FROM pipelines
        ORDER BY deployment_name, name
        "#,
    )
    .fetch_all(&pool)
    .await
    .map_err(PlatformStoreError::Load)?;

    Ok(rows.into_iter().map(PipelineRow::into_pipeline).collect())
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

/// Update Delivery status and optionally accumulate applied Output Identity changes.
pub async fn update_pipeline_delivery_progress(
    database_url: &str,
    deployment_name: &str,
    pipeline_name: &str,
    delivery_status: &str,
    additional_applied_changes: Option<i32>,
) -> Result<(), PlatformStoreError> {
    let pool = connect(database_url).await?;
    let result = if let Some(additional) = additional_applied_changes {
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
    } else {
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
            capture_low_watermark, capture_checkpoint
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
                capture_low_watermark, capture_checkpoint
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
                capture_low_watermark, capture_checkpoint
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
    target_kind: String,
    target_host: String,
    target_port: i32,
    target_database: String,
    target_username: String,
    target_password_ref_kind: String,
    target_password_ref_value: String,
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
}

impl PipelineRow {
    fn into_pipeline(self) -> Pipeline {
        Pipeline {
            deployment_name: self.deployment_name,
            name: self.name,
            mode: self.mode,
            source_table: self.source_table,
            source_schema: self.source_schema,
            target_collection: self.target_collection,
            delivery_status: self.delivery_status,
            delivery_applied_changes: self.delivery_applied_changes,
        }
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
        })
    }
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
