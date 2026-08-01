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
