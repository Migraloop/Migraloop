//! Platform Store: dedicated PostgreSQL data plane for the platform.

use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum PlatformStoreError {
    #[error("failed to connect to Platform Store: {0}")]
    Connect(#[source] sqlx::Error),
    #[error("failed to migrate Platform Store: {0}")]
    Migrate(#[source] sqlx::migrate::MigrateError),
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
