//! Platform Store Guardrails and warn-only resource thresholds (ADR-0010 / issue #28).
//!
//! Product-enforced minimums reject absurdly low bundled Postgres settings.
//! Free-disk pressure crosses a warn threshold only — never auto-pauses Pipelines.

use std::path::Path;
use std::process::Command;

use sqlx::PgPool;
use thiserror::Error;

use crate::PlatformStoreError;

/// Minimum `shared_buffers` the bundled Platform Store may run with.
pub const MIN_SHARED_BUFFERS_BYTES: u64 = 64 * 1024 * 1024;

/// Minimum `work_mem` the bundled Platform Store may run with.
pub const MIN_WORK_MEM_BYTES: u64 = 4 * 1024 * 1024;

/// Minimum `maintenance_work_mem` the bundled Platform Store may run with.
pub const MIN_MAINTENANCE_WORK_MEM_BYTES: u64 = 64 * 1024 * 1024;

/// Minimum `max_connections` the bundled Platform Store may run with.
pub const MIN_MAX_CONNECTIONS: u32 = 20;

/// Free-disk warn threshold. Crossing this surfaces a warning only (no auto-pause).
pub const DISK_FREE_WARN_BYTES: u64 = 1024 * 1024 * 1024;

/// Observed Platform Store Postgres settings relevant to guardrails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformStoreSettings {
    pub shared_buffers_bytes: u64,
    pub work_mem_bytes: u64,
    pub maintenance_work_mem_bytes: u64,
    pub max_connections: u32,
}

/// Observed Platform Store resource signals (warn-only thresholds).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlatformStoreResourceStatus {
    /// Free bytes on the store data volume when known.
    pub free_disk_bytes: Option<u64>,
    /// True when free disk is known and below [`DISK_FREE_WARN_BYTES`].
    pub disk_warn: bool,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum GuardrailError {
    #[error(
        "Platform Store Guardrails rejected settings: {summary}. \
         Raise the bundled Postgres settings (see compose defaults / handbook) \
         or stop overriding them below product minimums (ADR-0010)."
    )]
    BelowMinimum { summary: String },
}

/// Validate observed store settings against product-enforced minimums.
pub fn check_store_settings(settings: &PlatformStoreSettings) -> Result<(), GuardrailError> {
    let mut problems = Vec::new();

    if settings.shared_buffers_bytes < MIN_SHARED_BUFFERS_BYTES {
        problems.push(format!(
            "shared_buffers={} < minimum {}",
            format_bytes(settings.shared_buffers_bytes),
            format_bytes(MIN_SHARED_BUFFERS_BYTES)
        ));
    }
    if settings.work_mem_bytes < MIN_WORK_MEM_BYTES {
        problems.push(format!(
            "work_mem={} < minimum {}",
            format_bytes(settings.work_mem_bytes),
            format_bytes(MIN_WORK_MEM_BYTES)
        ));
    }
    if settings.maintenance_work_mem_bytes < MIN_MAINTENANCE_WORK_MEM_BYTES {
        problems.push(format!(
            "maintenance_work_mem={} < minimum {}",
            format_bytes(settings.maintenance_work_mem_bytes),
            format_bytes(MIN_MAINTENANCE_WORK_MEM_BYTES)
        ));
    }
    if settings.max_connections < MIN_MAX_CONNECTIONS {
        problems.push(format!(
            "max_connections={} < minimum {}",
            settings.max_connections, MIN_MAX_CONNECTIONS
        ));
    }

    if problems.is_empty() {
        Ok(())
    } else {
        Err(GuardrailError::BelowMinimum {
            summary: problems.join("; "),
        })
    }
}

/// Probe live `pg_settings`, honoring test injectors when set.
///
/// Inject env (test / fault injection only; unset = real probe):
/// - `MIGRALOOP_INJECT_PLATFORM_STORE_SHARED_BUFFERS_BYTES`
/// - `MIGRALOOP_INJECT_PLATFORM_STORE_WORK_MEM_BYTES`
/// - `MIGRALOOP_INJECT_PLATFORM_STORE_MAINTENANCE_WORK_MEM_BYTES`
/// - `MIGRALOOP_INJECT_PLATFORM_STORE_MAX_CONNECTIONS`
pub async fn probe_store_settings(
    database_url: &str,
) -> Result<PlatformStoreSettings, PlatformStoreError> {
    let pool = crate::connect(database_url).await?;
    probe_store_settings_on_pool(&pool).await
}

pub(crate) async fn probe_store_settings_on_pool(
    pool: &PgPool,
) -> Result<PlatformStoreSettings, PlatformStoreError> {
    // Memory GUCs: pg_size_bytes(current_setting(..., true)) yields bytes with units.
    let shared = setting_bytes_from_pg(pool, "shared_buffers").await?;
    let work = setting_bytes_from_pg(pool, "work_mem").await?;
    let maint = setting_bytes_from_pg(pool, "maintenance_work_mem").await?;
    let max_conn_raw = sqlx::query_scalar::<_, String>(
        "SELECT setting FROM pg_settings WHERE name = 'max_connections'",
    )
    .fetch_one(pool)
    .await
    .map_err(PlatformStoreError::Load)?;
    let max_conn: u32 = max_conn_raw.parse().map_err(|err| {
        PlatformStoreError::Load(sqlx::Error::Protocol(format!(
            "invalid max_connections from pg_settings: {err}"
        )))
    })?;

    let mut settings = PlatformStoreSettings {
        shared_buffers_bytes: shared,
        work_mem_bytes: work,
        maintenance_work_mem_bytes: maint,
        max_connections: max_conn,
    };

    if let Some(v) = inject_u64("MIGRALOOP_INJECT_PLATFORM_STORE_SHARED_BUFFERS_BYTES") {
        settings.shared_buffers_bytes = v;
    }
    if let Some(v) = inject_u64("MIGRALOOP_INJECT_PLATFORM_STORE_WORK_MEM_BYTES") {
        settings.work_mem_bytes = v;
    }
    if let Some(v) = inject_u64("MIGRALOOP_INJECT_PLATFORM_STORE_MAINTENANCE_WORK_MEM_BYTES") {
        settings.maintenance_work_mem_bytes = v;
    }
    if let Some(v) = inject_u64("MIGRALOOP_INJECT_PLATFORM_STORE_MAX_CONNECTIONS") {
        settings.max_connections = v as u32;
    }

    Ok(settings)
}

async fn setting_bytes_from_pg(pool: &PgPool, name: &str) -> Result<u64, PlatformStoreError> {
    // pg_size_bytes accepts the pretty form from current_setting(..., true).
    let bytes = sqlx::query_scalar::<_, i64>(&format!(
        "SELECT pg_size_bytes(current_setting('{name}', true))"
    ))
    .fetch_one(pool)
    .await
    .map_err(PlatformStoreError::Load)?;
    if bytes < 0 {
        return Err(PlatformStoreError::Load(sqlx::Error::Protocol(format!(
            "negative byte size for setting {name}"
        ))));
    }
    Ok(bytes as u64)
}

/// Probe free-disk / resource warn state.
///
/// Resolution order for free disk bytes:
/// 1. `MIGRALOOP_INJECT_PLATFORM_STORE_FREE_DISK_BYTES` (tests)
/// 2. `MIGRALOOP_PLATFORM_STORE_FREE_DISK_BYTES` (operator/orchestrator supplied)
/// 3. `stat`/`df` on `MIGRALOOP_PLATFORM_STORE_DATA_DIR` when set (compose mounts
///    the store volume into the app for this purpose)
/// 4. Unknown (`None`) — no warn when free disk cannot be observed
pub async fn probe_store_resources(
    _database_url: &str,
) -> Result<PlatformStoreResourceStatus, PlatformStoreError> {
    let free = observe_free_disk_bytes();
    let disk_warn = free.is_some_and(|b| b < DISK_FREE_WARN_BYTES);
    Ok(PlatformStoreResourceStatus {
        free_disk_bytes: free,
        disk_warn,
    })
}

fn observe_free_disk_bytes() -> Option<u64> {
    if let Some(v) = inject_u64("MIGRALOOP_INJECT_PLATFORM_STORE_FREE_DISK_BYTES") {
        return Some(v);
    }
    if let Some(v) = inject_u64("MIGRALOOP_PLATFORM_STORE_FREE_DISK_BYTES") {
        return Some(v);
    }
    let data_dir = std::env::var("MIGRALOOP_PLATFORM_STORE_DATA_DIR").ok()?;
    free_disk_bytes_on_path(Path::new(&data_dir))
}

fn free_disk_bytes_on_path(path: &Path) -> Option<u64> {
    if !path.exists() {
        return None;
    }
    // Linux `df -B1 --output=avail <path>`: last non-header line is available bytes.
    let output = Command::new("df")
        .args(["-B1", "--output=avail"])
        .arg(path)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && *line != "Avail" && line.chars().all(|c| c.is_ascii_digit()))
        .last()
        .and_then(|n| n.parse().ok())
}

fn inject_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.parse().ok())
}

/// Format byte counts for Operator-visible messages (MiB / GiB when aligned).
pub fn format_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    if bytes >= GIB && bytes % GIB == 0 {
        format!("{}GiB", bytes / GIB)
    } else if bytes >= MIB && bytes % MIB == 0 {
        format!("{}MiB", bytes / MIB)
    } else if bytes >= KIB && bytes % KIB == 0 {
        format!("{}KiB", bytes / KIB)
    } else {
        format!("{bytes}B")
    }
}

/// Operator-visible WARN line when free disk is below the warn threshold.
pub fn disk_warn_message(free_disk_bytes: u64) -> String {
    format!(
        "WARN: Platform Store free disk below threshold ({} < {}) — warn only; \
Pipelines are not auto-paused for disk pressure (ADR-0010)",
        format_bytes(free_disk_bytes),
        format_bytes(DISK_FREE_WARN_BYTES)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_settings_at_or_above_minimums() {
        let settings = PlatformStoreSettings {
            shared_buffers_bytes: MIN_SHARED_BUFFERS_BYTES,
            work_mem_bytes: MIN_WORK_MEM_BYTES,
            maintenance_work_mem_bytes: MIN_MAINTENANCE_WORK_MEM_BYTES,
            max_connections: MIN_MAX_CONNECTIONS,
        };
        assert!(check_store_settings(&settings).is_ok());
    }

    #[test]
    fn rejects_absurdly_low_shared_buffers() {
        let settings = PlatformStoreSettings {
            shared_buffers_bytes: 1024 * 1024,
            work_mem_bytes: MIN_WORK_MEM_BYTES,
            maintenance_work_mem_bytes: MIN_MAINTENANCE_WORK_MEM_BYTES,
            max_connections: MIN_MAX_CONNECTIONS,
        };
        let err = check_store_settings(&settings).expect_err("must reject");
        let msg = err.to_string();
        assert!(msg.contains("shared_buffers"), "{msg}");
        assert!(msg.contains("Guardrails rejected"), "{msg}");
    }

    #[test]
    fn rejects_low_max_connections() {
        let settings = PlatformStoreSettings {
            shared_buffers_bytes: MIN_SHARED_BUFFERS_BYTES,
            work_mem_bytes: MIN_WORK_MEM_BYTES,
            maintenance_work_mem_bytes: MIN_MAINTENANCE_WORK_MEM_BYTES,
            max_connections: 5,
        };
        let err = check_store_settings(&settings).expect_err("must reject");
        assert!(err.to_string().contains("max_connections=5"), "{err}");
    }

    #[test]
    fn disk_warn_message_is_warn_only() {
        let msg = disk_warn_message(512 * 1024 * 1024);
        assert!(msg.starts_with("WARN:"), "{msg}");
        assert!(msg.contains("not auto-paused"), "{msg}");
        assert!(msg.contains("512MiB"), "{msg}");
    }

    #[test]
    fn resource_status_warns_only_when_below_threshold() {
        let warn = PlatformStoreResourceStatus {
            free_disk_bytes: Some(DISK_FREE_WARN_BYTES - 1),
            disk_warn: true,
        };
        assert!(warn.disk_warn);
        let ok = PlatformStoreResourceStatus {
            free_disk_bytes: Some(DISK_FREE_WARN_BYTES),
            disk_warn: false,
        };
        assert!(!ok.disk_warn);
    }
}
