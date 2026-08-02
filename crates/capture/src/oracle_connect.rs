//! Shared Oracle OCI connection helpers (Instant Client / ODPI-C via `oracle` crate).

use oracle::{Connection, Connector};

use crate::logminer::OracleSourceConnect;
use crate::CaptureError;

/// Build an Easy Connect / service connect string for Instant Client.
pub fn oracle_connect_string(source: &OracleSourceConnect) -> String {
    format!(
        "//{}:{}/{}",
        source.host.trim(),
        source.port,
        source.database.trim()
    )
}

/// Open a synchronous OCI connection. Maps missing Instant Client to
/// [`CaptureError::OciUnavailable`] (no silent stub fallback).
pub fn connect_oracle(
    source: &OracleSourceConnect,
    password: &str,
) -> Result<Connection, CaptureError> {
    let connect_string = oracle_connect_string(source);
    Connector::new(source.username.trim(), password, &connect_string)
        .connect()
        .map_err(|err| map_oracle_error(&source.host, err))
}

/// Resolve Pipeline `source.schema` (empty → Oracle username as default schema).
pub fn resolve_oracle_schema(source: &OracleSourceConnect, schema: &str) -> String {
    let trimmed = schema.trim();
    if trimmed.is_empty() {
        source.username.trim().to_ascii_uppercase()
    } else {
        trimmed.to_ascii_uppercase()
    }
}

pub fn map_oracle_error(host: &str, err: oracle::Error) -> CaptureError {
    let detail = err.to_string();
    let lower = detail.to_ascii_lowercase();
    if lower.contains("dpi-1047")
        || lower.contains("instant client")
        || lower.contains("libclntsh")
        || lower.contains("cannot load")
        || lower.contains("odpi")
    {
        CaptureError::OciUnavailable {
            host: host.to_string(),
            detail: format!(
                "Oracle Instant Client / OCI libraries are not available in this runtime ({detail}); \
                 use Source host `contract` (or `stub`) for the LogMiner contract harness, \
                 or install Instant Client and set LD_LIBRARY_PATH for real OCI capture"
            ),
        }
    } else {
        CaptureError::OciUnavailable {
            host: host.to_string(),
            detail: format!("Oracle LogMiner (OCI) connection/session failed: {detail}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connect_string_uses_easy_connect() {
        let source = OracleSourceConnect {
            host: "db.example".into(),
            port: 1521,
            database: "FREEPDB1".into(),
            username: "sync_user".into(),
        };
        assert_eq!(oracle_connect_string(&source), "//db.example:1521/FREEPDB1");
    }

    #[test]
    fn empty_schema_defaults_to_username() {
        let source = OracleSourceConnect {
            host: "db.example".into(),
            port: 1521,
            database: "FREEPDB1".into(),
            username: "Sync_User".into(),
        };
        assert_eq!(resolve_oracle_schema(&source, ""), "SYNC_USER");
        assert_eq!(resolve_oracle_schema(&source, "  app  "), "APP");
    }
}
