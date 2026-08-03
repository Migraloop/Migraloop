//! Shared Oracle OCI connection helpers (Instant Client / ODPI-C via `oracle` crate).

use oracle::{Connection, Connector};

use crate::logminer::OracleSourceConnect;
use crate::CaptureError;

/// Build an Easy Connect / Easy Connect Plus string for Instant Client.
///
/// Cleartext uses `//host:port/service`. When TLS is enabled, builds a TCPS
/// DESCRIPTION connect string so Instant Client cannot silently fall back to TCP.
pub fn oracle_connect_string(source: &OracleSourceConnect) -> String {
    let host = source.host.trim();
    let service = source.database.trim();
    if !source.tls.enabled {
        return format!("//{host}:{}/{}", source.port, service);
    }

    let dn_match = if source.tls.insecure_skip_verify {
        "no"
    } else {
        "yes"
    };
    let mut security = format!("(SECURITY=(SSL_SERVER_DN_MATCH={dn_match})");
    let wallet = source.tls.wallet_location.trim();
    if !wallet.is_empty() {
        // Escape closing parens in path by rejecting them elsewhere; paths are
        // operator-supplied filesystem locations without DESCRIPTION metachar intent.
        security.push_str(&format!("(MY_WALLET_DIRECTORY={wallet})"));
    }
    security.push(')');

    format!(
        "(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST={host})(PORT={}))\
         (CONNECT_DATA=(SERVICE_NAME={service})){security})",
        source.port
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
        .map_err(|err| map_oracle_connect_error(&source.host, source.tls.enabled, err))
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

/// Map post-connect OCI errors (TLS already negotiated or cleartext session).
pub fn map_oracle_error(host: &str, err: oracle::Error) -> CaptureError {
    map_oracle_connect_error(host, false, err)
}

fn map_oracle_connect_error(host: &str, tls_enabled: bool, err: oracle::Error) -> CaptureError {
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
    } else if tls_enabled {
        CaptureError::OciUnavailable {
            host: host.to_string(),
            detail: format!(
                "Oracle Source TLS (TCPS) was requested but could not be established: {detail} \
                 (no silent cleartext fallback)"
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
    use migraloop_types::TlsSettings;

    fn base_source() -> OracleSourceConnect {
        OracleSourceConnect {
            host: "db.example".into(),
            port: 1521,
            database: "FREEPDB1".into(),
            username: "sync_user".into(),
            tls: TlsSettings::default(),
        }
    }

    #[test]
    fn connect_string_uses_easy_connect_when_cleartext() {
        let source = base_source();
        assert_eq!(oracle_connect_string(&source), "//db.example:1521/FREEPDB1");
    }

    #[test]
    fn connect_string_uses_tcps_description_when_tls_enabled() {
        let mut source = base_source();
        source.port = 2484;
        source.tls = TlsSettings {
            enabled: true,
            ca_file: String::new(),
            wallet_location: "/etc/oracle/wallet".into(),
            insecure_skip_verify: false,
        };
        let s = oracle_connect_string(&source);
        assert!(s.contains("PROTOCOL=tcps"), "got {s}");
        assert!(s.contains("HOST=db.example"), "got {s}");
        assert!(s.contains("PORT=2484"), "got {s}");
        assert!(s.contains("SERVICE_NAME=FREEPDB1"), "got {s}");
        assert!(s.contains("SSL_SERVER_DN_MATCH=yes"), "got {s}");
        assert!(s.contains("MY_WALLET_DIRECTORY=/etc/oracle/wallet"), "got {s}");
        assert!(!s.starts_with("//"), "must not use cleartext Easy Connect: {s}");
    }

    #[test]
    fn tls_insecure_skip_verify_disables_dn_match() {
        let mut source = base_source();
        source.tls = TlsSettings {
            enabled: true,
            ca_file: String::new(),
            wallet_location: String::new(),
            insecure_skip_verify: true,
        };
        let s = oracle_connect_string(&source);
        assert!(s.contains("SSL_SERVER_DN_MATCH=no"), "got {s}");
    }

    #[test]
    fn empty_schema_defaults_to_username() {
        let mut source = base_source();
        source.username = "Sync_User".into();
        assert_eq!(resolve_oracle_schema(&source, ""), "SYNC_USER");
        assert_eq!(resolve_oracle_schema(&source, "  app  "), "APP");
    }
}
