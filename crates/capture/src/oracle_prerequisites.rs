//! Oracle Source Prerequisites for LogMiner-backed Sync (ADR-0021).
//!
//! The platform probes Source settings and fails fast when unmet. It never
//! auto-alters customer Oracle configuration to "fix" prerequisites.

use std::collections::BTreeSet;

use thiserror::Error;

/// Minimum redo/archive retention the platform requires before capture runs.
///
/// Operators must retain redo long enough for Initial Load overlap, Incremental
/// Capture lag, and restart resume. v1 documents 24 hours as the floor.
pub const MIN_REDO_RETENTION_HOURS: u32 = 24;

/// Read-only observation of Oracle Source Prerequisite state.
///
/// Produced by a Source probe (LogMiner contract harness or OCI). Callers must
/// treat this as observational — never write these values back to Oracle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OracleSourcePrerequisiteState {
    /// `ALTER DATABASE ADD SUPPLEMENTAL LOG DATA` (MINIMUM) is enabled.
    pub database_supplemental_logging: bool,
    /// Tables that have PRIMARY KEY or ALL COLUMNS supplemental logging.
    pub tables_with_key_supplemental_logging: BTreeSet<String>,
    /// Configured redo/archive retention in hours (as reported by the probe).
    pub redo_retention_hours: u32,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum PrerequisiteError {
    #[error(
        "Oracle Source Prerequisites not met: {summary}. \
         The platform does not automatically alter Source System settings; \
         fix these on Oracle, then re-run. See handbook/en/source-system.md"
    )]
    Unmet { summary: String },
}

/// Validate observed Oracle Source Prerequisites for the tables about to be captured.
///
/// Checks (ADR-0021 examples): database supplemental logging, per-table key
/// supplemental logging for each required table, and sufficient redo retention.
pub fn check_oracle_source_prerequisites(
    state: &OracleSourcePrerequisiteState,
    required_tables: &[impl AsRef<str>],
) -> Result<(), PrerequisiteError> {
    let mut problems = Vec::new();

    if !state.database_supplemental_logging {
        problems.push(
            "database supplemental logging is not enabled \
             (need ALTER DATABASE ADD SUPPLEMENTAL LOG DATA)"
                .to_string(),
        );
    }

    for table in required_tables {
        let table = table.as_ref();
        if table.is_empty() {
            continue;
        }
        let covered = state
            .tables_with_key_supplemental_logging
            .iter()
            .any(|t| t.eq_ignore_ascii_case(table));
        if !covered {
            problems.push(format!(
                "table {table} is missing PRIMARY KEY or ALL COLUMNS supplemental logging"
            ));
        }
    }

    if state.redo_retention_hours < MIN_REDO_RETENTION_HOURS {
        problems.push(format!(
            "redo retention is insufficient ({}h < required {}h)",
            state.redo_retention_hours, MIN_REDO_RETENTION_HOURS
        ));
    }

    if problems.is_empty() {
        Ok(())
    } else {
        Err(PrerequisiteError::Unmet {
            summary: problems.join("; "),
        })
    }
}

/// Probe stub Source Prerequisite state for tests and early slices.
///
/// Env overrides (unset = satisfied defaults so existing seam tests keep passing):
/// - `MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING` — `on` (default) or `off`
/// - `MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING` — `all` (default), empty, or
///   comma-separated table names that have PK/ALL supplemental logging
/// - `MIGRALOOP_STUB_REDO_RETENTION_HOURS` — integer hours (default 72)
///
/// This probe is read-only: it never mutates Oracle / stub Source settings.
pub fn probe_oracle_source_prerequisites_stub() -> OracleSourcePrerequisiteState {
    let database_supplemental_logging = match std::env::var("MIGRALOOP_STUB_SUPPLEMENTAL_LOGGING")
    {
        Ok(value) => {
            let normalized = value.trim().to_ascii_lowercase();
            !(normalized == "off" || normalized == "0" || normalized == "false" || normalized == "no")
        }
        Err(_) => true,
    };

    let tables_with_key_supplemental_logging =
        match std::env::var("MIGRALOOP_STUB_TABLE_SUPPLEMENTAL_LOGGING") {
            Ok(value) => {
                let trimmed = value.trim();
                if trimmed.eq_ignore_ascii_case("all") {
                    stub_all_known_tables()
                } else if trimmed.is_empty() {
                    BTreeSet::new()
                } else {
                    trimmed
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                }
            }
            Err(_) => stub_all_known_tables(),
        };

    let redo_retention_hours = std::env::var("MIGRALOOP_STUB_REDO_RETENTION_HOURS")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(72);

    OracleSourcePrerequisiteState {
        database_supplemental_logging,
        tables_with_key_supplemental_logging,
        redo_retention_hours,
    }
}

fn stub_all_known_tables() -> BTreeSet<String> {
    // "all" follows the injected process catalog so arbitrary tables (issue #40)
    // satisfy supplemental-logging probes without a hard-coded business list.
    match crate::load_contract_source_catalog() {
        Ok(catalog) => catalog.table_names().into_iter().collect(),
        // Bad env JSON: empty set (fail-fast on table logging), never reintroduce
        // named scenario fixtures on the product path (issue #120).
        Err(_) => BTreeSet::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn satisfied_state_allows_proceed() {
        let state = OracleSourcePrerequisiteState {
            database_supplemental_logging: true,
            tables_with_key_supplemental_logging: BTreeSet::from(["CUSTOMERS".into()]),
            redo_retention_hours: MIN_REDO_RETENTION_HOURS,
        };
        assert!(check_oracle_source_prerequisites(&state, &["CUSTOMERS"]).is_ok());
    }

    #[test]
    fn missing_database_supplemental_logging_fails() {
        let state = OracleSourcePrerequisiteState {
            database_supplemental_logging: false,
            tables_with_key_supplemental_logging: BTreeSet::from(["CUSTOMERS".into()]),
            redo_retention_hours: 72,
        };
        let err = check_oracle_source_prerequisites(&state, &["CUSTOMERS"]).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("supplemental logging"));
        assert!(msg.contains("does not automatically alter"));
    }

    #[test]
    fn missing_table_supplemental_logging_names_table() {
        let state = OracleSourcePrerequisiteState {
            database_supplemental_logging: true,
            tables_with_key_supplemental_logging: BTreeSet::from(["ORDERS".into()]),
            redo_retention_hours: 72,
        };
        let err = check_oracle_source_prerequisites(&state, &["CUSTOMERS"]).unwrap_err();
        assert!(err.to_string().contains("CUSTOMERS"));
    }

    #[test]
    fn insufficient_redo_retention_fails() {
        let state = OracleSourcePrerequisiteState {
            database_supplemental_logging: true,
            tables_with_key_supplemental_logging: BTreeSet::from(["CUSTOMERS".into()]),
            redo_retention_hours: MIN_REDO_RETENTION_HOURS - 1,
        };
        let err = check_oracle_source_prerequisites(&state, &["CUSTOMERS"]).unwrap_err();
        assert!(err.to_string().contains("redo retention"));
    }
}
