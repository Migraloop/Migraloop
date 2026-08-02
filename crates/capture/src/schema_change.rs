//! Source Schema Change Handling (ADR-0009 / issue #23).
//!
//! Classifies Source DDL against Pipeline dependencies. Blocking impact warns and
//! pauses affected Pipelines; unaffecting / non-blocking changes continue.
//! Distinct from Poison Change quarantine (ADR-0015): this is for stream-wide
//! blockers, not single-row failures.
//!
//! Contract/Lab injection uses [`INJECT_SCHEMA_CHANGES_ENV`] (JSON file path) so
//! operator-seam tests can exercise classification without LogMiner DDL capture.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{CaptureError, CapturePosition};

/// Env var: path to a JSON file of injected Schema Change events (test/Lab).
pub const INJECT_SCHEMA_CHANGES_ENV: &str = "MIGRALOOP_INJECT_SCHEMA_CHANGES";

/// Kind of Source DDL relevant to Pipeline impact classification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaChangeKind {
    /// DROP TABLE (or equivalent) for a synced Source table.
    DropTable,
    /// DROP COLUMN for one or more columns.
    DropColumn { columns: Vec<String> },
    /// ALTER COLUMN / rename / type change that may block safe apply.
    AlterColumn { columns: Vec<String> },
    /// ADD COLUMN (schema may catch up; typically unaffecting for current Pipelines).
    AddColumn { columns: Vec<String> },
    /// Other DDL that does not affect Pipeline dependencies.
    Other,
}

impl SchemaChangeKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::DropTable => "drop_table",
            Self::DropColumn { .. } => "drop_column",
            Self::AlterColumn { .. } => "alter_column",
            Self::AddColumn { .. } => "add_column",
            Self::Other => "other",
        }
    }

    pub fn columns(&self) -> &[String] {
        match self {
            Self::DropColumn { columns } | Self::AlterColumn { columns } | Self::AddColumn { columns } => {
                columns
            }
            Self::DropTable | Self::Other => &[],
        }
    }
}

/// Impact of a Schema Change on one Pipeline (ADR-0009).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaImpact {
    /// Does not affect the Pipeline — processing continues; schema can catch up.
    Unaffecting,
    /// Affects the Pipeline but apply stays safe — processing continues.
    NonBlocking,
    /// Blocks safe apply — warn and pause the affected Pipeline.
    Blocking,
}

impl SchemaImpact {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unaffecting => "unaffecting",
            Self::NonBlocking => "non_blocking",
            Self::Blocking => "blocking",
        }
    }
}

/// One Source Schema Change event from Incremental Capture (or test/Lab injection).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaChangeEvent {
    pub table: String,
    #[serde(default)]
    pub schema: String,
    pub kind: SchemaChangeKind,
    pub position: CapturePosition,
    /// Stable id for Platform Store dedupe (same stream as row change_ids).
    pub change_id: String,
    /// Operator-visible DDL summary.
    pub summary: String,
}

/// Pipeline dependency surface used for Schema Change impact classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineSchemaDeps {
    pub source_table: String,
    pub source_schema: String,
    /// Columns the Pipeline's transform/output depends on (PK + Managed / used fields).
    pub dependency_columns: BTreeSet<String>,
}

/// Stable Platform Store dedupe id for a Schema Change.
pub fn schema_change_id(
    table: &str,
    position: CapturePosition,
    kind: &SchemaChangeKind,
) -> String {
    let mut cols: Vec<String> = kind
        .columns()
        .iter()
        .map(|c| c.to_ascii_uppercase())
        .collect();
    cols.sort();
    format!(
        "schema:{}:{}:{}:{}",
        table.to_ascii_uppercase(),
        position,
        kind.as_str(),
        cols.join(",")
    )
}

/// Classify a Schema Change against one Pipeline's dependencies (ADR-0009).
pub fn classify_schema_impact(
    deps: &PipelineSchemaDeps,
    change: &SchemaChangeEvent,
) -> SchemaImpact {
    if !change.table.eq_ignore_ascii_case(&deps.source_table) {
        return SchemaImpact::Unaffecting;
    }
    // Schema filter: empty deps schema means "any"; otherwise require match.
    if !deps.source_schema.is_empty()
        && !change.schema.is_empty()
        && !change.schema.eq_ignore_ascii_case(&deps.source_schema)
    {
        return SchemaImpact::Unaffecting;
    }

    match &change.kind {
        SchemaChangeKind::DropTable => SchemaImpact::Blocking,
        SchemaChangeKind::DropColumn { columns } | SchemaChangeKind::AlterColumn { columns } => {
            if columns_intersect_deps(columns, &deps.dependency_columns) {
                SchemaImpact::Blocking
            } else {
                // Unused column DDL on a referenced table — schema can catch up.
                SchemaImpact::Unaffecting
            }
        }
        SchemaChangeKind::AddColumn { columns } => {
            if columns_intersect_deps(columns, &deps.dependency_columns) {
                // Newly added column that the Pipeline already lists as a dependency
                // can still be applied safely once schema catches up.
                SchemaImpact::NonBlocking
            } else {
                SchemaImpact::Unaffecting
            }
        }
        SchemaChangeKind::Other => SchemaImpact::Unaffecting,
    }
}

fn columns_intersect_deps(columns: &[String], deps: &BTreeSet<String>) -> bool {
    columns.iter().any(|c| {
        deps.iter()
            .any(|d| d.eq_ignore_ascii_case(c))
    })
}

#[derive(Debug, Deserialize)]
struct InjectFile {
    changes: Vec<InjectChange>,
}

#[derive(Debug, Deserialize)]
struct InjectChange {
    scn: u64,
    table: String,
    #[serde(default)]
    schema: String,
    kind: InjectKind,
    #[serde(default)]
    columns: Vec<String>,
    #[serde(default)]
    summary: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum InjectKind {
    DropTable,
    DropColumn,
    AlterColumn,
    AddColumn,
    Other,
}

#[derive(Debug, Error)]
pub enum SchemaChangeInjectError {
    #[error("schema change inject file error: {0}")]
    Detail(String),
}

impl From<SchemaChangeInjectError> for CaptureError {
    fn from(err: SchemaChangeInjectError) -> Self {
        CaptureError::ContractCatalog {
            detail: err.to_string(),
        }
    }
}

/// Load injected Schema Change events from [`INJECT_SCHEMA_CHANGES_ENV`] when set.
///
/// Returns an empty list when the env var is unset. Used by contract/stub CI twins
/// and Lab Scenario orchestration — not a production Operator control.
pub fn load_injected_schema_changes() -> Result<Vec<SchemaChangeEvent>, SchemaChangeInjectError> {
    let Some(path) = std::env::var_os(INJECT_SCHEMA_CHANGES_ENV) else {
        return Ok(Vec::new());
    };
    let path = Path::new(&path);
    load_schema_changes_file(path)
}

/// Load Schema Change events from a JSON inject file.
pub fn load_schema_changes_file(
    path: &Path,
) -> Result<Vec<SchemaChangeEvent>, SchemaChangeInjectError> {
    let raw = fs::read_to_string(path).map_err(|err| {
        SchemaChangeInjectError::Detail(format!(
            "failed to read {}: {err}",
            path.display()
        ))
    })?;
    let file: InjectFile = serde_json::from_str(&raw).map_err(|err| {
        SchemaChangeInjectError::Detail(format!(
            "invalid schema change inject JSON {}: {err}",
            path.display()
        ))
    })?;

    let mut out = Vec::with_capacity(file.changes.len());
    for entry in file.changes {
        let kind = match entry.kind {
            InjectKind::DropTable => SchemaChangeKind::DropTable,
            InjectKind::DropColumn => SchemaChangeKind::DropColumn {
                columns: entry.columns.clone(),
            },
            InjectKind::AlterColumn => SchemaChangeKind::AlterColumn {
                columns: entry.columns.clone(),
            },
            InjectKind::AddColumn => SchemaChangeKind::AddColumn {
                columns: entry.columns.clone(),
            },
            InjectKind::Other => SchemaChangeKind::Other,
        };
        let position = CapturePosition(entry.scn);
        let change_id = schema_change_id(&entry.table, position, &kind);
        let summary = entry.summary.unwrap_or_else(|| {
            let cols = kind.columns().join(", ");
            if cols.is_empty() {
                format!("{} {}", kind.as_str(), entry.table)
            } else {
                format!("{} {} ({cols})", kind.as_str(), entry.table)
            }
        });
        out.push(SchemaChangeEvent {
            table: entry.table,
            schema: entry.schema,
            kind,
            position,
            change_id,
            summary,
        });
    }
    out.sort_by(|a, b| {
        a.position
            .cmp(&b.position)
            .then_with(|| a.change_id.cmp(&b.change_id))
    });
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deps(table: &str, columns: &[&str]) -> PipelineSchemaDeps {
        PipelineSchemaDeps {
            source_table: table.to_string(),
            source_schema: "APP".to_string(),
            dependency_columns: columns.iter().map(|c| (*c).to_string()).collect(),
        }
    }

    fn change(table: &str, kind: SchemaChangeKind) -> SchemaChangeEvent {
        let position = CapturePosition(1045);
        let change_id = schema_change_id(table, position, &kind);
        SchemaChangeEvent {
            table: table.to_string(),
            schema: "APP".to_string(),
            kind,
            position,
            change_id,
            summary: "test".to_string(),
        }
    }

    #[test]
    fn drop_managed_column_is_blocking() {
        let impact = classify_schema_impact(
            &deps("CUSTOMERS", &["ID", "NAME", "EMAIL"]),
            &change(
                "CUSTOMERS",
                SchemaChangeKind::DropColumn {
                    columns: vec!["NAME".to_string()],
                },
            ),
        );
        assert_eq!(impact, SchemaImpact::Blocking);
    }

    #[test]
    fn add_unused_column_is_unaffecting() {
        let impact = classify_schema_impact(
            &deps("CUSTOMERS", &["ID", "NAME"]),
            &change(
                "CUSTOMERS",
                SchemaChangeKind::AddColumn {
                    columns: vec!["NOTES".to_string()],
                },
            ),
        );
        assert_eq!(impact, SchemaImpact::Unaffecting);
    }

    #[test]
    fn ddl_on_other_table_is_unaffecting() {
        let impact = classify_schema_impact(
            &deps("CUSTOMERS", &["ID", "NAME"]),
            &change("ORDERS", SchemaChangeKind::DropTable),
        );
        assert_eq!(impact, SchemaImpact::Unaffecting);
    }

    #[test]
    fn drop_unused_column_is_unaffecting() {
        let impact = classify_schema_impact(
            &deps("CUSTOMERS", &["ID", "NAME"]),
            &change(
                "CUSTOMERS",
                SchemaChangeKind::DropColumn {
                    columns: vec!["BIO".to_string()],
                },
            ),
        );
        assert_eq!(impact, SchemaImpact::Unaffecting);
    }
}
