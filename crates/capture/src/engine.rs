//! Source System engine interface (issue #156 / ADR-0003).
//!
//! Deployment runtime Sync depends on [`SourceEngine`] / [`IncrementalCaptureSession`],
//! not Oracle concrete call sites. v1 adapters: Oracle LogMiner contract harness and
//! OCI, plus [`FakeSource`] for in-process seam tests — no second production Source.
//!
//! Discovered columns expose engine-agnostic [`migraloop_types::ColumnShape`] /
//! [`crate::SourceColumn::data_type`] at the seam (contract #207). Oracle allow-list
//! and type-brand rules stay adapter-private (ADR-0018).

use std::collections::BTreeMap;

use serde_json::Value;

use crate::oracle_prerequisites::{check_oracle_source_prerequisites, PrerequisiteError};
use crate::schema_change::{load_injected_schema_changes, SchemaChangeEvent, SchemaChangeInjectError};
use crate::{
    alignment_check_read_for_source, discover_source_schema, initial_load_chunk_for_source,
    open_oracle_incremental_capture, AlignmentCheckSample, CaptureError, CapturePosition,
    ChangeEvent, IncrementalCapture, InitialLoadChunk, InitialLoadChunkOptions,
    OracleSourceConnect, SourceColumn,
};

/// Opened Incremental Capture session (resume from a durable checkpoint).
pub trait IncrementalCaptureSession: Send {
    fn mechanism_label(&self) -> &'static str;

    fn fetch_changes_in_schema_limited(
        &self,
        schema: &str,
        table: &str,
        from_position: CapturePosition,
        limit: Option<usize>,
    ) -> Result<Vec<ChangeEvent>, CaptureError>;

    fn count_changes_in_schema(
        &self,
        schema: &str,
        table: &str,
        from_position: CapturePosition,
    ) -> Result<usize, CaptureError>;

    /// Prefetch many tables in one Capture session when the adapter can amortize
    /// setup (OCI LogMiner START/END). Default fans out to per-table fetch.
    fn prefetch_tables_limited(
        &self,
        requests: &[(String, String, CapturePosition, Option<usize>)],
    ) -> Result<Vec<Vec<ChangeEvent>>, CaptureError> {
        let mut out = Vec::with_capacity(requests.len());
        for (schema, table, from_position, limit) in requests {
            out.push(self.fetch_changes_in_schema_limited(
                schema,
                table,
                *from_position,
                *limit,
            )?);
        }
        Ok(out)
    }
}

impl IncrementalCaptureSession for IncrementalCapture {
    fn mechanism_label(&self) -> &'static str {
        IncrementalCapture::mechanism_label(self)
    }

    fn fetch_changes_in_schema_limited(
        &self,
        schema: &str,
        table: &str,
        from_position: CapturePosition,
        limit: Option<usize>,
    ) -> Result<Vec<ChangeEvent>, CaptureError> {
        IncrementalCapture::fetch_changes_in_schema_limited(
            self,
            schema,
            table,
            from_position,
            limit,
        )
    }

    fn count_changes_in_schema(
        &self,
        schema: &str,
        table: &str,
        from_position: CapturePosition,
    ) -> Result<usize, CaptureError> {
        IncrementalCapture::count_changes_in_schema(self, schema, table, from_position)
    }

    fn prefetch_tables_limited(
        &self,
        requests: &[(String, String, CapturePosition, Option<usize>)],
    ) -> Result<Vec<Vec<ChangeEvent>>, CaptureError> {
        IncrementalCapture::prefetch_tables_limited(self, requests)
    }
}

/// Source System capture surface used by Sync orchestration.
pub trait SourceEngine: Send {
    type Incremental: IncrementalCaptureSession;

    /// Fail-fast Source Prerequisites for tables about to be captured (ADR-0021).
    fn check_prerequisites(&self, required_tables: &[String]) -> Result<(), CaptureError>;

    /// Schema discovery for a Pipeline-referenced table.
    ///
    /// Returned [`SourceColumn`] values use engine-agnostic [`SourceColumn::data_type`] /
    /// [`migraloop_types::ColumnShape`] as the domain default (issue #207).
    fn discover_schema(&self, schema: &str, table: &str) -> Result<Vec<SourceColumn>, CaptureError>;

    /// Bounded Initial Load chunk (ADR-0004 / issue #124).
    fn initial_load_chunk(
        &self,
        schema: &str,
        table: &str,
        configured_timezone: Option<&str>,
        options: &InitialLoadChunkOptions,
    ) -> Result<InitialLoadChunk, CaptureError>;

    /// Resource-gated Source sample for Source Alignment Check.
    fn alignment_check_read(
        &self,
        schema: &str,
        table: &str,
        max_rows: u32,
        configured_timezone: Option<&str>,
    ) -> Result<AlignmentCheckSample, CaptureError>;

    /// Open Incremental Capture for resume from a durable checkpoint.
    fn open_incremental_capture(&self) -> Result<Self::Incremental, CaptureError>;

    /// Schema-change classification inputs from this Source (may be empty).
    fn schema_change_inputs(&self) -> Result<Vec<SchemaChangeEvent>, CaptureError>;

    /// Operator-visible Source engine / adapter label.
    fn kind_label(&self) -> &'static str;
}

fn map_prerequisite_error(err: PrerequisiteError) -> CaptureError {
    match err {
        PrerequisiteError::Unmet { summary } => CaptureError::PrerequisitesUnmet { summary },
    }
}

fn map_schema_inject_error(err: SchemaChangeInjectError) -> CaptureError {
    CaptureError::ContractCatalog {
        detail: err.to_string(),
    }
}

/// Oracle LogMiner Source adapter — contract harness or OCI (ADR-0003).
#[derive(Debug, Clone)]
pub struct OracleLogMinerSource {
    connect: OracleSourceConnect,
    password: String,
}

impl OracleLogMinerSource {
    pub fn new(connect: OracleSourceConnect, password: impl Into<String>) -> Self {
        Self {
            connect,
            password: password.into(),
        }
    }

    pub fn connect(&self) -> &OracleSourceConnect {
        &self.connect
    }

    pub fn is_contract_harness(&self) -> bool {
        self.connect.is_contract_harness()
    }
}

impl SourceEngine for OracleLogMinerSource {
    type Incremental = IncrementalCapture;

    fn check_prerequisites(&self, required_tables: &[String]) -> Result<(), CaptureError> {
        let capture = open_oracle_incremental_capture(&self.connect, &self.password)?;
        let state = capture.probe_prerequisites()?;
        check_oracle_source_prerequisites(&state, required_tables).map_err(map_prerequisite_error)
    }

    fn discover_schema(&self, schema: &str, table: &str) -> Result<Vec<SourceColumn>, CaptureError> {
        discover_source_schema(&self.connect, &self.password, schema, table)
    }

    fn initial_load_chunk(
        &self,
        schema: &str,
        table: &str,
        configured_timezone: Option<&str>,
        options: &InitialLoadChunkOptions,
    ) -> Result<InitialLoadChunk, CaptureError> {
        initial_load_chunk_for_source(
            &self.connect,
            &self.password,
            schema,
            table,
            configured_timezone,
            options,
        )
    }

    fn alignment_check_read(
        &self,
        schema: &str,
        table: &str,
        max_rows: u32,
        configured_timezone: Option<&str>,
    ) -> Result<AlignmentCheckSample, CaptureError> {
        alignment_check_read_for_source(
            &self.connect,
            &self.password,
            schema,
            table,
            max_rows,
            configured_timezone,
        )
    }

    fn open_incremental_capture(&self) -> Result<Self::Incremental, CaptureError> {
        open_oracle_incremental_capture(&self.connect, &self.password)
    }

    fn schema_change_inputs(&self) -> Result<Vec<SchemaChangeEvent>, CaptureError> {
        // Contract/Lab inject path; OCI has no separate DDL poll in v1.
        load_injected_schema_changes().map_err(map_schema_inject_error)
    }

    fn kind_label(&self) -> &'static str {
        if self.connect.is_contract_harness() {
            "oracle-logminer-contract"
        } else {
            "oracle-logminer-oci"
        }
    }
}

/// One Fake Source table definition for seam tests.
#[derive(Debug, Clone)]
pub struct FakeSourceTable {
    pub columns: Vec<SourceColumn>,
    pub primary_key: Vec<String>,
    pub rows: Vec<BTreeMap<String, Value>>,
    pub low_watermark: CapturePosition,
    pub changes: Vec<ChangeEvent>,
}

/// In-memory Source adapter for engine-seam tests (not a production engine).
#[derive(Debug, Default)]
pub struct FakeSource {
    tables: BTreeMap<String, FakeSourceTable>,
    schema_changes: Vec<SchemaChangeEvent>,
    /// When set, [`SourceEngine::check_prerequisites`] fails with this summary.
    prerequisites_unmet: Option<String>,
}

impl FakeSource {
    pub fn new() -> Self {
        Self {
            tables: BTreeMap::new(),
            schema_changes: Vec::new(),
            prerequisites_unmet: None,
        }
    }

    pub fn with_table(mut self, name: impl Into<String>, table: FakeSourceTable) -> Self {
        let name = name.into().to_ascii_uppercase();
        self.tables.insert(name, table);
        self
    }

    pub fn with_prerequisites_unmet(mut self, summary: impl Into<String>) -> Self {
        self.prerequisites_unmet = Some(summary.into());
        self
    }

    pub fn with_schema_changes(mut self, changes: Vec<SchemaChangeEvent>) -> Self {
        self.schema_changes = changes;
        self
    }

    fn table(&self, table: &str) -> Result<&FakeSourceTable, CaptureError> {
        let key = table.to_ascii_uppercase();
        self.tables
            .get(&key)
            .ok_or_else(|| CaptureError::UnknownTable(table.to_string()))
    }
}

/// Fake Incremental Capture session.
#[derive(Debug, Clone)]
pub struct FakeIncremental {
    changes_by_table: BTreeMap<String, Vec<ChangeEvent>>,
}

impl IncrementalCaptureSession for FakeIncremental {
    fn mechanism_label(&self) -> &'static str {
        "fake"
    }

    fn fetch_changes_in_schema_limited(
        &self,
        _schema: &str,
        table: &str,
        from_position: CapturePosition,
        limit: Option<usize>,
    ) -> Result<Vec<ChangeEvent>, CaptureError> {
        let key = table.to_ascii_uppercase();
        let mut out: Vec<ChangeEvent> = self
            .changes_by_table
            .get(&key)
            .into_iter()
            .flatten()
            .filter(|c| c.position >= from_position)
            .cloned()
            .collect();
        out.sort_by_key(|c| c.position.0);
        if let Some(limit) = limit {
            out.truncate(limit);
        }
        Ok(out)
    }

    fn count_changes_in_schema(
        &self,
        schema: &str,
        table: &str,
        from_position: CapturePosition,
    ) -> Result<usize, CaptureError> {
        Ok(self
            .fetch_changes_in_schema_limited(schema, table, from_position, None)?
            .len())
    }
}

impl SourceEngine for FakeSource {
    type Incremental = FakeIncremental;

    fn check_prerequisites(&self, required_tables: &[String]) -> Result<(), CaptureError> {
        if let Some(summary) = &self.prerequisites_unmet {
            return Err(CaptureError::PrerequisitesUnmet {
                summary: summary.clone(),
            });
        }
        for table in required_tables {
            if table.is_empty() {
                continue;
            }
            let _ = self.table(table)?;
        }
        Ok(())
    }

    fn discover_schema(&self, _schema: &str, table: &str) -> Result<Vec<SourceColumn>, CaptureError> {
        Ok(self.table(table)?.columns.clone())
    }

    fn initial_load_chunk(
        &self,
        _schema: &str,
        table: &str,
        _configured_timezone: Option<&str>,
        options: &InitialLoadChunkOptions,
    ) -> Result<InitialLoadChunk, CaptureError> {
        if options.chunk_size == 0 {
            return Err(CaptureError::ContractCatalog {
                detail: "Initial Load chunk_size must be >= 1".to_string(),
            });
        }
        let t = self.table(table)?;
        let watermark = options
            .established_watermark
            .unwrap_or(t.low_watermark);
        let slice: Vec<_> = t
            .rows
            .iter()
            .skip(options.offset)
            .take(options.chunk_size)
            .cloned()
            .collect();
        let exhausted = options.offset.saturating_add(slice.len()) >= t.rows.len();
        let cursor_pk = slice.last().map(|last| {
            t.primary_key
                .iter()
                .map(|pk| last.get(pk).cloned().unwrap_or(Value::Null))
                .collect()
        });
        Ok(InitialLoadChunk {
            table: table.to_ascii_uppercase(),
            low_watermark: watermark,
            primary_key: t.primary_key.clone(),
            columns: t.columns.clone(),
            rows: slice,
            cursor_pk,
            exhausted,
        })
    }

    fn alignment_check_read(
        &self,
        _schema: &str,
        table: &str,
        max_rows: u32,
        _configured_timezone: Option<&str>,
    ) -> Result<AlignmentCheckSample, CaptureError> {
        if max_rows == 0 {
            return Err(CaptureError::ContractCatalog {
                detail: "Source Alignment Check max_rows must be >= 1".to_string(),
            });
        }
        let t = self.table(table)?;
        let take = (max_rows as usize).min(t.rows.len());
        Ok(AlignmentCheckSample {
            table: table.to_ascii_uppercase(),
            primary_key: t.primary_key.clone(),
            columns: t.columns.clone(),
            rows: t.rows.iter().take(take).cloned().collect(),
            truncated: t.rows.len() > take,
            source_row_count: Some(t.rows.len()),
        })
    }

    fn open_incremental_capture(&self) -> Result<Self::Incremental, CaptureError> {
        let mut changes_by_table = BTreeMap::new();
        for (name, table) in &self.tables {
            changes_by_table.insert(name.clone(), table.changes.clone());
        }
        Ok(FakeIncremental { changes_by_table })
    }

    fn schema_change_inputs(&self) -> Result<Vec<SchemaChangeEvent>, CaptureError> {
        Ok(self.schema_changes.clone())
    }

    fn kind_label(&self) -> &'static str {
        "fake"
    }
}

/// Shared Source→normalized-row slice used by engine-seam tests (issue #156).
///
/// Discovers schema, reads one Initial Load chunk, and opens Incremental Capture.
/// Written only against [`SourceEngine`] so contract and Fake swap without rewrite.
pub fn source_engine_sync_probe<S: SourceEngine>(
    source: &S,
    schema: &str,
    table: &str,
) -> Result<(Vec<SourceColumn>, InitialLoadChunk, &'static str), CaptureError> {
    source.check_prerequisites(&[table.to_string()])?;
    let columns = source.discover_schema(schema, table)?;
    let chunk = source.initial_load_chunk(
        schema,
        table,
        None,
        &InitialLoadChunkOptions {
            chunk_size: 100,
            offset: 0,
            established_watermark: None,
        },
    )?;
    let incremental = source.open_incremental_capture()?;
    let mechanism = incremental.mechanism_label();
    let _ = source.schema_change_inputs()?;
    let _ = source.alignment_check_read(schema, table, 10, None)?;
    Ok((columns, chunk, mechanism))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_fake_source() -> FakeSource {
        FakeSource::new().with_table(
            "CUSTOMERS",
            FakeSourceTable {
                columns: vec![SourceColumn {
                    name: "ID".into(),
                    data_type: "NUMBER".into(),
                    supported: true,
                    precision: Some(10),
                    scale: Some(0),
                    size: None,
                }],
                primary_key: vec!["ID".into()],
                rows: vec![{
                    let mut row = BTreeMap::new();
                    row.insert("ID".into(), json!(1));
                    row
                }],
                low_watermark: CapturePosition(1000),
                changes: vec![],
            },
        )
    }

    #[test]
    fn fake_source_covers_source_engine_surface() {
        let source = sample_fake_source();
        let (columns, chunk, mechanism) =
            source_engine_sync_probe(&source, "", "CUSTOMERS").unwrap();
        assert_eq!(columns.len(), 1);
        assert_eq!(chunk.rows.len(), 1);
        assert_eq!(mechanism, "fake");
        assert_eq!(source.kind_label(), "fake");
    }

    #[test]
    fn source_engine_discover_uses_data_type_as_domain_default() {
        use migraloop_types::ColumnShape;

        let source = sample_fake_source();
        let columns = source.discover_schema("", "CUSTOMERS").unwrap();
        let col = &columns[0];
        assert_eq!(col.data_type, "NUMBER");
        assert_eq!(
            ColumnShape::from(col.clone()),
            ColumnShape {
                name: "ID".into(),
                data_type: "NUMBER".into(),
                precision: Some(10),
                scale: Some(0),
            }
        );
    }

    #[test]
    fn oracle_logminer_source_labels_contract_harness() {
        let source = OracleLogMinerSource::new(
            OracleSourceConnect {
                host: "contract".into(),
                port: 1521,
                database: "X".into(),
                username: "u".into(),
                tls: Default::default(),
            },
            "secret",
        );
        assert_eq!(source.kind_label(), "oracle-logminer-contract");
        assert!(source.is_contract_harness());
    }
}
