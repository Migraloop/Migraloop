//! Contract/stub Source catalog for CI and local slices.
//!
//! Named default fixtures keep scenario tests readable. Product discovery and
//! Initial Load on harness hosts read this catalog as data — not a closed
//! business-table match arm. Tests may inject additional tables via
//! `MIGRALOOP_CONTRACT_SOURCE_CATALOG` (JSON file path).

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::sync::{Mutex, MutexGuard, RwLock};

use serde::{Deserialize, Serialize};

use crate::{
    accounts_fixture, customers_fixture, events_fixture, is_allow_listed_oracle_type,
    normalize_snapshot_temporals, orders_fixture, CaptureError, CapturePosition,
    InitialLoadSnapshot, SourceColumn,
};

/// Env var: path to a JSON file that merges/overrides harness catalog tables.
pub const CONTRACT_SOURCE_CATALOG_ENV: &str = "MIGRALOOP_CONTRACT_SOURCE_CATALOG";

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ContractSourceCatalogFile {
    #[serde(default)]
    pub tables: Vec<InitialLoadSnapshot>,
}

/// In-process Source catalog used by contract/stub hosts for schema discovery
/// and Initial Load.
#[derive(Debug, Clone, PartialEq)]
pub struct ContractSourceCatalog {
    tables: BTreeMap<String, InitialLoadSnapshot>,
}

impl ContractSourceCatalog {
    pub fn empty() -> Self {
        Self {
            tables: BTreeMap::new(),
        }
    }

    /// Named scenario fixtures (CUSTOMERS / ORDERS / EVENTS / ACCOUNTS).
    pub fn with_default_fixtures() -> Self {
        let mut catalog = Self::empty();
        for snapshot in [
            customers_fixture(),
            orders_fixture(),
            events_fixture(),
            accounts_fixture(),
        ] {
            catalog.insert(snapshot);
        }
        catalog
    }

    pub fn insert(&mut self, snapshot: InitialLoadSnapshot) {
        let key = snapshot.table.trim().to_ascii_uppercase();
        let mut snapshot = snapshot;
        snapshot.table = key.clone();
        // Apply the same allow-list rules as live OCI discovery (ADR-0018).
        for column in &mut snapshot.columns {
            column.supported =
                column.supported && is_allow_listed_oracle_type(&column.oracle_type, column.size);
        }
        self.tables.insert(key, snapshot);
    }

    pub fn merge_file(&mut self, file: ContractSourceCatalogFile) {
        for snapshot in file.tables {
            self.insert(snapshot);
        }
    }

    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    pub fn schema(&self, table: &str) -> Result<Vec<SourceColumn>, CaptureError> {
        Ok(self.snapshot(table)?.columns.clone())
    }

    pub fn initial_load(
        &self,
        table: &str,
        configured_timezone: Option<&str>,
    ) -> Result<InitialLoadSnapshot, CaptureError> {
        let mut snapshot = self.snapshot(table)?.clone();
        normalize_snapshot_temporals(&mut snapshot, configured_timezone)?;
        Ok(snapshot)
    }

    fn snapshot(&self, table: &str) -> Result<&InitialLoadSnapshot, CaptureError> {
        let key = table.trim().to_ascii_uppercase();
        self.tables
            .get(&key)
            .ok_or_else(|| CaptureError::UnknownTable(key))
    }
}

/// Load the process catalog: optional unit-test override, else defaults + env JSON merge.
pub fn load_contract_source_catalog() -> Result<ContractSourceCatalog, CaptureError> {
    if let Some(override_catalog) = process_catalog_override() {
        return Ok(override_catalog);
    }

    let mut catalog = ContractSourceCatalog::with_default_fixtures();
    if let Some(path) = std::env::var_os(CONTRACT_SOURCE_CATALOG_ENV) {
        let path = Path::new(&path);
        let file = load_catalog_file(path)?;
        catalog.merge_file(file);
    }
    Ok(catalog)
}

pub fn load_catalog_file(path: &Path) -> Result<ContractSourceCatalogFile, CaptureError> {
    let raw = fs::read_to_string(path).map_err(|err| CaptureError::ContractCatalog {
        detail: format!(
            "failed to read {CONTRACT_SOURCE_CATALOG_ENV} file {}: {err}",
            path.display()
        ),
    })?;
    serde_json::from_str(&raw).map_err(|err| CaptureError::ContractCatalog {
        detail: format!(
            "invalid {CONTRACT_SOURCE_CATALOG_ENV} JSON {}: {err}",
            path.display()
        ),
    })
}

static PROCESS_CATALOG_OVERRIDE: RwLock<Option<ContractSourceCatalog>> = RwLock::new(None);

/// Serializes unit tests that mutate the process catalog override or
/// `MIGRALOOP_CONTRACT_SOURCE_CATALOG` (parallel cargo test races otherwise).
static CONTRACT_CATALOG_TEST_LOCK: Mutex<()> = Mutex::new(());

fn process_catalog_override() -> Option<ContractSourceCatalog> {
    PROCESS_CATALOG_OVERRIDE
        .read()
        .ok()
        .and_then(|guard| guard.clone())
}

/// Hold across any test that sets/clears the process catalog override or env catalog path.
pub fn lock_contract_catalog_for_test() -> MutexGuard<'static, ()> {
    CONTRACT_CATALOG_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Install a process-wide catalog override (unit tests). Clear with [`clear_contract_source_catalog_override`].
pub fn set_contract_source_catalog_override(catalog: ContractSourceCatalog) {
    if let Ok(mut guard) = PROCESS_CATALOG_OVERRIDE.write() {
        *guard = Some(catalog);
    }
}

pub fn clear_contract_source_catalog_override() {
    if let Ok(mut guard) = PROCESS_CATALOG_OVERRIDE.write() {
        *guard = None;
    }
}

/// Helper for building snapshots in tests / JSON authors.
pub fn snapshot(
    table: &str,
    low_watermark: u64,
    primary_key: &[&str],
    columns: Vec<SourceColumn>,
    rows: Vec<BTreeMap<String, serde_json::Value>>,
) -> InitialLoadSnapshot {
    InitialLoadSnapshot {
        table: table.to_ascii_uppercase(),
        low_watermark: CapturePosition(low_watermark),
        primary_key: primary_key.iter().map(|s| (*s).to_string()).collect(),
        columns,
        rows,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{col, number_col};
    use serde_json::json;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn widgets_snapshot() -> InitialLoadSnapshot {
        let mut row = BTreeMap::new();
        row.insert("WID".into(), json!(1));
        row.insert("LABEL".into(), json!("alpha"));
        row.insert("PHOTO".into(), json!("blob-bytes"));
        snapshot(
            "WIDGETS",
            9000,
            &["WID"],
            vec![
                number_col("WID", 10, 0, true),
                col("LABEL", "VARCHAR2", true),
                col("PHOTO", "BLOB", false),
            ],
            vec![row],
        )
    }

    #[test]
    fn catalog_discovers_and_loads_table_outside_hard_coded_fixture_names() {
        let mut catalog = ContractSourceCatalog::empty();
        catalog.insert(widgets_snapshot());

        let columns = catalog.schema("widgets").expect("schema");
        assert_eq!(columns.len(), 3);
        assert!(columns.iter().any(|c| c.name == "LABEL" && c.supported));
        assert!(columns.iter().any(|c| c.name == "PHOTO" && !c.supported));

        let loaded = catalog.initial_load("WIDGETS", None).expect("initial load");
        assert_eq!(loaded.table, "WIDGETS");
        assert_eq!(loaded.low_watermark, CapturePosition(9000));
        assert_eq!(loaded.primary_key, vec!["WID".to_string()]);
        assert_eq!(loaded.rows.len(), 1);
        assert_eq!(loaded.rows[0].get("LABEL"), Some(&json!("alpha")));
        assert_eq!(loaded.omitted_columns().len(), 1);
        assert_eq!(loaded.omitted_columns()[0].name, "PHOTO");
    }

    #[test]
    fn default_fixtures_remain_available_for_named_scenario_tests() {
        let catalog = ContractSourceCatalog::with_default_fixtures();
        assert!(catalog.schema("CUSTOMERS").is_ok());
        assert!(catalog.schema("ORDERS").is_ok());
        assert!(catalog.schema("EVENTS").is_ok());
        assert!(catalog.schema("ACCOUNTS").is_ok());
        assert!(catalog.schema("WIDGETS").is_err());
    }

    #[test]
    fn env_catalog_file_merges_arbitrary_table_into_process_catalog() {
        let _lock = lock_contract_catalog_for_test();
        clear_contract_source_catalog_override();
        let file = ContractSourceCatalogFile {
            tables: vec![widgets_snapshot()],
        };
        let path = std::env::temp_dir().join(format!(
            "migraloop_contract_catalog_{}.json",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::write(&path, serde_json::to_string_pretty(&file).unwrap()).expect("write catalog");

        std::env::set_var(CONTRACT_SOURCE_CATALOG_ENV, &path);
        let catalog = load_contract_source_catalog().expect("load");
        std::env::remove_var(CONTRACT_SOURCE_CATALOG_ENV);
        let _ = fs::remove_file(&path);

        assert!(catalog.schema("CUSTOMERS").is_ok(), "defaults retained");
        let widgets = catalog.initial_load("WIDGETS", None).expect("widgets");
        assert_eq!(widgets.rows[0].get("LABEL"), Some(&json!("alpha")));
    }

    #[test]
    fn insert_reapplies_allow_list_so_mis_marked_blob_is_unsupported() {
        let mut catalog = ContractSourceCatalog::empty();
        let mut row = BTreeMap::new();
        row.insert("WID".into(), json!(1));
        row.insert("PHOTO".into(), json!("blob"));
        catalog.insert(snapshot(
            "WIDGETS",
            1,
            &["WID"],
            vec![
                number_col("WID", 10, 0, true),
                // Author wrongly claims BLOB is supported — catalog must correct.
                SourceColumn {
                    name: "PHOTO".into(),
                    oracle_type: "BLOB".into(),
                    supported: true,
                    precision: None,
                    scale: None,
                    size: None,
                },
            ],
            vec![row],
        ));
        let columns = catalog.schema("WIDGETS").expect("schema");
        let photo = columns.iter().find(|c| c.name == "PHOTO").expect("PHOTO");
        assert!(!photo.supported, "BLOB must remain unsupported after insert");
    }
}
