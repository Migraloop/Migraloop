//! Shared helpers for `migraloop-app` integration tests.
//!
//! Each `tests/*.rs` binary that needs these does `mod common;`.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use migraloop_capture::{
    named_scenario_logminer_contents, ContractSourceCatalog, CONTRACT_SOURCE_CATALOG_ENV,
    INJECT_LOGMINER_CONTENTS_ENV, LogMinerContent,
};
use serde_json::json;

static UNIQUE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Unique suffix for ephemeral Postgres / Mongo names under parallel `cargo test`.
///
/// Wall-clock nanos alone collide when multiple tests (or binaries) call
/// `SystemTime::now()` in the same tick — CI saw
/// `duplicate key value violates unique constraint "pg_database_datname_index"`.
/// Pid separates cargo test binaries; the atomic seq separates threads inside one binary.
pub fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let seq = UNIQUE_SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{nanos}_{}_{seq}", std::process::id())
}

/// Injectable named-scenario Source doubles (CUSTOMERS / ORDERS / EVENTS / ACCOUNTS).
///
/// Product runtime no longer ships these catalogs (issue #120). Contract/stub
/// CLI tests must install these files and pass the env vars into apply/sync
/// (and Source-touching align/drift) subprocesses.
///
/// Allow unused: each `tests/*.rs` binary compiles this module; only some use it.
#[allow(dead_code)]
pub struct NamedScenarioDoubles {
    pub catalog_path: PathBuf,
    pub logminer_path: PathBuf,
}

#[allow(dead_code)]
impl NamedScenarioDoubles {
    /// Write named-scenario catalog + LogMiner Incremental doubles under `dir`.
    pub fn install(dir: &Path) -> Self {
        Self::install_with_extra_logminer(dir, &[])
    }

    /// Same as [`install`], appending extra Incremental contents (e.g. backpressure backlog).
    pub fn install_with_extra_logminer(dir: &Path, extra: &[LogMinerContent]) -> Self {
        let catalog_path = dir.join(format!(
            "named_scenario_catalog_{}.json",
            unique_suffix()
        ));
        let logminer_path = dir.join(format!(
            "named_scenario_logminer_{}.json",
            unique_suffix()
        ));

        let catalog = ContractSourceCatalog::with_default_fixtures();
        let catalog_json = serde_json::to_string_pretty(&catalog.to_file())
            .expect("serialize named-scenario catalog");
        fs::write(&catalog_path, catalog_json).expect("write named-scenario catalog");

        let mut contents = named_scenario_logminer_contents();
        contents.extend(extra.iter().cloned());
        let inject = json!({ "contents": contents });
        fs::write(
            &logminer_path,
            serde_json::to_string_pretty(&inject).expect("serialize logminer inject"),
        )
        .expect("write named-scenario logminer inject");

        Self {
            catalog_path,
            logminer_path,
        }
    }

    /// Attach catalog + LogMiner inject env vars for contract/stub Source commands.
    pub fn apply_env<'a>(&self, cmd: &'a mut Command) -> &'a mut Command {
        cmd.env(CONTRACT_SOURCE_CATALOG_ENV, &self.catalog_path)
            .env(INJECT_LOGMINER_CONTENTS_ENV, &self.logminer_path)
    }
}
