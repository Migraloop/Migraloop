//! Deployment runtime public interface: Operator Deployment verbs plus necessary
//! session / factory entry points (issue #172).
//!
//! **Verbs:** [`apply`], Incremental Sync ([`sync_incremental`], [`run_incremental_sync`],
//! [`run_incremental_sync_with_engines`], [`run_continuous_incremental_sync`],
//! [`supervise_continuous_incremental_sync`] with typed [`SyncOptions`]), Pipeline
//! lifecycle ([`pause_pipeline`], [`resume_pipeline`], [`remove_pipeline`]),
//! [`source_alignment_check`], [`drift_check`], status inventory ([`status_inventory`],
//! [`status_inventory_from_url`]), Observability Surface assembly
//! ([`assemble_observability_surface`]), cutover facts / hand-off
//! ([`cutover_facts_from_base`], [`handoff_from_low_watermark`],
//! [`resume_for_incremental`]), and inspect
//! ([`inspect_base_rows`], [`inspect_derived_rows`], [`inspect_target_documents`]).
//!
//! **Session / factory entry points:** [`source_engine_from_connection`],
//! [`target_engine_from_deployment`], plus structured observability emit helpers
//! used by the Operator edge.
//!
//! Internal helpers (Base apply crumbs, Delivery orchestration pieces, identity display)
//! stay `pub(crate)` — they fail the deletion test as a public surface. The Operator CLI
//! is a thin adapter (clap/config/env + narrative formatting) over this interface.

use std::collections::{BTreeMap, BTreeSet};

use migraloop_capture::{
    classify_number, is_allow_listed_oracle_type, CapturePosition, InitialLoadChunkOptions,
    NumberMongoMapping, OracleLogMinerSource, OracleSourceConnect, SourceColumn, SourceEngine,
    TypeError,
};
use migraloop_delivery::{
    DeliveryColumn, DeliveryDocument, ManagedFieldAs, MongoTargetConnection, TargetEngine,
};
use migraloop_platform_store::{
    check_store_settings, disk_warn_message, BaseColumn, BaseDataset, Deployment, DerivedDataset,
    OmittedColumn, Pipeline, PlatformStore, PlatformStoreHealth, SecretRef, SystemConnection,
};
use migraloop_transform::{
    evaluate_transform_with_bases, infer_derived_columns, initial_maintenance_state,
    parse_transform_steps, secondary_base_refs, MaintenanceStateBlob, OutputColumn, TransformOp,
};
use migraloop_types::{output_identity_key, resolve_secret_ref, ColumnShape};
use thiserror::Error;

#[cfg(test)]
mod engines;
mod backpressure;
mod cutover;
mod observability;
mod incremental;
mod lifecycle;
mod poison;
mod schema_impact;
mod sync_options;

pub use cutover::{
    cutover_facts_from_base, handoff_from_low_watermark, handoff_from_optional_low_watermark,
    resume_for_incremental, CutoverFacts, CutoverHandoff, IncrementalResume,
};
pub use observability::{
    assemble_observability_surface, emit_event, render_prometheus_metrics, BaseSyncObservation,
    DeliveryHealth, EventValue, ObservabilitySurface, PipelineDeliveryObservation, SyncHealth,
};
pub(crate) use observability::sync_health_label_for_progress;

pub use incremental::{
    run_incremental_sync, run_incremental_sync_with_engines, run_continuous_incremental_sync,
    supervise_continuous_incremental_sync, sync_incremental, sync_incremental_with_options,
    SyncCycleOutcome, SyncInvocation,
};
pub use sync_options::{
    BackpressureOptions, PoisonOptions, SyncOptions, SyncOptionsOverrides,
};
pub use lifecycle::{
    drift_check, inspect_base_rows, inspect_derived_rows, inspect_target_documents, pause_pipeline,
    remove_pipeline, resume_pipeline, source_alignment_check, status_inventory,
    status_inventory_from_url, StatusInventory, DEFAULT_ALIGNMENT_MAX_ROWS, DEFAULT_DRIFT_MAX_ROWS,
};

/// Dependency-graph seam marker (ADR-0024 modular monorepo).
pub const SEAM: &str = "runtime";

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error("{0}")]
    Failed(String),
}

pub(crate) fn resolve_secret_value(reference: &SecretRef, field: &str) -> Result<String, RuntimeError> {
    resolve_secret_ref(reference, field).map_err(|err| RuntimeError::Failed(err.to_string()))
}

pub(crate) fn output_identity_from_row(
    row: &serde_json::Map<String, serde_json::Value>,
    identity_fields: &[String],
) -> Result<serde_json::Value, RuntimeError> {
    if identity_fields.is_empty() {
        return Err(RuntimeError::Failed(
            "Output Identity fields are empty".to_string(),
        ));
    }
    if identity_fields.len() == 1 {
        let key = &identity_fields[0];
        return row.get(key).cloned().ok_or_else(|| {
            RuntimeError::Failed(format!("row missing Output Identity column {key}"))
        });
    }
    let mut identity = serde_json::Map::new();
    for key in identity_fields {
        let value = row.get(key).cloned().ok_or_else(|| {
            RuntimeError::Failed(format!("row missing Output Identity column {key}"))
        })?;
        identity.insert(key.clone(), value);
    }
    Ok(serde_json::Value::Object(identity))
}

pub(crate) fn transform_ops_from_pipeline(pipeline: &Pipeline) -> Result<Vec<TransformOp>, RuntimeError> {
    let Some(value) = &pipeline.transform_json else {
        return Err(RuntimeError::Failed(format!(
            "Transform Pipeline {} is missing transform definition",
            pipeline.name
        )));
    };
    let steps = value.as_array().ok_or_else(|| {
        RuntimeError::Failed(format!(
            "Transform Pipeline {} transform must be an array of operators",
            pipeline.name
        ))
    })?;
    parse_transform_steps(steps)
        .map_err(|err| RuntimeError::Failed(format!("Transform Pipeline {}: {err}", pipeline.name)))
}

/// Base Dataset (schema, table) pairs a Pipeline references — primary `source.table`
/// plus every `equiLookup.from` / `union.from` secondary Base.
pub(crate) fn pipeline_base_table_refs(pipeline: &Pipeline) -> Vec<(String, String)> {
    let mut tables = BTreeSet::new();
    if !pipeline.source_table.is_empty() {
        tables.insert((
            pipeline.source_schema.clone(),
            pipeline.source_table.clone(),
        ));
    }
    if pipeline.mode == "transform" {
        if let Ok(ops) = transform_ops_from_pipeline(pipeline) {
            for sec in secondary_base_refs(&ops) {
                let schema = sec.schema.unwrap_or_else(|| pipeline.source_schema.clone());
                tables.insert((schema, sec.table));
            }
        }
    }
    tables.into_iter().collect()
}

pub(crate) fn pipeline_references_table(pipeline: &Pipeline, table: &str) -> bool {
    pipeline_base_table_refs(pipeline)
        .iter()
        .any(|(_, t)| t.eq_ignore_ascii_case(table))
}

/// Load secondary Base rows for all `equiLookup.from` / `union.from` tables on this Pipeline.
/// Secondary Base rows plus column metadata (for unwind-flattened foreign fields).
pub(crate) async fn load_secondary_bases_and_columns_for_pipeline(
    store: &PlatformStore,
    pipeline: &Pipeline,
    ops: &[TransformOp],
) -> Result<
    (
        BTreeMap<String, Vec<serde_json::Map<String, serde_json::Value>>>,
        Vec<BaseColumn>,
    ),
    RuntimeError,
> {
    let mut secondary = BTreeMap::new();
    let mut columns = Vec::new();
    let mut seen_cols = BTreeSet::new();
    for sec in secondary_base_refs(ops) {
        let (base, rows) = store.get_base_rows(
            &sec.table,
            Some(&pipeline.deployment_name),
        )
        .await
        .map_err(|err| {
            RuntimeError::Failed(format!(
                "Transform Pipeline {}: secondary Base Dataset `{}` (equiLookup/union.from) unavailable: {err}",
                pipeline.name, sec.table
            ))
        })?;
        for col in base.columns {
            if seen_cols.insert(col.name.clone()) {
                columns.push(col);
            }
        }
        secondary.insert(sec.table, rows.into_iter().map(|r| r.data).collect());
    }
    Ok((secondary, columns))
}

/// Open the v1 Target engine adapter for a Deployment (MongoDB document Delivery).
///
/// Returns the [`TargetEngine`] interface so production wiring and Sync/Delivery
/// call sites do not name the concrete Mongo adapter type.
pub fn target_engine_from_deployment(
    deployment: &Deployment,
) -> Result<impl TargetEngine, RuntimeError> {
    if !deployment.target.kind.eq_ignore_ascii_case("mongodb") {
        return Err(RuntimeError::Failed(format!(
            "unsupported Target System kind {:?} (v1 ships MongoDB only)",
            deployment.target.kind
        )));
    }
    if deployment.target.port <= 0 || deployment.target.port > u16::MAX as i32 {
        return Err(RuntimeError::Failed(
            "target.port must be a valid TCP port".to_string(),
        ));
    }
    let password = resolve_secret_value(&deployment.target.password_ref, "target.password")?;
    Ok(MongoTargetConnection {
        host: deployment.target.host.clone(),
        port: deployment.target.port as u16,
        database: deployment.target.database.clone(),
        username: deployment.target.username.clone(),
        password,
        tls: deployment.target.tls.clone(),
    })
}

/// Open the v1 Source engine adapter for a Deployment Source System connection.
///
/// Returns the [`SourceEngine`] interface so production wiring and Sync call sites
/// do not name the concrete Oracle LogMiner adapter type. Contract harness vs OCI
/// selection stays inside the adapter (ADR-0003).
pub fn source_engine_from_connection(
    source: &SystemConnection,
) -> Result<impl SourceEngine, RuntimeError> {
    if !source.kind.eq_ignore_ascii_case("oracle") {
        return Err(RuntimeError::Failed(format!(
            "unsupported Source System kind {:?} (v1 ships Oracle LogMiner only)",
            source.kind
        )));
    }
    let connect = oracle_source_connect(source)?;
    let password = resolve_secret_value(&source.password_ref, "source.password")?;
    Ok(OracleLogMinerSource::new(connect, password))
}

pub(crate) fn source_timezone_opt(deployment: &Deployment) -> Option<&str> {
    let tz = deployment.source.timezone.trim();
    if tz.is_empty() {
        None
    } else {
        Some(tz)
    }
}

fn base_columns_from_source(columns: &[&SourceColumn]) -> Vec<BaseColumn> {
    columns
        .iter()
        .map(|c| BaseColumn::from(c.column_shape()))
        .collect()
}

fn delivery_columns_from_base(columns: &[BaseColumn]) -> Vec<DeliveryColumn> {
    columns
        .iter()
        .cloned()
        .map(ColumnShape::from)
        .map(DeliveryColumn::from)
        .collect()
}

pub(crate) fn apply_field_mappings_to_row(
    row: &serde_json::Map<String, serde_json::Value>,
    pipeline: &Pipeline,
) -> serde_json::Map<String, serde_json::Value> {
    let mut out = serde_json::Map::new();
    for (key, value) in row {
        match pipeline.field_mappings.get(key) {
            Some(ManagedFieldAs::Omit) => continue,
            Some(ManagedFieldAs::String) => {
                let as_string = match value {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    other => other.to_string(),
                };
                out.insert(key.clone(), serde_json::Value::String(as_string));
            }
            None | Some(ManagedFieldAs::Default) => {
                out.insert(key.clone(), value.clone());
            }
        }
    }
    out
}

/// Apply-time validation for Managed/transform inputs (ADR-0018 / ADR-0023).
pub(crate) fn validate_pipeline_managed_fields(
    pipeline: &Pipeline,
    source_columns: &[SourceColumn],
    managed_column_names: &BTreeSet<String>,
) -> Result<(), RuntimeError> {
    let by_name: std::collections::BTreeMap<&str, &SourceColumn> = source_columns
        .iter()
        .map(|c| (c.name.as_str(), c))
        .collect();

    for (field, mapping) in &pipeline.field_mappings {
        match by_name.get(field.as_str()) {
            None => {
                return Err(RuntimeError::Failed(format!(
                    "Pipeline {} fields.{} references unknown Source column",
                    pipeline.name, field
                )));
            }
            Some(col)
                if !col.supported || !is_allow_listed_oracle_type(&col.oracle_type, col.size) =>
            {
                if *mapping != ManagedFieldAs::Omit {
                    return Err(RuntimeError::Failed(format!(
                        "Pipeline {}: {} (column {field})",
                        pipeline.name,
                        TypeError::UnsupportedAsManaged {
                            oracle_type: col.oracle_type.clone(),
                        }
                    )));
                }
            }
            Some(_) => {}
        }
    }

    for name in managed_column_names {
        let Some(col) = by_name.get(name.as_str()) else {
            continue;
        };
        if !col.is_number() {
            continue;
        }
        if classify_number(col.precision, col.scale) != NumberMongoMapping::Unsafe {
            continue;
        }
        match pipeline.field_mappings.get(name) {
            Some(ManagedFieldAs::String) | Some(ManagedFieldAs::Omit) => {}
            None | Some(ManagedFieldAs::Default) => {
                return Err(RuntimeError::Failed(format!(
                    "NUMBER column {name} has unsafe declared precision/scale \
                     (precision={:?}, scale={:?}); Pipeline {} cannot apply until \
                     fields.{name}.as is string or omit (ADR-0023); never default IEEE double",
                    col.precision, col.scale, pipeline.name
                )));
            }
        }
    }
    Ok(())
}

pub(crate) fn delivery_document_for_row(
    row: &serde_json::Map<String, serde_json::Value>,
    identity_fields: &[String],
    columns: &[BaseColumn],
    pipeline: &Pipeline,
) -> Result<DeliveryDocument, RuntimeError> {
    let managed = apply_field_mappings_to_row(row, pipeline);
    let identity = output_identity_from_row(&managed, identity_fields).or_else(|_| {
        // Identity may be omitted from Managed via field mapping; fall back to full row.
        output_identity_from_row(row, identity_fields)
    })?;
    Ok(DeliveryDocument {
        identity,
        managed_fields: managed,
        columns: delivery_columns_from_base(columns),
        field_as: pipeline.field_mappings.clone(),
    })
}

pub(crate) async fn ensure_store_session_healthy(store: &PlatformStore) -> Result<(), RuntimeError> {
    match store.health().await {
        PlatformStoreHealth::Healthy { .. } => {
            // Settings guardrails reject absurd under-provisioning; disk warn is
            // intentionally not a hard failure here (ADR-0010 warn-only).
            let settings = store
                .probe_settings()
                .await
                .map_err(|err| RuntimeError::Failed(err.to_string()))?;
            check_store_settings(&settings).map_err(|err| RuntimeError::Failed(err.to_string()))?;
            let resources = store
                .probe_resources()
                .await
                .map_err(|err| RuntimeError::Failed(err.to_string()))?;
            if let (true, Some(free)) = (resources.disk_warn, resources.free_disk_bytes) {
                let msg = disk_warn_message(free);
                println!("{msg}");
                emit_event(
                    "platform_store_disk_warn",
                    &[
                        ("free_disk_bytes", EventValue::from(free as i64)),
                        (
                            "warn_threshold_bytes",
                            EventValue::from(migraloop_platform_store::DISK_FREE_WARN_BYTES as i64),
                        ),
                        ("auto_pause", EventValue::from(false)),
                    ],
                );
            }
            Ok(())
        }
        PlatformStoreHealth::Unhealthy { reason } => Err(RuntimeError::Failed(format!(
            "Platform Store is not healthy; run `migraloop migrate` first: {reason}"
        ))),
        PlatformStoreHealth::Unreachable { reason } => Err(RuntimeError::Failed(format!(
            "Platform Store is unreachable: {reason}"
        ))),
    }
}

async fn sync_base_datasets_for_pipelines(
    store: &PlatformStore,
    deployment: &Deployment,
    pipelines: &[Pipeline],
) -> Result<(), RuntimeError> {
    let deployment_name = &deployment.name;
    let configured_tz = source_timezone_opt(deployment);
    let mut tables = BTreeSet::new();
    for pipeline in pipelines {
        for (schema, table) in pipeline_base_table_refs(pipeline) {
            tables.insert((schema, table));
        }
    }
    let keep: Vec<(String, String)> = tables.iter().cloned().collect();

    // Capture scope follows Pipeline references: drop Bases for tables no longer referenced.
    store
        .delete_base_datasets_not_in(deployment_name, &keep)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    for (schema, table) in tables {
        let existing = if store
            .base_dataset_exists(deployment_name, &schema, &table)
            .await
            .map_err(|err| RuntimeError::Failed(err.to_string()))?
        {
            let (dataset, _) = store
                .get_base_rows(&table, Some(deployment_name))
                .await
                .map_err(|err| RuntimeError::Failed(err.to_string()))?;
            Some(dataset)
        } else {
            None
        };

        if let Some(ref dataset) = existing {
            let resumable = dataset.status == "initial_load_in_progress"
                || dataset.status == "initial_load_paused";
            if !resumable {
                // Existing Bases stay; do not reload on Pipeline re-apply (ADR-0019).
                ensure_base_primary_key(store, deployment, &schema, &table, configured_tz).await?;
                continue;
            }
        }

        run_chunked_initial_load(
            store,
            deployment,
            pipelines,
            &schema,
            &table,
            configured_tz,
            existing.as_ref(),
        )
        .await?;
    }

    Ok(())
}

/// Default Initial Load Source read window (issue #124). Override via
/// `MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE` (must be > 0).
fn initial_load_chunk_size() -> usize {
    std::env::var("MIGRALOOP_INITIAL_LOAD_CHUNK_SIZE")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &usize| *n > 0)
        .unwrap_or(1000)
}

/// Optional Operator throttle for Initial Load (rows/sec). `0` / unset = no artificial cap.
fn initial_load_rows_per_sec() -> Option<u64> {
    std::env::var("MIGRALOOP_INITIAL_LOAD_ROWS_PER_SEC")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &u64| *n > 0)
}

/// Test/Lab inject: pause Initial Load after N successful chunks.
fn initial_load_pause_after_chunks() -> Option<u64> {
    std::env::var("MIGRALOOP_INITIAL_LOAD_PAUSE_AFTER_CHUNKS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &u64| *n > 0)
}

/// Test/Lab inject: artificial Platform Store / Downstream pressure during Initial Load.
fn initial_load_store_delay_ms() -> Option<u64> {
    std::env::var("MIGRALOOP_INITIAL_LOAD_STORE_DELAY_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &u64| *n > 0)
}

async fn run_chunked_initial_load(
    store: &PlatformStore,
    deployment: &Deployment,
    pipelines: &[Pipeline],
    schema: &str,
    table: &str,
    configured_tz: Option<&str>,
    existing: Option<&BaseDataset>,
) -> Result<(), RuntimeError> {
    let deployment_name = &deployment.name;
    let source = source_engine_from_connection(&deployment.source)?;
    let chunk_size = initial_load_chunk_size();
    let rate_limit = initial_load_rows_per_sec();
    let pause_after = initial_load_pause_after_chunks();
    let store_delay = initial_load_store_delay_ms();

    let mut offset = existing.map(|d| d.row_count.max(0) as usize).unwrap_or(0);
    let mut established = existing
        .and_then(|d| d.capture_low_watermark)
        .and_then(CapturePosition::from_i64);
    let mut chunks_done: u64 = 0;
    let mut primary_key = existing.map(|d| d.primary_key.clone()).unwrap_or_default();
    let mut columns = existing.map(|d| d.columns.clone()).unwrap_or_default();
    let mut omitted_columns = existing
        .map(|d| d.omitted_columns.clone())
        .unwrap_or_default();
    let mut supported_names: BTreeSet<String> = columns.iter().map(|c| c.name.clone()).collect();
    let mut low_watermark = established;

    loop {
        // Honor durable Pipeline pause between chunks (Operator `migraloop pause`).
        if initial_load_should_pause(store, deployment_name, table, pipelines).await? {
            persist_initial_load_pause(
                store,
                deployment_name,
                schema,
                table,
                &primary_key,
                &columns,
                &omitted_columns,
                offset,
                low_watermark,
                existing.and_then(|d| d.initial_load_cursor.clone()),
            )
            .await?;
            return Ok(());
        }

        let source_started = std::time::Instant::now();
        let chunk = source
            .initial_load_chunk(
                schema,
                table,
                configured_tz,
                &InitialLoadChunkOptions {
                    chunk_size,
                    offset,
                    established_watermark: established,
                },
            )
            .map_err(|err| RuntimeError::Failed(err.to_string()))?;
        let source_ms = source_started.elapsed().as_millis() as u64;

        if primary_key.is_empty() {
            primary_key = chunk.primary_key.clone();
        }
        if columns.is_empty() {
            let supported = chunk
                .columns
                .iter()
                .filter(|c| c.supported)
                .collect::<Vec<_>>();
            columns = base_columns_from_source(&supported);
            omitted_columns = chunk
                .columns
                .iter()
                .filter(|c| !c.supported)
                .map(|c| {
                    let shape = c.column_shape();
                    OmittedColumn {
                        name: shape.name,
                        oracle_type: shape.data_type,
                    }
                })
                .collect();
            supported_names = columns.iter().map(|c| c.name.clone()).collect();
        }

        low_watermark = Some(chunk.low_watermark);
        established = Some(chunk.low_watermark);

        let rows: Vec<serde_json::Map<String, serde_json::Value>> = chunk
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .filter(|(name, _)| supported_names.contains(name))
                    .collect()
            })
            .collect();

        let start_ordinal = offset as i32;
        offset = offset.saturating_add(rows.len());
        chunks_done = chunks_done.saturating_add(1);

        let status = if chunk.exhausted {
            "initial_load_complete"
        } else {
            "initial_load_in_progress"
        };
        let cursor = if chunk.exhausted {
            None
        } else {
            chunk.cursor_pk.clone()
        };
        let wm = chunk.low_watermark;
        let handoff = handoff_from_low_watermark(wm);
        let dataset = BaseDataset {
            deployment_name: deployment_name.to_string(),
            source_table: table.to_string(),
            source_schema: schema.to_string(),
            status: status.to_string(),
            primary_key: primary_key.clone(),
            columns: columns.clone(),
            omitted_columns: omitted_columns.clone(),
            row_count: offset as i32,
            sync_applied_changes: 0,
            sync_health: "unknown".to_string(),
            capture_low_watermark: Some(handoff.low_watermark),
            capture_checkpoint: Some(handoff.checkpoint),
            sync_lag: 0,
            source_alignment: "unknown".to_string(),
            source_alignment_checked_rows: 0,
            source_alignment_mismatched_rows: 0,
            initial_load_cursor: cursor.clone(),
        };

        let persist_started = std::time::Instant::now();
        if let Some(ms) = store_delay {
            tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
        }
        store
            .append_base_dataset_chunk(&dataset, &rows, start_ordinal)
            .await
            .map_err(|err| RuntimeError::Failed(err.to_string()))?;
        let persist_ms = persist_started.elapsed().as_millis() as u64;

        let rate_note = rate_limit
            .map(|r| format!(" rate_limit={r}/s"))
            .unwrap_or_default();
        println!(
            "Initial Load progress: {table} chunk={chunks_done} rows={offset} \
             chunk_size={chunk_size}{rate_note} low-watermark={wm}"
        );
        let mut progress_fields = vec![
            ("table", EventValue::from(table)),
            ("chunk", EventValue::from(chunks_done as i64)),
            ("rows", EventValue::from(offset as i64)),
            ("chunk_size", EventValue::from(chunk_size)),
            ("low_watermark", EventValue::from(wm.as_i64())),
            ("deployment", EventValue::from(deployment_name.as_str())),
        ];
        if let Some(rate) = rate_limit {
            progress_fields.push(("rate_limit_rows_per_sec", EventValue::from(rate as i64)));
        }
        emit_event("initial_load_progress", &progress_fields);

        // Back off when Downstream/store or Source pressure is visible (issue #124).
        let pressure_ms = store_delay.unwrap_or(0).max(persist_ms).max(source_ms);
        if store_delay.is_some() || persist_ms >= 25 || source_ms >= 25 {
            let backoff_ms = pressure_ms.max(10);
            let pressure = if store_delay.is_some() || persist_ms >= source_ms {
                "Downstream/store"
            } else {
                "Source"
            };
            println!(
                "Initial Load backoff: {table} delay_ms={backoff_ms} \
                 ({pressure} pressure; chunk window stays bounded)"
            );
            emit_event(
                "initial_load_backoff",
                &[
                    ("table", EventValue::from(table)),
                    ("delay_ms", EventValue::from(backoff_ms as i64)),
                    ("chunk_size", EventValue::from(chunk_size)),
                    ("pressure", EventValue::from(pressure)),
                    ("deployment", EventValue::from(deployment_name.as_str())),
                ],
            );
            if store_delay.is_none() {
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms.min(250))).await;
            }
        }

        if let Some(rate) = rate_limit {
            if !rows.is_empty() {
                let sleep_ms = (rows.len() as u128)
                    .saturating_mul(1000)
                    .saturating_div(rate as u128) as u64;
                if sleep_ms > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(sleep_ms)).await;
                }
            }
        }

        if chunk.exhausted {
            println!(
                "Initial Load complete: Base Dataset {table} ({} rows) low-watermark={wm}",
                offset
            );
            emit_event(
                "initial_load_complete",
                &[
                    ("table", EventValue::from(table)),
                    ("rows", EventValue::from(offset as i64)),
                    ("low_watermark", EventValue::from(wm.as_i64())),
                    ("deployment", EventValue::from(deployment_name.as_str())),
                    ("chunk_size", EventValue::from(chunk_size)),
                ],
            );
            return Ok(());
        }

        if pause_after.is_some_and(|n| chunks_done >= n) {
            persist_initial_load_pause(
                store,
                deployment_name,
                schema,
                table,
                &primary_key,
                &columns,
                &omitted_columns,
                offset,
                Some(wm),
                cursor,
            )
            .await?;
            return Ok(());
        }
    }
}

async fn initial_load_should_pause(
    store: &PlatformStore,
    deployment_name: &str,
    table: &str,
    pipelines: &[Pipeline],
) -> Result<bool, RuntimeError> {
    let live = store
        .list_pipelines()
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    for pipeline in pipelines {
        if !pipeline_references_table(pipeline, table)
            || (pipeline.deployment_name != deployment_name && !pipeline.deployment_name.is_empty())
        {
            // `pipelines` arg may still have deployment_name unset before persist; match by name.
            if !pipeline_references_table(pipeline, table) {
                continue;
            }
        }
        if let Some(stored) = live
            .iter()
            .find(|p| p.deployment_name == deployment_name && p.name == pipeline.name)
        {
            if stored.paused {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

async fn persist_initial_load_pause(
    store: &PlatformStore,
    deployment_name: &str,
    schema: &str,
    table: &str,
    primary_key: &[String],
    columns: &[BaseColumn],
    omitted_columns: &[OmittedColumn],
    rows_loaded: usize,
    low_watermark: Option<CapturePosition>,
    cursor: Option<Vec<serde_json::Value>>,
) -> Result<(), RuntimeError> {
    let handoff = handoff_from_optional_low_watermark(low_watermark);
    let dataset = BaseDataset {
        deployment_name: deployment_name.to_string(),
        source_table: table.to_string(),
        source_schema: schema.to_string(),
        status: "initial_load_paused".to_string(),
        primary_key: primary_key.to_vec(),
        columns: columns.to_vec(),
        omitted_columns: omitted_columns.to_vec(),
        row_count: rows_loaded as i32,
        sync_applied_changes: 0,
        sync_health: "unknown".to_string(),
        capture_low_watermark: handoff.map(|h| h.low_watermark),
        capture_checkpoint: handoff.map(|h| h.checkpoint),
        sync_lag: 0,
        source_alignment: "unknown".to_string(),
        source_alignment_checked_rows: 0,
        source_alignment_mismatched_rows: 0,
        initial_load_cursor: cursor,
    };
    store
        .append_base_dataset_chunk(&dataset, &[], rows_loaded as i32)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    println!(
        "Initial Load paused: Base Dataset {table} ({} rows) — durable progress retained; \
         re-run `migraloop apply` (or resume + apply) to continue without tearing down the Deployment",
        rows_loaded
    );
    emit_event(
        "initial_load_paused",
        &[
            ("table", EventValue::from(table)),
            ("rows", EventValue::from(rows_loaded as i64)),
            (
                "low_watermark",
                EventValue::from(handoff.map(|h| h.low_watermark).unwrap_or(0)),
            ),
            ("deployment", EventValue::from(deployment_name)),
        ],
    );
    Ok(())
}

async fn ensure_base_primary_key(
    store: &PlatformStore,
    deployment: &Deployment,
    source_schema: &str,
    source_table: &str,
    configured_timezone: Option<&str>,
) -> Result<(), RuntimeError> {
    let deployment_name = &deployment.name;
    let (dataset, _) = store
        .get_base_rows(source_table, Some(deployment_name))
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    if !dataset.primary_key.is_empty() {
        return Ok(());
    }

    // Metadata-only: one bounded chunk for PK — never a full-table Initial Load slam.
    let source = source_engine_from_connection(&deployment.source)?;
    let chunk = source
        .initial_load_chunk(
            source_schema,
            source_table,
            configured_timezone,
            &InitialLoadChunkOptions {
                chunk_size: 1,
                offset: 0,
                established_watermark: None,
            },
        )
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    if chunk.primary_key.is_empty() {
        return Err(RuntimeError::Failed(format!(
            "Source table {source_table} has no primary key for Output Identity"
        )));
    }

    store
        .update_base_primary_key(
            deployment_name,
            source_schema,
            source_table,
            &chunk.primary_key,
        )
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    Ok(())
}

/// Load Source schema metadata for apply-time Managed field validation.
///
/// Goes through the Source engine interface (contract catalog or OCI discovery).
pub(crate) fn source_columns_for_pipeline(
    deployment: &Deployment,
    schema: &str,
    table: &str,
) -> Result<Vec<SourceColumn>, RuntimeError> {
    let source = source_engine_from_connection(&deployment.source)?;
    source
        .discover_schema(schema, table)
        .map_err(|err| RuntimeError::Failed(err.to_string()))
}

pub(crate) fn oracle_source_connect(
    source: &SystemConnection,
) -> Result<OracleSourceConnect, RuntimeError> {
    if source.port <= 0 || source.port > u16::MAX as i32 {
        return Err(RuntimeError::Failed(
            "source.port must be a valid TCP port".to_string(),
        ));
    }
    Ok(OracleSourceConnect {
        host: source.host.clone(),
        port: source.port as u16,
        database: source.database.clone(),
        username: source.username.clone(),
        tls: source.tls.clone(),
    })
}

/// Fail-fast Source Prerequisites before capture runs (ADR-0021).
///
/// Delegates to the Source engine adapter (contract or OCI). Read-only; never
/// auto-alters customer Source configuration.
pub(crate) fn ensure_source_prerequisites(
    source: &SystemConnection,
    source_tables: &[String],
) -> Result<(), RuntimeError> {
    let engine = source_engine_from_connection(source)?;
    engine
        .check_prerequisites(source_tables)
        .map_err(|err| RuntimeError::Failed(err.to_string()))
}

fn pipeline_source_tables(pipelines: &[Pipeline]) -> Vec<String> {
    let mut tables = BTreeSet::new();
    for pipeline in pipelines {
        for (_, table) in pipeline_base_table_refs(pipeline) {
            if !table.is_empty() {
                tables.insert(table);
            }
        }
    }
    tables.into_iter().collect()
}

/// Whether a Pipeline has a Target Binding configured for Delivery.
pub(crate) fn pipeline_has_target(pipeline: &Pipeline) -> bool {
    (pipeline.mode == "direct" || pipeline.mode == "transform")
        && !pipeline.target_collection.is_empty()
}

/// Whether two Pipeline declarations are semantically the same (mode, Source table,
/// Target Binding, field mappings, transform / Output Identity) — excluding metadata
/// such as `description`. A semantic difference is a Pipeline revision/Change.
///
/// Used so runtime Pipeline add can preserve Delivery progress for unchanged Pipelines
/// (ADR-0007) without treating a declaration change as a no-op add.
fn pipeline_semantic_unchanged(previous: &Pipeline, next: &Pipeline) -> bool {
    previous.mode == next.mode
        && previous.source_table == next.source_table
        && previous.source_schema == next.source_schema
        && previous.target_collection == next.target_collection
        && previous.field_mappings == next.field_mappings
        && previous.output_identity == next.output_identity
        && previous.transform_json == next.transform_json
}

fn pipeline_metadata_only_change(previous: &Pipeline, next: &Pipeline) -> bool {
    pipeline_semantic_unchanged(previous, next) && previous.description != next.description
}

/// Preserve Delivery progress and pause for Pipelines whose semantic declaration is
/// unchanged (including metadata-only description edits).
///
/// `pipelines_from_document` always starts at pending/0; without this merge, every
/// apply would look like a Deployment restart for already-running Pipelines.
fn preserve_unchanged_pipeline_delivery(existing: &[Pipeline], pipelines: &mut [Pipeline]) {
    for pipeline in pipelines.iter_mut() {
        let Some(previous) = existing.iter().find(|p| p.name == pipeline.name) else {
            continue;
        };
        if pipeline_semantic_unchanged(previous, pipeline) {
            pipeline.delivery_status = previous.delivery_status.clone();
            pipeline.delivery_applied_changes = previous.delivery_applied_changes;
            pipeline.delivery_lag = previous.delivery_lag;
            pipeline.paused = previous.paused;
            pipeline.drift_status = previous.drift_status.clone();
            pipeline.drift_checked_rows = previous.drift_checked_rows;
            pipeline.drift_mismatched_rows = previous.drift_mismatched_rows;
        }
    }
}

/// Pipelines that need ordinary Delivery start: newly added, or semantically
/// unchanged but not yet delivered. Semantic revisions use the revision path.
fn pipelines_needing_delivery_start<'a>(
    existing: &[Pipeline],
    pipelines: &'a [Pipeline],
) -> Vec<&'a Pipeline> {
    pipelines
        .iter()
        .filter(|pipeline| {
            if !pipeline_has_target(pipeline) || pipeline.paused {
                return false;
            }
            let Some(previous) = existing.iter().find(|p| p.name == pipeline.name) else {
                // Newly added Pipeline — start Delivery after Initial Load as needed.
                return true;
            };
            if !pipeline_semantic_unchanged(previous, pipeline) {
                // Semantic revision — handled by pause → rebuild → re-Deliver.
                return false;
            }
            // Unchanged, already-delivered Pipelines keep running without re-Delivery.
            previous.delivery_status != "delivered"
        })
        .collect()
}

/// Existing Pipelines whose semantic declaration changed (revision rebuild path).
fn pipelines_needing_revision_rebuild<'a>(
    existing: &[Pipeline],
    pipelines: &'a [Pipeline],
) -> Vec<&'a Pipeline> {
    pipelines
        .iter()
        .filter(|pipeline| {
            if !pipeline_has_target(pipeline) {
                return false;
            }
            let Some(previous) = existing.iter().find(|p| p.name == pipeline.name) else {
                return false;
            };
            !pipeline_semantic_unchanged(previous, pipeline)
        })
        .collect()
}

fn pipelines_with_metadata_only_change<'a>(
    existing: &[Pipeline],
    pipelines: &'a [Pipeline],
) -> Vec<&'a Pipeline> {
    pipelines
        .iter()
        .filter(|pipeline| {
            let Some(previous) = existing.iter().find(|p| p.name == pipeline.name) else {
                return false;
            };
            pipeline_metadata_only_change(previous, pipeline)
        })
        .collect()
}

pub(crate) async fn deliver_pipelines(
    store: &PlatformStore,
    deployment: &Deployment,
    pipelines: &[&Pipeline],
) -> Result<(), RuntimeError> {
    deliver_pipelines_with_options(store, deployment, pipelines, false, false).await
}

/// Deliver Pipelines. `reconcile_deletes` removes Target identities that disappeared
/// (used for revision rebuild and resume catch-up). When `ignore_paused` is true,
/// Delivery runs even if the Pipeline is still marked paused (revision transition).
pub(crate) async fn deliver_pipelines_with_options(
    store: &PlatformStore,
    deployment: &Deployment,
    pipelines: &[&Pipeline],
    reconcile_deletes: bool,
    ignore_paused: bool,
) -> Result<(), RuntimeError> {
    let needs_delivery = pipelines
        .iter()
        .any(|p| pipeline_has_target(p) && (ignore_paused || !p.paused));
    if !needs_delivery {
        return Ok(());
    }

    let target = target_engine_from_deployment(deployment)?;

    for pipeline in pipelines {
        if !pipeline_has_target(pipeline) || (!ignore_paused && pipeline.paused) {
            continue;
        }

        match pipeline.mode.as_str() {
            "direct" => {
                deliver_direct_pipeline_with_options(
                    store,
                    deployment,
                    pipeline,
                    &target,
                    reconcile_deletes,
                )
                .await?;
            }
            "transform" => {
                deliver_transform_pipeline_with_options(
                    store,
                    deployment,
                    pipeline,
                    &target,
                    reconcile_deletes,
                )
                .await?;
            }
            other => {
                return Err(RuntimeError::Failed(format!(
                    "unsupported pipeline.mode {other:?} for Delivery"
                )));
            }
        }
    }

    Ok(())
}

/// Direct Pipeline Delivery. When `reconcile_deletes` is true (resume / revision),
/// also remove Target documents whose Output Identity is no longer in Base.
pub(crate) async fn deliver_direct_pipeline_with_options<T: TargetEngine>(
    store: &PlatformStore,
    deployment: &Deployment,
    pipeline: &Pipeline,
    target: &T,
    reconcile_deletes: bool,
) -> Result<(), RuntimeError> {
    let (dataset, rows) = store
        .get_base_rows(&pipeline.source_table, Some(&pipeline.deployment_name))
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    let source_columns =
        source_columns_for_pipeline(deployment, &pipeline.source_schema, &pipeline.source_table)?;
    let managed_names: BTreeSet<String> = dataset
        .columns
        .iter()
        .map(|c| c.name.clone())
        .filter(|name| {
            !matches!(
                pipeline.field_mappings.get(name),
                Some(ManagedFieldAs::Omit)
            )
        })
        .collect();
    validate_pipeline_managed_fields(pipeline, &source_columns, &managed_names)?;

    if dataset.primary_key.is_empty() {
        return Err(RuntimeError::Failed(
            "Base Dataset has no primary key for Output Identity".to_string(),
        ));
    }

    let mut documents = Vec::with_capacity(rows.len());
    let mut live_identities = BTreeSet::new();
    for row in &rows {
        // Direct Pipeline Managed fields default to all supported Base columns,
        // minus omit mappings; unsafe NUMBER requires string/omit (ADR-0023).
        let document =
            delivery_document_for_row(&row.data, &dataset.primary_key, &dataset.columns, pipeline)?;
        live_identities.insert(identity_key(&document.identity));
        documents.push(document);
    }

    let delivered = target
        .upsert_managed(&pipeline.target_collection, &documents)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    let mut deleted = 0usize;
    if reconcile_deletes {
        deleted =
            reconcile_target_deletes(target, &pipeline.target_collection, &live_identities).await?;
    }

    store
        .record_delivery_progress(
            &pipeline.deployment_name,
            &pipeline.name,
            Some("delivered"),
            Some((delivered + deleted) as i32),
            None,
        )
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    if reconcile_deletes && deleted > 0 {
        println!(
            "Delivery complete: Pipeline {} → {}.{} ({} documents, {} deletes)",
            pipeline.name,
            deployment.target.database,
            pipeline.target_collection,
            delivered,
            deleted
        );
    } else {
        println!(
            "Delivery complete: Pipeline {} → {}.{} ({} documents)",
            pipeline.name, deployment.target.database, pipeline.target_collection, delivered
        );
    }
    emit_event(
        "delivery_complete",
        &[
            ("pipeline", EventValue::from(pipeline.name.as_str())),
            (
                "deployment",
                EventValue::from(pipeline.deployment_name.as_str()),
            ),
            (
                "collection",
                EventValue::from(pipeline.target_collection.as_str()),
            ),
            ("documents", EventValue::from(delivered)),
            ("deletes", EventValue::from(deleted)),
        ],
    );
    Ok(())
}

pub(crate) fn identity_key(identity: &serde_json::Value) -> String {
    output_identity_key(identity)
}

pub(crate) fn target_document_identity_key(document: &serde_json::Value) -> Option<String> {
    document.get("_id").map(identity_key)
}

async fn reconcile_target_deletes<T: TargetEngine>(
    target: &T,
    collection: &str,
    live_identities: &BTreeSet<String>,
) -> Result<usize, RuntimeError> {
    let documents = target
        .list_documents(collection)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    let mut stale = Vec::new();
    for document in documents {
        let Some(key) = target_document_identity_key(&document) else {
            continue;
        };
        if !live_identities.contains(&key) {
            if let Some(id) = document.get("_id") {
                stale.push(id.clone());
            }
        }
    }
    if stale.is_empty() {
        return Ok(0);
    }
    target
        .delete_by_identity(collection, &stale)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))
}

pub(crate) async fn deliver_transform_pipeline_with_options<T: TargetEngine>(
    store: &PlatformStore,
    deployment: &Deployment,
    pipeline: &Pipeline,
    target: &T,
    reconcile_deletes: bool,
) -> Result<(), RuntimeError> {
    if pipeline.output_identity.is_empty() {
        return Err(RuntimeError::Failed(format!(
            "Transform Pipeline {} requires outputIdentity before it can run",
            pipeline.name
        )));
    }

    let (base, base_rows) = store
        .get_base_rows(&pipeline.source_table, Some(&pipeline.deployment_name))
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    let ops = transform_ops_from_pipeline(pipeline)?;
    let (secondary, secondary_columns) =
        load_secondary_bases_and_columns_for_pipeline(store, pipeline, &ops).await?;
    let base_maps: Vec<_> = base_rows.iter().map(|r| r.data.clone()).collect();
    let derived_rows =
        evaluate_transform_with_bases(&ops, &base_maps, &secondary).map_err(|err| {
            RuntimeError::Failed(format!("Transform Pipeline {}: {err}", pipeline.name))
        })?;

    let derived_columns = derived_columns_for_ops(
        &base.columns,
        &ops,
        &derived_rows,
        &secondary_columns,
    );
    let source_columns =
        source_columns_for_pipeline(deployment, &pipeline.source_schema, &pipeline.source_table)?;
    let managed_names: BTreeSet<String> = derived_columns
        .iter()
        .map(|c| c.name.clone())
        .filter(|name| {
            !matches!(
                pipeline.field_mappings.get(name),
                Some(ManagedFieldAs::Omit)
            )
        })
        .collect();
    validate_pipeline_managed_fields(pipeline, &source_columns, &managed_names)?;

    for field in &pipeline.output_identity {
        if !derived_columns.iter().any(|c| c.name == *field) {
            return Err(RuntimeError::Failed(format!(
                "Transform Pipeline {} outputIdentity field {field} is not present in Derived output",
                pipeline.name
            )));
        }
    }

    let dataset = DerivedDataset {
        deployment_name: pipeline.deployment_name.clone(),
        pipeline_name: pipeline.name.clone(),
        status: "materialized".to_string(),
        output_identity: pipeline.output_identity.clone(),
        columns: derived_columns.clone(),
        row_count: derived_rows.len() as i32,
    };
    store
        .replace_derived_dataset(&dataset, &derived_rows)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    persist_initial_maintenance_state(store, pipeline, &ops, &base_maps).await?;

    println!(
        "Derived Dataset materialized: Pipeline {} ({} rows)",
        pipeline.name, dataset.row_count
    );

    let mut documents = Vec::with_capacity(derived_rows.len());
    let mut live_identities = BTreeSet::new();
    for row in &derived_rows {
        let document =
            delivery_document_for_row(row, &pipeline.output_identity, &derived_columns, pipeline)?;
        live_identities.insert(identity_key(&document.identity));
        documents.push(document);
    }

    let delivered = target
        .upsert_managed(&pipeline.target_collection, &documents)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    let mut deleted = 0usize;
    if reconcile_deletes {
        deleted =
            reconcile_target_deletes(target, &pipeline.target_collection, &live_identities).await?;
    }

    store
        .record_delivery_progress(
            &pipeline.deployment_name,
            &pipeline.name,
            Some("delivered"),
            Some((delivered + deleted) as i32),
            None,
        )
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    if reconcile_deletes && deleted > 0 {
        println!(
            "Delivery complete: Pipeline {} → {}.{} ({} documents, {} deletes)",
            pipeline.name,
            deployment.target.database,
            pipeline.target_collection,
            delivered,
            deleted
        );
    } else {
        println!(
            "Delivery complete: Pipeline {} → {}.{} ({} documents)",
            pipeline.name, deployment.target.database, pipeline.target_collection, delivered
        );
    }
    Ok(())
}

fn to_output_columns(columns: &[BaseColumn]) -> Vec<OutputColumn> {
    columns.iter().cloned().map(ColumnShape::from).collect()
}

fn from_output_columns(columns: Vec<OutputColumn>) -> Vec<BaseColumn> {
    columns.into_iter().map(BaseColumn::from).collect()
}

/// Derived columns via the transform schema-inference interface (no TransformOp walk here).
pub(crate) fn derived_columns_for_ops(
    base_columns: &[BaseColumn],
    ops: &[TransformOp],
    derived_rows: &[serde_json::Map<String, serde_json::Value>],
    secondary_columns: &[BaseColumn],
) -> Vec<BaseColumn> {
    from_output_columns(infer_derived_columns(
        ops,
        &to_output_columns(base_columns),
        &to_output_columns(secondary_columns),
        derived_rows,
    ))
}

async fn persist_initial_maintenance_state(
    store: &PlatformStore,
    pipeline: &Pipeline,
    ops: &[TransformOp],
    base_rows: &[serde_json::Map<String, serde_json::Value>],
) -> Result<(), RuntimeError> {
    match initial_maintenance_state(ops, base_rows).map_err(|err| {
        RuntimeError::Failed(format!(
            "Transform Pipeline {}: failed to build Maintenance State: {err}",
            pipeline.name
        ))
    })? {
        Some(blob) => persist_maintenance_state_blob(store, pipeline, &blob).await,
        None => store
            .delete_maintenance_state(&pipeline.deployment_name, &pipeline.name)
            .await
            .map_err(|err| RuntimeError::Failed(err.to_string())),
    }
}

pub(crate) async fn persist_maintenance_state_blob(
    store: &PlatformStore,
    pipeline: &Pipeline,
    state: &MaintenanceStateBlob,
) -> Result<(), RuntimeError> {
    store
        .replace_maintenance_state(
            &pipeline.deployment_name,
            &pipeline.name,
            state.as_str(),
        )
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))
}

/// Apply a Deployment: persist, table-level Initial Load, and Delivery start.
///
/// This is the Deployment runtime's primary Operator verb for the apply path.
/// Table-level Initial Load and Direct Pipeline Delivery (plus Transform first
/// Delivery when a Pipeline revision needs rebuild) are sequenced here — not in
/// the CLI clap adapter.
///
/// Callers (CLI adapter, in-process tests) supply an already-open Platform Store
/// session and resolved Deployment / Pipeline values — config parsing stays outside.
pub async fn apply(
    store: &PlatformStore,
    deployment: Deployment,
    mut pipelines: Vec<Pipeline>,
) -> Result<(), RuntimeError> {
    ensure_store_session_healthy(store).await?;

    // ADR-0021: fail-fast Source Prerequisites before discovery / Initial Load.
    // Deployment-only apply (no Pipeline tables) does not open LogMiner yet.
    let source_tables = pipeline_source_tables(&pipelines);
    if deployment.source.kind.eq_ignore_ascii_case("oracle") && !source_tables.is_empty() {
        ensure_source_prerequisites(&deployment.source, &source_tables)?;
    }

    // Apply-time Managed validation before Initial Load / Delivery so unsafe NUMBER
    // and unsupported Managed inputs fail configure-time (ADR-0018 / ADR-0023).
    // Real Oracle hosts discover schema via OCI; contract/stub use the contract catalog.
    for pipeline in &pipelines {
        let source_columns = source_columns_for_pipeline(
            &deployment,
            &pipeline.source_schema,
            &pipeline.source_table,
        )?;
        let managed_names: BTreeSet<String> = source_columns
            .iter()
            .filter(|c| c.supported)
            .map(|c| c.name.clone())
            .filter(|name| {
                !matches!(
                    pipeline.field_mappings.get(name),
                    Some(ManagedFieldAs::Omit)
                )
            })
            .collect();
        validate_pipeline_managed_fields(pipeline, &source_columns, &managed_names)?;
    }

    let existing_pipelines = store
        .list_pipelines()
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?
        .into_iter()
        .filter(|p| p.deployment_name == deployment.name)
        .collect::<Vec<_>>();
    let existing_names: BTreeSet<String> =
        existing_pipelines.iter().map(|p| p.name.clone()).collect();
    // Owned summaries so we can mutate `pipelines` below without overlapping borrows.
    let added_pipeline_summaries: Vec<(String, String)> = pipelines
        .iter()
        .filter(|p| !existing_names.contains(&p.name))
        .map(|p| (p.name.clone(), p.source_table.clone()))
        .collect();

    // Runtime add (ADR-0007): keep already-running Pipelines' Delivery progress
    // (and Operator pause) when the semantic declaration is unchanged.
    preserve_unchanged_pipeline_delivery(&existing_pipelines, &mut pipelines);

    let revision_names: BTreeSet<String> =
        pipelines_needing_revision_rebuild(&existing_pipelines, &pipelines)
            .into_iter()
            .map(|p| p.name.clone())
            .collect();
    let metadata_only_names: BTreeSet<String> =
        pipelines_with_metadata_only_change(&existing_pipelines, &pipelines)
            .into_iter()
            .map(|p| p.name.clone())
            .collect();

    // Change (ADR-0007): pause old Delivery before swapping the revision so a
    // concurrent sync cannot Deliver under the previous transform/binding.
    for name in &revision_names {
        if let Some(previous) = existing_pipelines.iter().find(|p| p.name == *name) {
            if !previous.paused {
                store
                    .set_pipeline_paused(&deployment.name, name, true)
                    .await
                    .map_err(|err| RuntimeError::Failed(err.to_string()))?;
            }
            println!("Pipeline revision: {name} — paused old Delivery");
        }
        if let Some(pipeline) = pipelines.iter_mut().find(|p| p.name == *name) {
            // Hold pause through replace until rebuild/re-Deliver finishes.
            pipeline.paused = true;
            pipeline.delivery_status = if pipeline_has_target(pipeline) {
                "pending".to_string()
            } else {
                "not_configured".to_string()
            };
            pipeline.delivery_applied_changes = 0;
            pipeline.drift_status = "unknown".to_string();
            pipeline.drift_checked_rows = 0;
            pipeline.drift_mismatched_rows = 0;
        }
    }

    store
        .upsert_deployment(&deployment)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    store
        .replace_pipelines(&deployment.name, &pipelines)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    // Table-level Initial Load only for newly referenced tables; existing Bases stay
    // on their incremental path (ADR-0019). Shared Bases are never rebuilt for a
    // Pipeline revision (ADR-0007 Change).
    sync_base_datasets_for_pipelines(store, &deployment, &pipelines).await?;

    if !existing_pipelines.is_empty() {
        for (name, source_table) in &added_pipeline_summaries {
            println!("Runtime Pipeline add: {name} (source={source_table})");
        }
    }

    for name in &metadata_only_names {
        println!("Pipeline revision: {name} (metadata-only; rebuild skipped)");
    }

    // Semantic revisions: rebuild Derived / re-Deliver with delete reconciliation,
    // then clear the transition pause so incremental work continues.
    let to_revise: Vec<&Pipeline> = pipelines
        .iter()
        .filter(|p| revision_names.contains(&p.name))
        .collect();
    if !to_revise.is_empty() {
        let reconcile_deletes = true;
        let ignore_paused = true;
        deliver_pipelines_with_options(
            store,
            &deployment,
            &to_revise,
            reconcile_deletes,
            ignore_paused,
        )
        .await?;
        for pipeline in &to_revise {
            store
                .set_pipeline_paused(&deployment.name, &pipeline.name, false)
                .await
                .map_err(|err| RuntimeError::Failed(err.to_string()))?;
            println!(
                "Pipeline revision: {} — rebuilt and re-Delivered; incremental resumed",
                pipeline.name
            );
        }
    }

    // Start Delivery only for Pipelines that need ordinary first Delivery; do not
    // re-Deliver unchanged already-delivered Pipelines (others keep running — ADR-0007 Add).
    let to_deliver = pipelines_needing_delivery_start(&existing_pipelines, &pipelines);
    deliver_pipelines(store, &deployment, &to_deliver).await?;

    println!("Deployment applied: {}", deployment.name);
    Ok(())
}
