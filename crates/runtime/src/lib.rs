//! Deployment runtime public interface: Operator Deployment verbs plus necessary
//! session / factory entry points (issue #172 / #208).
//!
//! Apply / Initial Load / Delivery start live in the [`apply`] module; Incremental
//! Sync lives in [`incremental`] — split by concept for navigability without
//! widening this seam.
//!
//! **Verbs:** [`apply()`] / [`apply_with_options`] / [`apply_with_engines`] (typed
//! [`ApplyOptions`]), Incremental Sync ([`run_incremental_sync`],
//! [`run_incremental_sync_with_engines`], [`run_continuous_incremental_sync`],
//! [`supervise_continuous_incremental_sync`] with typed [`SyncOptions`]), Pipeline
//! lifecycle ([`pause_pipeline`], [`resume_pipeline`], [`remove_pipeline`]),
//! [`source_alignment_check`], [`drift_check`], status inventory ([`status_inventory`],
//! [`status_inventory_from_url`]), Observability Surface assembly
//! ([`assemble_observability_surface`]), Capacity Estimate
//! ([`capacity_estimate_from_inventory`]), cutover facts / hand-off
//! ([`cutover_facts_from_base`], [`handoff_from_low_watermark`],
//! [`resume_for_incremental`]), and inspect
//! ([`inspect_base_rows`], [`inspect_derived_rows`], [`inspect_target_documents`]).
//!
//! **Session / factory entry points:** [`source_engine_from_connection`],
//! [`target_engine_from_deployment`], plus structured observability emit helpers
//! used by the Operator edge. Kind selection lives in the factories; apply / Sync
//! orchestration does not re-gate on `kind == "oracle"` (#206).
//!
//! Internal helpers (shared Base/Delivery crumbs, identity display) stay `pub(crate)` —
//! they fail the deletion test as a public surface. Cutover, Poison, Schema Change,
//! Observability, and Backpressure remain distinct policy owners. The Operator CLI
//! is a thin adapter (clap/config/env + narrative formatting) over this interface.

use std::collections::{BTreeMap, BTreeSet};

use migraloop_capture::{
    OracleLogMinerSource, OracleSourceConnect, SourceColumn, SourceEngine,
};
use migraloop_delivery::{
    DeliveryDocument, ManagedFieldAs, MongoTargetConnection, TargetEngine,
};
use migraloop_platform_store::{
    check_store_settings, disk_warn_message, BaseColumn, Deployment, Pipeline, PlatformStore,
    PlatformStoreHealth, SecretRef, SystemConnection,
};
use migraloop_transform::{parse_transform_steps, secondary_base_refs, TransformOp};
use migraloop_types::{classify_number, resolve_secret_ref, NumberMongoMapping};
use thiserror::Error;

#[cfg(test)]
mod engines;
mod apply;
mod apply_options;
mod backpressure;
mod capacity;
mod cutover;
mod observability;
mod incremental;
mod lifecycle;
mod poison;
mod schema_impact;
mod sync_options;
mod verify_repair;

pub use cutover::{
    cutover_facts_from_base, handoff_from_low_watermark, handoff_from_optional_low_watermark,
    resume_for_incremental, CutoverFacts, CutoverHandoff, IncrementalResume,
};
pub use capacity::{
    assemble_component_pressure, assemble_component_pressure_from_surface,
    capacity_estimate_from_inventory, estimate_capacity, infra_saturated, CapacityEstimate,
    ComponentPressure, ComponentPressureOverride, ComponentPressureOverrides,
    CAPACITY_REFERENCE_E2E_QPS, COMPONENT_APP, COMPONENT_PLATFORM_STORE, COMPONENT_PRESSURE_NAMES,
    COMPONENT_SOURCE, COMPONENT_TARGET,
};
pub use observability::{
    assemble_observability_surface, emit_event, render_prometheus_metrics, BaseSyncObservation,
    DeliveryHealth, EventValue, ObservabilitySurface, PipelineDeliveryObservation, SyncHealth,
};
pub(crate) use observability::sync_health_label_for_progress;

pub use incremental::{
    run_incremental_sync, run_incremental_sync_with_engines, run_continuous_incremental_sync,
    supervise_continuous_incremental_sync, SyncCycleOutcome, SyncInvocation,
};
pub use apply::{apply, apply_with_engines, apply_with_options};
pub(crate) use apply::{
    deliver_direct_pipeline_with_options, deliver_transform_pipeline_with_options,
    derived_columns_for_ops, identity_key, persist_maintenance_state_blob,
    target_document_identity_key,
};
pub use apply_options::{
    ApplyOptions, ApplyOptionsOverrides, InitialLoadOptions,
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
///
/// Trusts engine-agnostic Source column metadata (`supported` + [`SourceColumn::data_type`])
/// at the runtime seam. Oracle allow-list / type brand stay adapter-private (#206).
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
            Some(col) if !col.supported => {
                if *mapping != ManagedFieldAs::Omit {
                    return Err(RuntimeError::Failed(format!(
                        "Pipeline {}: unsupported type {} cannot be used as a \
                         Managed/transform input (column {field})",
                        pipeline.name,
                        col.data_type,
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
        columns: columns.to_vec(),
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

/// Load Source schema metadata for apply-time Managed field validation.
///
/// Goes through the Source engine interface (injected or factory-selected).
pub(crate) fn source_columns_for_pipeline<S: SourceEngine>(
    source: &S,
    schema: &str,
    table: &str,
) -> Result<Vec<SourceColumn>, RuntimeError> {
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
/// Delegates to the Source engine adapter (injected or factory-selected).
/// Read-only; never auto-alters customer Source configuration.
pub(crate) fn ensure_source_prerequisites<S: SourceEngine>(
    source: &S,
    source_tables: &[String],
) -> Result<(), RuntimeError> {
    source
        .check_prerequisites(source_tables)
        .map_err(|err| RuntimeError::Failed(err.to_string()))
}

/// Whether a Pipeline has a Target Binding configured for Delivery.
pub(crate) fn pipeline_has_target(pipeline: &Pipeline) -> bool {
    (pipeline.mode == "direct" || pipeline.mode == "transform")
        && !pipeline.target_collection.is_empty()
}

