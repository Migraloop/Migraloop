//! Incremental Sync orchestration for the Deployment runtime (#153 / #176 / #180).
//!
//! Owns Incremental Capture (including continuous supervise/resume), overlap
//! apply ordering (Deliver-before-checkpoint + change-id dedupe), and
//! ChangeEvent → Base Dataset apply. Poison quarantine, Schema Change pause,
//! and bounded Backpressure sit behind small internal seams (`crate::poison`,
//! `crate::schema_impact`, `crate::backpressure`) with typed [`SyncOptions`] on
//! the sync invocation. Cutover watermark / checkpoint / readiness live in
//! [`crate::cutover`] (ADR-0004 / #175). The Operator CLI is a thin adapter over
//! [`sync_incremental`] / [`supervise_continuous_incremental_sync`].

use std::collections::{BTreeMap, BTreeSet};

use migraloop_capture::{
    normalize_change_temporals, CapturePosition, ChangeEvent, ChangeOp,
    IncrementalCaptureSession, SchemaChangeEvent, SourceColumn, SourceEngine,
};
use migraloop_delivery::TargetEngine;
use migraloop_platform_store::{BaseColumn, BaseDataset, Pipeline, PlatformStore};
use migraloop_transform::{
    analyze_base_change, evaluate_transform_for_identities_with_bases, identity_matches_row,
    secondary_base_refs, AffectOutcome, BaseChangeContext, BaseChangeKind, MaintenanceStateBlob,
    TransformOp,
};

use crate::backpressure::{emit_backpressure, window_under_backpressure};
use crate::cutover::resume_for_incremental;
use crate::observability::{emit_event, EventValue};
use crate::poison::{
    delete_with_bounded_retries, quarantine_poison_change, upsert_with_bounded_retries,
    with_bounded_delivery_retries,
};
use crate::schema_impact::apply_schema_change_impacts;
use crate::sync_options::SyncOptions;
use crate::{
    delivery_document_for_row, derived_columns_for_ops, ensure_store_session_healthy,
    load_secondary_bases_and_columns_for_pipeline, output_identity_from_row,
    persist_maintenance_state_blob, pipeline_base_table_refs, pipeline_references_table,
    source_engine_from_connection, source_timezone_opt, target_engine_from_deployment,
    transform_ops_from_pipeline, RuntimeError,
};

async fn load_maintenance_state_blob(
    store: &PlatformStore,
    pipeline: &Pipeline,
) -> Result<Option<MaintenanceStateBlob>, RuntimeError> {
    match store
        .get_maintenance_state_json(
            &pipeline.deployment_name,
            &pipeline.name,
        )
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?
    {
        Some(json) => Ok(Some(MaintenanceStateBlob::from_persisted(json))),
        None => Ok(None),
    }
}

fn base_change_kind(op: ChangeOp) -> BaseChangeKind {
    match op {
        ChangeOp::Insert => BaseChangeKind::Insert,
        ChangeOp::Update => BaseChangeKind::Update,
        ChangeOp::Delete => BaseChangeKind::Delete,
    }
}

/// Incremental Transform maintenance for one Base change (Affect Analysis driven).
///
/// `changed_table` / `changed_base_rows` are the Base that just received the change
/// (primary `source.table` or an `equiLookup.from` / `union.from` secondary).
///
/// Returns the next opaque Maintenance State blob when the transform uses one. Callers
/// persist it only after durable Base/checkpoint progress so a Sync retry cannot
/// analyze/bump the same change twice.
async fn maintain_transform_pipeline_for_change<T: TargetEngine>(
    store: &PlatformStore,
    pipeline: &Pipeline,
    target: &T,
    changed_table: &str,
    changed_base_rows: &[serde_json::Map<String, serde_json::Value>],
    change: &ChangeEvent,
    pre_apply: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<Option<MaintenanceStateBlob>, RuntimeError> {
    let ops = transform_ops_from_pipeline(pipeline)?;
    let after = match change.op {
        ChangeOp::Insert | ChangeOp::Update => {
            Some(
                changed_base_rows
                    .iter()
                    .find(|row| row_matches_identity(row, &change.identity))
                    .cloned()
                    .ok_or_else(|| {
                        RuntimeError::Failed(format!(
                            "Base Dataset {changed_table} missing row for change identity {:?} after apply",
                            change.identity
                        ))
                    })?,
            )
        }
        ChangeOp::Delete => None,
    };

    let (primary_columns, primary_rows) =
        if changed_table.eq_ignore_ascii_case(&pipeline.source_table) {
            let (base, _) = store
                .get_base_rows(&pipeline.source_table, Some(&pipeline.deployment_name))
                .await
                .map_err(|err| RuntimeError::Failed(err.to_string()))?;
            (base.columns, changed_base_rows.to_vec())
        } else {
            let (base, rows) = store
                .get_base_rows(&pipeline.source_table, Some(&pipeline.deployment_name))
                .await
                .map_err(|err| RuntimeError::Failed(err.to_string()))?;
            (base.columns, rows.into_iter().map(|r| r.data).collect())
        };

    let kind = base_change_kind(change.op);
    let is_primary = changed_table.eq_ignore_ascii_case(&pipeline.source_table);

    // Load opaque prior Maintenance State when present; transform decides whether it applies.
    let prior_state = load_maintenance_state_blob(store, pipeline).await?;

    // Load secondary Bases before Affect Analysis so equiLookup/union/unwind can
    // resolve multi-Base Output Identities (including disappeared identities).
    let (mut secondary, secondary_columns) =
        load_secondary_bases_and_columns_for_pipeline(&store, pipeline, &ops).await?;
    if !is_primary {
        for sec in secondary_base_refs(&ops) {
            if sec.table.eq_ignore_ascii_case(changed_table) {
                // Incremental Delivery runs before the changed Base is persisted —
                // prefer the in-memory after-image for the table that just changed.
                secondary.insert(sec.table, changed_base_rows.to_vec());
            }
        }
    }

    let analysis = analyze_base_change(
        &ops,
        &BaseChangeContext {
            changed_base: changed_table,
            primary_base: &pipeline.source_table,
            kind,
            before: pre_apply,
            after: after.as_ref(),
            primary_rows: &primary_rows,
            secondary_bases: &secondary,
        },
        prior_state.as_ref(),
    )
    .map_err(|err| RuntimeError::Failed(format!("Transform Pipeline {}: {err}", pipeline.name)))?;

    match analysis.outcome {
        AffectOutcome::SkipUnusedFields => {
            println!(
                "Affect Analysis: Pipeline {} skipped (unused fields only)",
                pipeline.name
            );
        }
        AffectOutcome::SkipValueUnchanged => {
            println!(
                "Affect Analysis: Pipeline {} skipped (value-level; no Derived change)",
                pipeline.name
            );
        }
        AffectOutcome::Recompute { identities } => {
            println!(
                "Affect Analysis: Pipeline {} affected identities={}",
                pipeline.name,
                identities.len()
            );
            recompute_and_deliver_affected_identities(
                store,
                pipeline,
                target,
                &primary_columns,
                &secondary_columns,
                &primary_rows,
                &secondary,
                &ops,
                &identities,
            )
            .await?;
        }
    }
    // Caller persists after durable Base/checkpoint — return next blob only.
    Ok(analysis.next_maintenance_state)
}

async fn recompute_and_deliver_affected_identities<T: TargetEngine>(
    store: &PlatformStore,
    pipeline: &Pipeline,
    target: &T,
    base_columns: &[BaseColumn],
    secondary_columns: &[BaseColumn],
    primary_rows: &[serde_json::Map<String, serde_json::Value>],
    secondary_bases: &BTreeMap<String, Vec<serde_json::Map<String, serde_json::Value>>>,
    ops: &[TransformOp],
    identities: &[serde_json::Map<String, serde_json::Value>],
) -> Result<(), RuntimeError> {
    let recomputed = evaluate_transform_for_identities_with_bases(
        ops,
        primary_rows,
        secondary_bases,
        identities,
    )
    .map_err(|err| RuntimeError::Failed(format!("Transform Pipeline {}: {err}", pipeline.name)))?;

    let (mut dataset, existing_rows) = store
        .get_derived_rows(
            &pipeline.name,
            Some(&pipeline.deployment_name),
        )
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    // Grouped transforms (groupBy/distinct/addToSet): match by grouping keys.
    // Row-grain transforms: match Derived rows by Pipeline Output Identity only.
    // Affect Analysis identities may include shaped Managed fields (rename/addFields)
    // whose values changed — those must not leave stale Derived duplicates behind.
    let grouped = ops.iter().any(|op| {
        matches!(
            op,
            TransformOp::GroupBy { .. }
                | TransformOp::Distinct { .. }
                | TransformOp::AddToSet { .. }
        )
    });
    let identity_targets_row =
        |identity: &serde_json::Map<String, serde_json::Value>,
         row: &serde_json::Map<String, serde_json::Value>| {
            if grouped {
                identity_matches_row(identity, row)
            } else {
                pipeline
                    .output_identity
                    .iter()
                    .all(|key| match (identity.get(key), row.get(key)) {
                        (Some(a), Some(b)) => migraloop_transform::json_values_eq(a, b),
                        _ => false,
                    })
            }
        };

    let mut merged: Vec<serde_json::Map<String, serde_json::Value>> = existing_rows
        .into_iter()
        .map(|r| r.data)
        .filter(|row| !identities.iter().any(|id| identity_targets_row(id, row)))
        .collect();
    merged.extend(recomputed.clone());

    let derived_columns = derived_columns_for_ops(base_columns, ops, &merged, secondary_columns);
    dataset.status = "materialized".to_string();
    dataset.columns = derived_columns.clone();
    dataset.row_count = merged.len() as i32;
    store
        .replace_derived_dataset(&dataset, &merged)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    let mut upserts = Vec::new();
    for row in &recomputed {
        upserts.push(delivery_document_for_row(
            row,
            &pipeline.output_identity,
            &derived_columns,
            pipeline,
        )?);
    }
    let mut deletes = Vec::new();
    for identity in identities {
        let still_present = recomputed
            .iter()
            .any(|row| identity_targets_row(identity, row));
        if !still_present {
            deletes.push(output_identity_from_row(
                identity,
                &pipeline.output_identity,
            )?);
        }
    }

    let mut delivered = 0i32;
    if !upserts.is_empty() {
        delivered += target.upsert_managed(&pipeline.target_collection, &upserts).await
            .map_err(|err| RuntimeError::Failed(err.to_string()))? as i32;
    }
    if !deletes.is_empty() {
        delivered += target.delete_by_identity(&pipeline.target_collection, &deletes).await
            .map_err(|err| RuntimeError::Failed(err.to_string()))? as i32;
    }

    if delivered > 0 {
        store
            .record_delivery_progress(
                &pipeline.deployment_name,
                &pipeline.name,
                Some("delivered"),
                Some(delivered),
                None,
            )
            .await
            .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    }

    println!(
        "Delivery complete: Pipeline {} upserts={} deletes={} (Affect Analysis)",
        pipeline.name,
        upserts.len(),
        deletes.len()
    );
    Ok(())
}

fn row_matches_identity(
    row: &serde_json::Map<String, serde_json::Value>,
    identity: &std::collections::BTreeMap<String, serde_json::Value>,
) -> bool {
    identity
        .iter()
        .all(|(key, expected)| row.get(key) == Some(expected))
}

fn supported_row_from_change(
    change: &ChangeEvent,
    supported_names: &BTreeSet<String>,
    source_columns: &[SourceColumn],
    configured_timezone: Option<&str>,
) -> Result<serde_json::Map<String, serde_json::Value>, RuntimeError> {
    let Some(row) = &change.row else {
        return Err(RuntimeError::Failed(format!(
            "Incremental {:?} change for {:?} is missing row data",
            change.op, change.identity
        )));
    };
    let mut as_btree: BTreeMap<String, serde_json::Value> = row
        .iter()
        .filter(|(name, _)| supported_names.contains(*name))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect();
    normalize_change_temporals(source_columns, &mut as_btree, configured_timezone)
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    Ok(as_btree.into_iter().collect())
}

pub(crate) fn apply_change_events_to_base_rows(
    rows: &mut Vec<serde_json::Map<String, serde_json::Value>>,
    changes: &[ChangeEvent],
    supported_names: &BTreeSet<String>,
    source_columns: &[SourceColumn],
    configured_timezone: Option<&str>,
) -> Result<(), RuntimeError> {
    for change in changes {
        match change.op {
            ChangeOp::Insert | ChangeOp::Update => {
                let managed = supported_row_from_change(
                    change,
                    supported_names,
                    source_columns,
                    configured_timezone,
                )?;
                if let Some(existing) = rows
                    .iter_mut()
                    .find(|row| row_matches_identity(row, &change.identity))
                {
                    *existing = managed;
                } else {
                    rows.push(managed);
                }
            }
            ChangeOp::Delete => {
                rows.retain(|row| !row_matches_identity(row, &change.identity));
            }
        }
    }
    Ok(())
}

/// Idle poll interval between continuous Incremental Capture cycles (`migraloop run`).
///
/// Override via `MIGRALOOP_SYNC_POLL_INTERVAL_MS` (must be > 0). Default 1000ms.
fn sync_poll_interval() -> std::time::Duration {
    let ms = std::env::var("MIGRALOOP_SYNC_POLL_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &u64| *n > 0)
        .unwrap_or(1000);
    std::time::Duration::from_millis(ms)
}

async fn set_delivery_lag_for_table(
    store: &PlatformStore,
    pipelines: &[Pipeline],
    table: &str,
    delivery_lag: i32,
) -> Result<(), RuntimeError> {
    for pipeline in pipelines {
        if pipeline.target_collection.is_empty() || !pipeline_references_table(pipeline, table) {
            continue;
        }
        store
            .record_delivery_progress(
                &pipeline.deployment_name,
                &pipeline.name,
                None,
                None,
                Some(delivery_lag),
            )
            .await
            .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    }
    Ok(())
}

fn identity_value_from_change(
    change: &ChangeEvent,
    identity_fields: &[String],
) -> Result<serde_json::Value, RuntimeError> {
    let identity_map: serde_json::Map<String, serde_json::Value> = change
        .identity
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    Ok(output_identity_from_row(&identity_map, identity_fields)?)
}

/// Row DML or Source Schema Change in the Incremental Capture stream (ADR-0009).
enum IncrementalItem {
    Row(ChangeEvent),
    Schema(SchemaChangeEvent),
}

impl IncrementalItem {
    fn position(&self) -> CapturePosition {
        match self {
            Self::Row(c) => c.position,
            Self::Schema(c) => c.position,
        }
    }
}

fn base_with_sync_progress(
    dataset: &BaseDataset,
    status: impl Into<String>,
    row_count: i32,
    sync_applied_changes: i32,
    capture_checkpoint: Option<i64>,
    sync_lag: i32,
) -> BaseDataset {
    BaseDataset {
        deployment_name: dataset.deployment_name.clone(),
        source_table: dataset.source_table.clone(),
        source_schema: dataset.source_schema.clone(),
        status: status.into(),
        primary_key: dataset.primary_key.clone(),
        columns: dataset.columns.clone(),
        omitted_columns: dataset.omitted_columns.clone(),
        row_count,
        sync_applied_changes,
        sync_health: crate::sync_health_label_for_progress(sync_lag).to_string(),
        capture_low_watermark: dataset.capture_low_watermark,
        capture_checkpoint,
        sync_lag,
        source_alignment: dataset.source_alignment.clone(),
        source_alignment_checked_rows: dataset.source_alignment_checked_rows,
        source_alignment_mismatched_rows: dataset.source_alignment_mismatched_rows,
        initial_load_cursor: dataset.initial_load_cursor.clone(),
    }
}

/// One-shot Incremental Capture → Delivery (`migraloop sync`).
///
/// Uses [`SyncOptions::from_env_compat`] (thin temporary shim). Prefer
/// [`sync_incremental_with_options`] with typed options (#180).
pub async fn sync_incremental(store: &PlatformStore) -> Result<(), RuntimeError> {
    sync_incremental_with_options(store, SyncOptions::from_env_compat()).await
}

/// One-shot Incremental Capture → Delivery with typed [`SyncOptions`].
pub async fn sync_incremental_with_options(
    store: &PlatformStore,
    options: SyncOptions,
) -> Result<(), RuntimeError> {
    run_incremental_sync(store, SyncInvocation::OneShot, options)
        .await
        .map(|_| ())
}

/// Continuous Incremental Capture → Affect Analysis → Delivery inside `migraloop run`.
///
/// Prefers an open Platform Store session (issue #172). Idle-polls only when caught
/// up or when no Deployments are applied yet so compose can start before `apply`.
/// While there is pending Source work, cycles continue immediately (bounded windows
/// still apply). Errors are logged and retried — Observability metrics keep serving
/// on the same single active instance (issue #145).
pub async fn run_continuous_incremental_sync(store: &PlatformStore, options: SyncOptions) {
    let poll = sync_poll_interval();
    println!(
        "Continuous Incremental Capture: poll_interval_ms={}",
        poll.as_millis()
    );
    emit_event(
        "continuous_sync_start",
        &[(
            "poll_interval_ms",
            EventValue::from(poll.as_millis() as i64),
        )],
    );
    loop {
        match run_incremental_sync(store, SyncInvocation::ContinuousCycle, options.clone()).await {
            Ok(SyncCycleOutcome::Progressed) => {
                // Catch up backlog without waiting for the idle poll interval.
            }
            Ok(SyncCycleOutcome::Idle) => {
                tokio::time::sleep(poll).await;
            }
            Err(err) => {
                eprintln!("Continuous Incremental Capture cycle failed: {err}");
                emit_event(
                    "continuous_sync_error",
                    &[("error", EventValue::from(err.to_string()))],
                );
                tokio::time::sleep(poll).await;
            }
        }
    }
}

/// Supervise continuous Sync on an open Platform Store session so a panic does not
/// leave `/metrics` up with Sync dead.
///
/// Takes a session handle (cloned into each worker via shared pool) rather than a
/// URL reopen loop — callers open the store once at the Operator edge.
pub async fn supervise_continuous_incremental_sync(store: PlatformStore, options: SyncOptions) {
    loop {
        let worker = store.clone();
        let opts = options.clone();
        match tokio::spawn(async move { run_continuous_incremental_sync(&worker, opts).await })
            .await
        {
            Ok(()) => {
                eprintln!("Continuous Incremental Capture task ended unexpectedly; restarting");
                emit_event(
                    "continuous_sync_error",
                    &[("error", EventValue::from("task ended unexpectedly"))],
                );
            }
            Err(join_err) => {
                eprintln!("Continuous Incremental Capture task panicked: {join_err}; restarting");
                emit_event(
                    "continuous_sync_error",
                    &[("error", EventValue::from(join_err.to_string()))],
                );
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}

/// How Incremental Capture is invoked: one-shot CLI vs continuous `run` cycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncInvocation {
    /// `migraloop sync` — Lab / operator catch-up; errors when no Deployments exist.
    OneShot,
    /// Continuous cycle inside `migraloop run` — idle when no Deployments; quieter logs.
    ContinuousCycle,
}

/// Outcome of one Incremental Capture cycle (drives continuous idle-poll vs immediate catch-up).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncCycleOutcome {
    /// No pending Source changes applied this cycle (caught up, or no Deployments yet).
    Idle,
    /// At least one bounded window of Incremental work was processed.
    Progressed,
}

/// Run one Incremental Capture cycle through the Deployment runtime.
///
/// Default Operator path: constructs v1 Oracle LogMiner + MongoDB adapters via
/// factory helpers (Oracle-kind gate preserved for factory wiring). Typed knobs
/// come from `options` — CLI / Lab build them via [`SyncOptions::for_cli`];
/// in-process tests pass explicit values (#180).
pub async fn run_incremental_sync(
    store: &PlatformStore,
    invocation: SyncInvocation,
    options: SyncOptions,
) -> Result<SyncCycleOutcome, RuntimeError> {
    let prepared =
        prepare_incremental_sync_cycle(store, invocation, options.normalized()).await?;
    let Some(mut cycle) = prepared else {
        return Ok(SyncCycleOutcome::Idle);
    };

    for deployment_idx in 0..cycle.deployments.len() {
        let deployment = cycle.deployments[deployment_idx].clone();
        let Some((mut deployment_pipelines, tables)) =
            pipelines_and_tables_for_deployment(&cycle, &deployment)
        else {
            continue;
        };

        // Factory path: Oracle-kind gate + v1 concrete adapters behind interfaces.
        let source_engine = if deployment.source.kind.eq_ignore_ascii_case("oracle") {
            Some(source_engine_from_connection(&deployment.source)?)
        } else {
            None
        };
        let target = target_engine_from_deployment(&deployment)?;
        let Some(source) = source_engine.as_ref() else {
            if tables.is_empty() {
                continue;
            }
            return Err(RuntimeError::Failed(format!(
                "Incremental Capture requires an Oracle Source System (LogMiner); \
                 got kind={}",
                deployment.source.kind
            )));
        };

        sync_deployment_incremental(
            store,
            &mut cycle,
            &deployment,
            &mut deployment_pipelines,
            tables,
            source,
            &target,
        )
        .await?;
    }

    finish_incremental_sync_cycle(cycle.quiet, cycle.progressed)
}

/// Run one Incremental Capture cycle with caller-injected Source/Target engines
/// and typed [`SyncOptions`].
///
/// Seam for Fake Source/Target (and any pre-built adapters): skips Deployment
/// `source.kind` / factory Oracle-kind gates — capture capability comes from the
/// injected [`SourceEngine`]. Fault injection and queue knobs come from
/// `options` (not process env). Default CLI `apply`/`run`/`sync` keep using
/// [`run_incremental_sync`] with [`SyncOptions::for_cli`] (typed flags primary;
/// env fault injection is a thin temporary shim) with no Operator-visible change.
pub async fn run_incremental_sync_with_engines<S: SourceEngine, T: TargetEngine>(
    store: &PlatformStore,
    invocation: SyncInvocation,
    source: &S,
    target: &T,
    options: SyncOptions,
) -> Result<SyncCycleOutcome, RuntimeError> {
    let prepared =
        prepare_incremental_sync_cycle(store, invocation, options.normalized()).await?;
    let Some(mut cycle) = prepared else {
        return Ok(SyncCycleOutcome::Idle);
    };

    for deployment_idx in 0..cycle.deployments.len() {
        let deployment = cycle.deployments[deployment_idx].clone();
        let Some((mut deployment_pipelines, tables)) =
            pipelines_and_tables_for_deployment(&cycle, &deployment)
        else {
            continue;
        };

        sync_deployment_incremental(
            store,
            &mut cycle,
            &deployment,
            &mut deployment_pipelines,
            tables,
            source,
            target,
        )
        .await?;
    }

    finish_incremental_sync_cycle(cycle.quiet, cycle.progressed)
}

struct IncrementalSyncCycle {
    deployments: Vec<migraloop_platform_store::Deployment>,
    pipelines: Vec<Pipeline>,
    options: SyncOptions,
    quiet: bool,
    applied_this_run: u32,
    progressed: bool,
    /// Held for the cycle so continuous `run` + one-shot `sync` do not multi-write.
    _sync_lock: migraloop_platform_store::IncrementalSyncLock,
}

async fn prepare_incremental_sync_cycle(
    store: &PlatformStore,
    invocation: SyncInvocation,
    options: SyncOptions,
) -> Result<Option<IncrementalSyncCycle>, RuntimeError> {
    ensure_store_session_healthy(store).await?;

    // Serialize Incremental Capture writers: continuous `run` + one-shot `sync`
    // (Lab / catch-up) must not multi-write the same Deployment (ADR-0005).
    let sync_lock = store
        .acquire_incremental_sync_lock()
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    let deployments = store
        .list_deployments()
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    if deployments.is_empty() {
        return match invocation {
            SyncInvocation::OneShot => Err(RuntimeError::Failed(
                "no Deployments applied; run `migraloop apply` first".to_string(),
            )),
            // Compose / Lab Fixture may start `run` before any Deployment is applied.
            SyncInvocation::ContinuousCycle => Ok(None),
        };
    }

    let pipelines = store
        .list_pipelines()
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    Ok(Some(IncrementalSyncCycle {
        deployments,
        pipelines,
        options,
        quiet: matches!(invocation, SyncInvocation::ContinuousCycle),
        applied_this_run: 0,
        progressed: false,
        _sync_lock: sync_lock,
    }))
}

fn finish_incremental_sync_cycle(
    quiet: bool,
    progressed: bool,
) -> Result<SyncCycleOutcome, RuntimeError> {
    if !quiet {
        println!("Incremental Capture and Delivery complete");
    }
    Ok(if progressed {
        SyncCycleOutcome::Progressed
    } else {
        SyncCycleOutcome::Idle
    })
}

fn pipelines_and_tables_for_deployment(
    cycle: &IncrementalSyncCycle,
    deployment: &migraloop_platform_store::Deployment,
) -> Option<(Vec<Pipeline>, BTreeSet<(String, String)>)> {
    let deployment_pipelines: Vec<_> = cycle
        .pipelines
        .iter()
        .filter(|p| p.deployment_name == deployment.name)
        .cloned()
        .collect();
    if deployment_pipelines.is_empty() {
        return None;
    }
    let mut tables = BTreeSet::new();
    for pipeline in &deployment_pipelines {
        for (schema, table) in pipeline_base_table_refs(pipeline) {
            tables.insert((schema, table));
        }
    }
    Some((deployment_pipelines, tables))
}

/// One Deployment's Incremental Capture + Delivery against trait-bound engines.
async fn sync_deployment_incremental<S: SourceEngine, T: TargetEngine>(
    store: &PlatformStore,
    cycle: &mut IncrementalSyncCycle,
    deployment: &migraloop_platform_store::Deployment,
    deployment_pipelines: &mut Vec<Pipeline>,
    tables: BTreeSet<(String, String)>,
    source: &S,
    target: &T,
) -> Result<(), RuntimeError> {
        let fail_after = cycle.options.fail_after_changes;
        let max_poison_attempts = cycle.options.poison.max_attempts;
        let poison_identity_keys = cycle.options.poison.poison_identity_keys.clone();
        let queue_capacity = cycle.options.backpressure.queue_capacity;
        let delivery_delay_ms = cycle.options.backpressure.delivery_delay_ms;
        let quiet = cycle.quiet;
        // ADR-0021: fail-fast Source Prerequisites before Incremental Capture.
        // Source engine is provided by factory wiring or injection (issue #169).
        let source_tables: Vec<String> = tables.iter().map(|(_, t)| t.clone()).collect();
        source
            .check_prerequisites(&source_tables)
            .map_err(|err| RuntimeError::Failed(err.to_string()))?;
        let injected_schema_changes = source
            .schema_change_inputs()
            .map_err(|err| RuntimeError::Failed(err.to_string()))?;
        let capture = source
            .open_incremental_capture()
            .map_err(|err| RuntimeError::Failed(err.to_string()))?;
        if !quiet {
            println!(
                "Incremental Capture: mechanism={}",
                IncrementalCaptureSession::mechanism_label(&capture)
            );
        }

        // Resume from durable Platform Store checkpoint (inclusive SCN). Cutover
        // (`crate::cutover`) sets checkpoint = low-watermark-1 so the first Incremental
        // still covers the ADR-0004 overlap window. Inclusive resume plus change-id
        // dedupe keeps same-SCN siblings visible after a mid-SCN stop or bounded window
        // (issue #143). Prefer duplicates over gaps: Deliver each change before durable
        // Base/checkpoint/change-id persistence so a Delivery failure can retry.
        for (schema, table) in tables {
            let (dataset, base_rows) = store
                .get_base_rows(&table, Some(&deployment.name))
                .await
                .map_err(|err| RuntimeError::Failed(err.to_string()))?;

            let cutover = resume_for_incremental(
                &table,
                dataset.capture_low_watermark,
                dataset.capture_checkpoint,
            )?;
            let low_watermark = cutover.low_watermark;
            let mut resume_from = cutover.resume_from;
            let checkpoint_before = cutover.checkpoint_before;

            let supported_names: BTreeSet<String> =
                dataset.columns.iter().map(|c| c.name.clone()).collect();
            let source_columns = source
                .discover_schema(&schema, &table)
                .map_err(|err| RuntimeError::Failed(err.to_string()))?;
            let configured_tz = source_timezone_opt(deployment);
            let mut rows: Vec<serde_json::Map<String, serde_json::Value>> =
                base_rows.into_iter().map(|r| r.data).collect();
            let mut sync_applied = dataset.sync_applied_changes;

            for pipeline in deployment_pipelines.iter() {
                if pipeline.paused
                    && !pipeline.target_collection.is_empty()
                    && pipeline_references_table(pipeline, &table)
                {
                    println!(
                        "Pipeline {} paused — skipping Delivery/processing for {table}",
                        pipeline.name
                    );
                }
            }

            let mut windows_processed = 0usize;

            // ADR-0020: bounded Incremental windows. Capture only fills up to
            // queue_capacity; Downstream slowness drains slowly and backpressures
            // further fetch instead of buffering the full backlog in RAM.
            loop {
                // Already-applied ids at/after the inclusive resume SCN must be skipped
                // *before* the bounded window limit so same-SCN siblings are not starved
                // by re-fetched duplicates (issue #143).
                let applied_at_or_after = store
                    .list_applied_change_ids_from_position(
                        &deployment.name,
                        &schema,
                        &table,
                        resume_from.as_i64(),
                    )
                    .await
                    .map_err(|err| RuntimeError::Failed(err.to_string()))?;
                let applied_skip: BTreeSet<_> = applied_at_or_after.into_iter().collect();
                let fetch_limit = Some(queue_capacity.saturating_add(applied_skip.len()));

                // Count Source backlog without materializing row images so Sync/
                // Delivery Health lag can reflect delay under a bounded window.
                let source_pending_total = capture
                    .count_changes_in_schema(&schema, &table, resume_from)
                    .map_err(|err| RuntimeError::Failed(err.to_string()))?;
                let source_pending = source_pending_total.saturating_sub(applied_skip.len());
                let fetched_changes = capture
                    .fetch_changes_in_schema_limited(&schema, &table, resume_from, fetch_limit)
                    .map_err(|err| RuntimeError::Failed(err.to_string()))?;
                let candidate_changes: Vec<_> = fetched_changes
                    .into_iter()
                    .filter(|c| !applied_skip.contains(&c.change_id))
                    .take(queue_capacity)
                    .collect();
                let table_schema_changes: Vec<SchemaChangeEvent> = injected_schema_changes
                    .iter()
                    .filter(|c| c.table.eq_ignore_ascii_case(&table))
                    .filter(|c| c.position >= resume_from)
                    .cloned()
                    .collect();
                let mut candidate_ids: Vec<String> = candidate_changes
                    .iter()
                    .map(|c| c.change_id.clone())
                    .collect();
                candidate_ids.extend(table_schema_changes.iter().map(|c| c.change_id.clone()));
                let unapplied_ids = store
                    .filter_unapplied_change_ids(
                        &deployment.name,
                        &schema,
                        &table,
                        &candidate_ids,
                    )
                    .await
                    .map_err(|err| RuntimeError::Failed(err.to_string()))?;
                let unapplied_set: BTreeSet<_> = unapplied_ids.into_iter().collect();
                let schema_pending = table_schema_changes
                    .iter()
                    .filter(|c| unapplied_set.contains(&c.change_id))
                    .count();
                // Source count is from inclusive resume_from minus already-applied ids.
                // Window fetch may be smaller; lag uses full Source+schema pending.
                let pending_at_window_start = source_pending.saturating_add(schema_pending);
                let mut items: Vec<IncrementalItem> = candidate_changes
                    .into_iter()
                    .filter(|c| unapplied_set.contains(&c.change_id))
                    .map(IncrementalItem::Row)
                    .collect();
                items.extend(
                    table_schema_changes
                        .into_iter()
                        .filter(|c| unapplied_set.contains(&c.change_id))
                        .map(IncrementalItem::Schema),
                );
                // Stable sort by SCN only so same-SCN row order from LogMiner
                // (RS_ID/SSN) is preserved — do not re-order by change_id string
                // (op sorts before rs_id/ssn and can invert capture order).
                items.sort_by(|a, b| a.position().cmp(&b.position()));
                if items.len() > queue_capacity {
                    items.truncate(queue_capacity);
                }

                if items.is_empty() {
                    let status =
                        if windows_processed == 0 && dataset.status == "initial_load_complete" {
                            dataset.status.clone()
                        } else {
                            "incremental".to_string()
                        };
                    let caught_up = base_with_sync_progress(
                        &dataset,
                        status,
                        rows.len() as i32,
                        sync_applied,
                        if windows_processed == 0 {
                            dataset.capture_checkpoint
                        } else {
                            // Inclusive resume cursor sits on the drained SCN.
                            Some(resume_from.as_i64())
                        },
                        0,
                    );
                    store
                        .record_sync_window_progress(&caught_up, &rows, &[])
                        .await
                        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
                    set_delivery_lag_for_table(
                        store,
                        deployment_pipelines,
                        &table,
                        0,
                    )
                    .await?;
                    if !quiet {
                        if windows_processed == 0 {
                            println!(
                                "Incremental Capture: Base Dataset {table} resume from checkpoint — \
                                 0 new changes (already applied; lag=0)"
                            );
                        } else {
                            println!(
                                "Incremental Capture: Base Dataset {table} caught up (lag=0; \
                                 bounded queue capacity={queue_capacity})"
                            );
                        }
                    }
                    break;
                }

                let queue_depth = items.len();
                let reported_lag = pending_at_window_start as i32;
                // Backpressure seam (ADR-0020): full bounded window slows capture;
                // Downstream delay alone does not pause Pipelines.
                if window_under_backpressure(queue_depth, queue_capacity) {
                    emit_backpressure(
                        &table,
                        &deployment.name,
                        queue_depth,
                        queue_capacity,
                        reported_lag,
                    );
                }

                if !quiet {
                    if windows_processed == 0 {
                        println!(
                            "Incremental Capture: resuming Base Dataset {table} from \
                             checkpoint={checkpoint_before} (inclusive resume={resume_from}, \
                             queue_depth={queue_depth}, capacity={queue_capacity}, \
                             low-watermark={low_watermark})"
                        );
                    } else {
                        println!(
                            "Incremental Capture: Base Dataset {table} next bounded window \
                             resume={resume_from} queue_depth={queue_depth} \
                             capacity={queue_capacity}"
                        );
                    }
                }
                emit_event(
                    "incremental_capture",
                    &[
                        ("table", EventValue::from(table.as_str())),
                        ("queue_depth", EventValue::from(queue_depth)),
                        ("capacity", EventValue::from(queue_capacity)),
                        ("lag", EventValue::from(reported_lag)),
                        ("deployment", EventValue::from(deployment.name.as_str())),
                        ("resume_from", EventValue::from(resume_from.to_string())),
                    ],
                );

                for (index, item) in items.iter().enumerate() {
                    // Remaining Source+schema pending after this durable apply.
                    let lag = (pending_at_window_start as i32) - (index as i32 + 1);
                    let lag = lag.max(0);
                    match item {
                        IncrementalItem::Schema(schema_change) => {
                            apply_schema_change_impacts(
                                store,
                                &mut *deployment_pipelines,
                                &dataset,
                                &schema,
                                &table,
                                schema_change,
                            )
                            .await?;

                            let current_checkpoint = schema_change.position.as_i64();
                            let updated = base_with_sync_progress(
                                &dataset,
                                "incremental",
                                rows.len() as i32,
                                sync_applied,
                                Some(current_checkpoint),
                                lag,
                            );
                            store
                                .record_sync_window_progress(
                                    &updated,
                                    &rows,
                                    &[(
                                        schema_change.change_id.clone(),
                                        schema_change.position.as_i64(),
                                    )],
                                )
                                .await
                                .map_err(|err| RuntimeError::Failed(err.to_string()))?;
                            set_delivery_lag_for_table(
                                store,
                                deployment_pipelines,
                                &table,
                                lag,
                            )
                            .await?;
                            cycle.applied_this_run += 1;
                            println!(
                                "Incremental Capture: Base Dataset {table} applied schema change_id={} \
                                 checkpoint={current_checkpoint} lag={lag}",
                                schema_change.change_id
                            );
                        }
                        IncrementalItem::Row(change) => {
                            // Capture pre-apply Base row for Affect Analysis (unused-field skip / group keys).
                            let pre_apply = rows
                                .iter()
                                .find(|row| row_matches_identity(row, &change.identity))
                                .cloned();

                            if let Err(err) = apply_change_events_to_base_rows(
                                &mut rows,
                                std::slice::from_ref(change),
                                &supported_names,
                                &source_columns,
                                configured_tz,
                            ) {
                                // Durable failure signal for Observability Surface (#174).
                                // Keep backlog lag visible; do not advance checkpoint.
                                let mut failed = base_with_sync_progress(
                                    &dataset,
                                    dataset.status.clone(),
                                    rows.len() as i32,
                                    sync_applied,
                                    dataset.capture_checkpoint,
                                    lag.max(1),
                                );
                                failed.sync_health =
                                    crate::observability::sync_health_label_failed().to_string();
                                let _ = store
                                    .record_sync_window_progress(&failed, &rows, &[])
                                    .await;
                                return Err(err);
                            }

                            // Delivery before durable checkpoint so retries prefer duplicate applies.
                            // Collect next Maintenance State blobs and persist only after Base/checkpoint.
                            let mut pending_maintenance: Vec<(&Pipeline, MaintenanceStateBlob)> =
                                Vec::new();
                            for pipeline in deployment_pipelines.iter() {
                                if pipeline.target_collection.is_empty()
                                    || !pipeline_references_table(pipeline, &table)
                                {
                                    continue;
                                }
                                if pipeline.paused {
                                    // Skip Delivery/processing; Base Capture still advances for shared Bases.
                                    continue;
                                }

                                match pipeline.mode.as_str() {
                                    "direct" => {
                                        // Direct Pipelines only Deliver their primary source.table.
                                        if !pipeline.source_table.eq_ignore_ascii_case(&table) {
                                            continue;
                                        }
                                        match change.op {
                                            ChangeOp::Insert | ChangeOp::Update => {
                                                let Some(base_row) = rows.iter().find(|row| {
                                                    row_matches_identity(row, &change.identity)
                                                }) else {
                                                    return Err(RuntimeError::Failed(format!(
                                                    "Base Dataset {} missing row for Output Identity {:?}",
                                                    pipeline.source_table, change.identity
                                                )));
                                                };
                                                let document = delivery_document_for_row(
                                                    base_row,
                                                    &dataset.primary_key,
                                                    &dataset.columns,
                                                    pipeline,
                                                )?;
                                                match upsert_with_bounded_retries(
                                                    target,
                                                    &pipeline.target_collection,
                                                    &document,
                                                    max_poison_attempts,
                                                    &poison_identity_keys,
                                                    delivery_delay_ms,
                                                )
                                                .await
                                                {
                                                    Ok(upserted) => {
                                                        store
                                                            .record_delivery_progress(
                                                                &pipeline.deployment_name,
                                                                &pipeline.name,
                                                                Some("delivered"),
                                                                Some(upserted as i32),
                                                                Some(lag),
                                                            )
                                                            .await
                                                            .map_err(|err| {
                                                                RuntimeError::Failed(err.to_string())
                                                            })?;
                                                        println!(
                                                        "Delivery complete: Pipeline {} upserts={upserted} \
                                                         deletes=0 (checkpoint-bound)",
                                                        pipeline.name
                                                    );
                                                    }
                                                    Err((attempts, last_error)) => {
                                                        quarantine_poison_change(
                                                            store,
                                                            pipeline,
                                                            &schema,
                                                            &table,
                                                            change,
                                                            document.identity.clone(),
                                                            "delivery",
                                                            attempts,
                                                            &last_error,
                                                        )
                                                        .await?;
                                                        store
                                                            .record_delivery_progress(
                                                                &pipeline.deployment_name,
                                                                &pipeline.name,
                                                                None,
                                                                None,
                                                                Some(lag),
                                                            )
                                                            .await
                                                            .map_err(|err| {
                                                                RuntimeError::Failed(err.to_string())
                                                            })?;
                                                    }
                                                }
                                            }
                                            ChangeOp::Delete => {
                                                let identity = identity_value_from_change(
                                                    change,
                                                    &dataset.primary_key,
                                                )?;
                                                match delete_with_bounded_retries(
                                                    target,
                                                    &pipeline.target_collection,
                                                    &identity,
                                                    max_poison_attempts,
                                                    &poison_identity_keys,
                                                    delivery_delay_ms,
                                                )
                                                .await
                                                {
                                                    Ok(deleted) => {
                                                        store
                                                            .record_delivery_progress(
                                                                &pipeline.deployment_name,
                                                                &pipeline.name,
                                                                Some("delivered"),
                                                                Some(deleted as i32),
                                                                Some(lag),
                                                            )
                                                            .await
                                                            .map_err(|err| {
                                                                RuntimeError::Failed(err.to_string())
                                                            })?;
                                                        println!(
                                                        "Delivery complete: Pipeline {} upserts=0 \
                                                         deletes={deleted} (checkpoint-bound)",
                                                        pipeline.name
                                                    );
                                                    }
                                                    Err((attempts, last_error)) => {
                                                        quarantine_poison_change(
                                                            store,
                                                            pipeline,
                                                            &schema,
                                                            &table,
                                                            change,
                                                            identity,
                                                            "delivery",
                                                            attempts,
                                                            &last_error,
                                                        )
                                                        .await?;
                                                        store
                                                            .record_delivery_progress(
                                                                &pipeline.deployment_name,
                                                                &pipeline.name,
                                                                None,
                                                                None,
                                                                Some(lag),
                                                            )
                                                            .await
                                                            .map_err(|err| {
                                                                RuntimeError::Failed(err.to_string())
                                                            })?;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    "transform" => {
                                        let maintain = with_bounded_delivery_retries(
                                            max_poison_attempts,
                                            delivery_delay_ms,
                                            || async {
                                                maintain_transform_pipeline_for_change(
                                                    store,
                                                    pipeline,
                                                    target,
                                                    &table,
                                                    &rows,
                                                    change,
                                                    pre_apply.as_ref(),
                                                )
                                                .await
                                                .map_err(|err| err.to_string())
                                            },
                                        )
                                        .await;
                                        match maintain {
                                            Ok(Some(blob)) => {
                                                pending_maintenance.push((pipeline, blob));
                                            }
                                            Ok(None) => {}
                                            Err((attempts, last_error)) => {
                                                let identity = identity_value_from_change(
                                                    change,
                                                    if pipeline.output_identity.is_empty() {
                                                        &dataset.primary_key
                                                    } else {
                                                        &pipeline.output_identity
                                                    },
                                                )
                                                .unwrap_or_else(|_| {
                                                    serde_json::Value::Object(
                                                        change
                                                            .identity
                                                            .iter()
                                                            .map(|(k, v)| (k.clone(), v.clone()))
                                                            .collect(),
                                                    )
                                                });
                                                quarantine_poison_change(
                                                    store,
                                                    pipeline,
                                                    &schema,
                                                    &table,
                                                    change,
                                                    identity,
                                                    "delivery",
                                                    attempts,
                                                    &last_error,
                                                )
                                                .await?;
                                            }
                                        }
                                        store
                                            .record_delivery_progress(
                                                &pipeline.deployment_name,
                                                &pipeline.name,
                                                None,
                                                None,
                                                Some(lag),
                                            )
                                            .await
                                            .map_err(|err| RuntimeError::Failed(err.to_string()))?;
                                    }
                                    other => {
                                        return Err(RuntimeError::Failed(format!(
                                            "unsupported pipeline.mode {other:?} during Incremental Capture"
                                        )));
                                    }
                                }
                            }

                            sync_applied += 1;
                            let current_checkpoint = change.position.as_i64();
                            let updated = base_with_sync_progress(
                                &dataset,
                                "incremental",
                                rows.len() as i32,
                                sync_applied,
                                Some(current_checkpoint),
                                lag,
                            );

                            store
                                .record_sync_window_progress(
                                    &updated,
                                    &rows,
                                    &[(change.change_id.clone(), change.position.as_i64())],
                                )
                                .await
                                .map_err(|err| RuntimeError::Failed(err.to_string()))?;
                            // Durable Base/checkpoint first, then Maintenance State — matches
                            // Delivery-before-checkpoint: Sync retries re-analyze with prior state.
                            for (pipeline, blob) in &pending_maintenance {
                                persist_maintenance_state_blob(store, pipeline, blob).await?;
                            }
                            set_delivery_lag_for_table(
                                store,
                                deployment_pipelines,
                                &table,
                                lag,
                            )
                            .await?;

                            cycle.applied_this_run += 1;
                            println!(
                                "Incremental Capture: Base Dataset {table} applied change_id={} \
                                 checkpoint={current_checkpoint} lag={lag} rows={}",
                                change.change_id, updated.row_count
                            );
                        }
                    }

                    if let Some(limit) = fail_after {
                        if cycle.applied_this_run >= limit {
                            let current_checkpoint = item.position().as_i64();
                            return Err(RuntimeError::Failed(format!(
                                "simulated process kill after {limit} durable checkpoint(s) \
                                 (fail_after_changes / FAIL_AFTER); \
                                 resume from Platform Store checkpoint={current_checkpoint}"
                            )));
                        }
                    }
                }

                // Stay on the last applied SCN (inclusive). Already-applied change ids are
                // skipped on the next fetch so same-SCN siblings still drain; exclusive
                // SCN+1 advance would gap unapplied peers (issue #143).
                let last_pos = items.last().expect("non-empty window").position().as_i64();
                resume_from = CapturePosition::from_i64(last_pos).ok_or_else(|| {
                    RuntimeError::Failed(format!(
                        "invalid capture position advance for Base Dataset {table}: {last_pos}"
                    ))
                })?;
                windows_processed += 1;
                cycle.progressed = true;
            }
        }
    Ok(())
}
