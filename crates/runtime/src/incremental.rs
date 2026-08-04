//! Incremental Sync orchestration for the Deployment runtime (#153).
//!
//! Owns Incremental Capture (including continuous supervise/resume), cutover
//! overlap apply ordering, poison quarantine, schema-change pause, bounded
//! backpressure, and ChangeEvent → Base Dataset apply. The Operator CLI is a
//! thin adapter over [`sync_incremental`] / [`supervise_continuous_incremental_sync`].

use std::collections::{BTreeMap, BTreeSet};

use migraloop_capture::{
    classify_schema_impact, load_injected_schema_changes, normalize_change_temporals,
    CapturePosition, ChangeEvent, ChangeOp, PipelineSchemaDeps, SchemaChangeEvent, SchemaImpact,
    SourceColumn,
};
use migraloop_delivery::{
    delete_documents_by_identity, upsert_managed_documents, DeliveryDocument, ManagedFieldAs,
    MongoTargetConnection,
};
use migraloop_platform_store::{
    BaseColumn, BaseDataset, Pipeline, PlatformStore, QuarantinedChange, SchemaChangeImpact,
};
use migraloop_transform::{
    analyze_base_change, evaluate_transform_for_identities_with_bases, identity_matches_row,
    parse_transform_steps, secondary_base_refs, used_base_fields, AffectOutcome, BaseChangeContext,
    BaseChangeKind, MaintenanceStateBlob, TransformOp,
};

use crate::observability::{emit_event, EventValue};
use crate::{
    delivery_document_for_row, derived_columns_for_ops, ensure_oracle_source_prerequisites,
    ensure_store_session_healthy, load_secondary_bases_and_columns_for_pipeline,
    mongo_target_from_deployment, open_deployment_incremental_capture, output_identity_from_row,
    persist_maintenance_state_blob, pipeline_base_table_refs, pipeline_references_table,
    source_columns_for_pipeline, source_timezone_opt, transform_ops_from_pipeline, RuntimeError,
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
async fn maintain_transform_pipeline_for_change(
    store: &PlatformStore,
    pipeline: &Pipeline,
    mongo: &MongoTargetConnection,
    changed_table: &str,
    changed_base_rows: &[serde_json::Map<String, serde_json::Value>],
    change: &ChangeEvent,
    pre_apply: Option<&serde_json::Map<String, serde_json::Value>>,
) -> Result<(), RuntimeError> {
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
            if let Some(next) = &analysis.next_maintenance_state {
                persist_maintenance_state_blob(store, pipeline, next).await?;
            }
            println!(
                "Affect Analysis: Pipeline {} skipped (unused fields only)",
                pipeline.name
            );
            Ok(())
        }
        AffectOutcome::SkipValueUnchanged => {
            if let Some(next) = &analysis.next_maintenance_state {
                persist_maintenance_state_blob(store, pipeline, next).await?;
            }
            println!(
                "Affect Analysis: Pipeline {} skipped (value-level; no Derived change)",
                pipeline.name
            );
            Ok(())
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
                mongo,
                &primary_columns,
                &secondary_columns,
                &primary_rows,
                &secondary,
                &ops,
                &identities,
            )
            .await?;
            // Persist next Maintenance State only after successful Derived/Delivery work
            // so a Delivery retry cannot analyze/bump the same change twice.
            if let Some(next) = &analysis.next_maintenance_state {
                persist_maintenance_state_blob(store, pipeline, next).await?;
            }
            Ok(())
        }
    }
}

async fn recompute_and_deliver_affected_identities(
    store: &PlatformStore,
    pipeline: &Pipeline,
    mongo: &MongoTargetConnection,
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
        delivered += upsert_managed_documents(mongo, &pipeline.target_collection, &upserts)
            .await
            .map_err(|err| RuntimeError::Failed(err.to_string()))? as i32;
    }
    if !deletes.is_empty() {
        delivered += delete_documents_by_identity(mongo, &pipeline.target_collection, &deletes)
            .await
            .map_err(|err| RuntimeError::Failed(err.to_string()))? as i32;
    }

    if delivered > 0 {
        store
            .update_pipeline_delivery_progress(
                &pipeline.deployment_name,
                &pipeline.name,
                "delivered",
                Some(delivered),
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

pub fn apply_change_events_to_base_rows(
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

/// Test-only fault injection for restart-resume coverage (ADR-0011).
/// When set, sync exits after N durable checkpoints to simulate mid-incremental process kill.
fn sync_fail_after_changes() -> Option<u32> {
    std::env::var("MIGRALOOP_SYNC_FAIL_AFTER_CHANGES")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
}

/// Bounded Delivery retries before Poison Change quarantine (ADR-0015 / issue #22).
fn poison_max_attempts() -> u32 {
    std::env::var("MIGRALOOP_POISON_MAX_ATTEMPTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(3)
}

/// Test/Lab fault injection: comma-separated Output Identity keys that always fail Delivery.
fn delivery_poison_identity_keys() -> BTreeSet<String> {
    std::env::var("MIGRALOOP_DELIVERY_POISON_IDENTITIES")
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Bounded Incremental Capture / Delivery queue capacity (ADR-0020 / issue #26).
///
/// Stages never materialize more than this many pending changes at once; capture
/// slows when Downstream cannot drain the window. Override via
/// `MIGRALOOP_SYNC_QUEUE_CAPACITY` (must be > 0). Default 256.
fn sync_queue_capacity() -> usize {
    std::env::var("MIGRALOOP_SYNC_QUEUE_CAPACITY")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &usize| *n > 0)
        .unwrap_or(256)
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

/// Test/Lab fault injection: artificial Downstream Delivery slowness (milliseconds).
fn delivery_delay_ms() -> Option<u64> {
    std::env::var("MIGRALOOP_DELIVERY_DELAY_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|n: &u64| *n > 0)
}

async fn apply_delivery_delay() {
    if let Some(ms) = delivery_delay_ms() {
        tokio::time::sleep(std::time::Duration::from_millis(ms)).await;
    }
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
            .update_pipeline_delivery_lag(
                &pipeline.deployment_name,
                &pipeline.name,
                delivery_lag,
            )
            .await
            .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    }
    Ok(())
}

pub fn format_output_identity(identity: &serde_json::Value) -> String {
    match identity {
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn identity_is_poison(identity: &serde_json::Value, poison_keys: &BTreeSet<String>) -> bool {
    if poison_keys.is_empty() {
        return false;
    }
    poison_keys.contains(&format_output_identity(identity))
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

async fn upsert_with_bounded_retries(
    mongo: &MongoTargetConnection,
    collection: &str,
    document: &DeliveryDocument,
    max_attempts: u32,
) -> Result<usize, (u32, String)> {
    let poison = delivery_poison_identity_keys();
    let mut last_error = String::new();
    for attempt in 1..=max_attempts {
        apply_delivery_delay().await;
        if identity_is_poison(&document.identity, &poison) {
            last_error = format!(
                "injected poison Delivery failure for Output Identity {}",
                format_output_identity(&document.identity)
            );
        } else {
            match upsert_managed_documents(mongo, collection, std::slice::from_ref(document)).await
            {
                Ok(n) => return Ok(n),
                Err(err) => last_error = err.to_string(),
            }
        }
        if attempt < max_attempts {
            eprintln!("Delivery retry {attempt}/{max_attempts} failed: {last_error}");
        }
    }
    Err((max_attempts, last_error))
}

async fn delete_with_bounded_retries(
    mongo: &MongoTargetConnection,
    collection: &str,
    identity: &serde_json::Value,
    max_attempts: u32,
) -> Result<usize, (u32, String)> {
    let poison = delivery_poison_identity_keys();
    let mut last_error = String::new();
    for attempt in 1..=max_attempts {
        apply_delivery_delay().await;
        if identity_is_poison(identity, &poison) {
            last_error = format!(
                "injected poison Delivery failure for Output Identity {}",
                format_output_identity(identity)
            );
        } else {
            match delete_documents_by_identity(mongo, collection, std::slice::from_ref(identity))
                .await
            {
                Ok(n) => return Ok(n),
                Err(err) => last_error = err.to_string(),
            }
        }
        if attempt < max_attempts {
            eprintln!("Delivery retry {attempt}/{max_attempts} failed: {last_error}");
        }
    }
    Err((max_attempts, last_error))
}

async fn quarantine_poison_change(
    store: &PlatformStore,
    pipeline: &Pipeline,
    schema: &str,
    table: &str,
    change: &ChangeEvent,
    output_identity: serde_json::Value,
    stage: &str,
    attempts: u32,
    last_error: &str,
) -> Result<(), RuntimeError> {
    let record = QuarantinedChange {
        deployment_name: pipeline.deployment_name.clone(),
        pipeline_name: pipeline.name.clone(),
        source_schema: schema.to_string(),
        source_table: table.to_string(),
        change_id: change.change_id.clone(),
        capture_position: change.position.as_i64(),
        output_identity,
        stage: stage.to_string(),
        attempts: attempts as i32,
        last_error: last_error.to_string(),
        status: "quarantined".to_string(),
    };
    let identity_label = format_output_identity(&record.output_identity);
    store
        .upsert_quarantined_change(&record)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    eprintln!(
        "ALERT: Poison Change quarantined Pipeline={} identity={} change_id={} \
         stage={stage} attempts={attempts}: {last_error}",
        pipeline.name, identity_label, change.change_id
    );
    println!(
        "Quarantine: Pipeline={} identity={} change_id={} stage={stage} \
         attempts={attempts} unhealthy / not aligned",
        pipeline.name, identity_label, change.change_id
    );
    emit_event(
        "poison_quarantine",
        &[
            ("level", EventValue::from("alert")),
            ("pipeline", EventValue::from(pipeline.name.as_str())),
            ("identity", EventValue::from(identity_label.as_str())),
            ("change_id", EventValue::from(change.change_id.as_str())),
            ("stage", EventValue::from(stage)),
            ("attempts", EventValue::from(attempts as i64)),
        ],
    );
    Ok(())
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

/// Dependency columns for Schema Change impact classification.
fn pipeline_schema_deps(pipeline: &Pipeline, dataset: &BaseDataset) -> PipelineSchemaDeps {
    let mut dependency_columns: BTreeSet<String> = dataset.primary_key.iter().cloned().collect();
    let is_primary = dataset
        .source_table
        .eq_ignore_ascii_case(&pipeline.source_table);
    match pipeline.mode.as_str() {
        "direct" => {
            for col in &dataset.columns {
                if pipeline.field_mappings.get(&col.name) == Some(&ManagedFieldAs::Omit) {
                    continue;
                }
                dependency_columns.insert(col.name.clone());
            }
        }
        "transform" => {
            if is_primary {
                if let Some(transform) = &pipeline.transform_json {
                    if let Some(steps) = transform.as_array() {
                        if let Ok(ops) = parse_transform_steps(steps) {
                            dependency_columns.extend(used_base_fields(&ops));
                        }
                    }
                }
                for field in &pipeline.output_identity {
                    dependency_columns.insert(field.clone());
                }
            } else if let Ok(ops) = transform_ops_from_pipeline(pipeline) {
                if let Some(suffix) = union_suffix_ops_for_table(&ops, &dataset.source_table) {
                    // union.from rows are shaped only by steps after the union —
                    // Schema Change deps match Affect Analysis used fields.
                    let used = used_base_fields(suffix);
                    if used.is_empty() {
                        for col in &dataset.columns {
                            dependency_columns.insert(col.name.clone());
                        }
                    } else {
                        dependency_columns.extend(used);
                    }
                    for field in &pipeline.output_identity {
                        dependency_columns.insert(field.clone());
                    }
                } else {
                    // equiLookup embeds full foreign rows — any column drop/type change blocks.
                    for col in &dataset.columns {
                        dependency_columns.insert(col.name.clone());
                    }
                }
            } else {
                for col in &dataset.columns {
                    dependency_columns.insert(col.name.clone());
                }
            }
        }
        _ => {
            for col in &dataset.columns {
                dependency_columns.insert(col.name.clone());
            }
        }
    }
    PipelineSchemaDeps {
        source_table: dataset.source_table.clone(),
        source_schema: dataset.source_schema.clone(),
        dependency_columns,
    }
}

/// Operators after the `union.from` step for `table` (secondary contribution shape).
fn union_suffix_ops_for_table<'a>(
    ops: &'a [TransformOp],
    table: &str,
) -> Option<&'a [TransformOp]> {
    let idx = ops.iter().position(|op| match op {
        TransformOp::Union { from, .. } => from.eq_ignore_ascii_case(table),
        _ => false,
    })?;
    Some(&ops[idx + 1..])
}

/// Classify Schema Change impact for Pipelines on this table; warn+pause on Blocking.
async fn apply_schema_change_impacts(
    store: &PlatformStore,
    deployment_pipelines: &mut [Pipeline],
    dataset: &BaseDataset,
    schema: &str,
    table: &str,
    change: &SchemaChangeEvent,
) -> Result<(), RuntimeError> {
    for pipeline in deployment_pipelines.iter_mut() {
        if !pipeline_references_table(pipeline, table) {
            continue;
        }
        // Schema must match the referenced Base (primary schema or equiLookup/union fromSchema).
        let refs = pipeline_base_table_refs(pipeline);
        let schema_ok = refs.iter().any(|(ref_schema, ref_table)| {
            ref_table.eq_ignore_ascii_case(table)
                && (ref_schema.is_empty()
                    || schema.is_empty()
                    || ref_schema.eq_ignore_ascii_case(schema))
        });
        if !schema_ok {
            continue;
        }
        let deps = pipeline_schema_deps(pipeline, dataset);
        let impact = classify_schema_impact(&deps, change);
        match impact {
            SchemaImpact::Blocking => {
                if !pipeline.paused {
                    store
                        .set_pipeline_paused(
                            &pipeline.deployment_name,
                            &pipeline.name,
                            true,
                        )
                        .await
                        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
                    pipeline.paused = true;
                }
                let record = SchemaChangeImpact {
                    deployment_name: pipeline.deployment_name.clone(),
                    pipeline_name: pipeline.name.clone(),
                    source_schema: schema.to_string(),
                    source_table: table.to_string(),
                    change_id: change.change_id.clone(),
                    capture_position: change.position.as_i64(),
                    ddl_summary: change.summary.clone(),
                    impact: impact.as_str().to_string(),
                    status: "active".to_string(),
                };
                store
                    .upsert_schema_change_impact(&record)
                    .await
                    .map_err(|err| RuntimeError::Failed(err.to_string()))?;
                eprintln!(
                    "WARN: Schema Change blocked Pipeline={} change_id={} ddl={} — \
                     pausing affected Pipeline (not poison quarantine)",
                    pipeline.name, change.change_id, change.summary
                );
                println!(
                    "Schema Change: Pipeline={} impact=blocking change_id={} ddl={} paused",
                    pipeline.name, change.change_id, change.summary
                );
                emit_event(
                    "schema_change_blocked",
                    &[
                        ("level", EventValue::from("warn")),
                        ("pipeline", EventValue::from(pipeline.name.as_str())),
                        ("change_id", EventValue::from(change.change_id.as_str())),
                        ("ddl", EventValue::from(change.summary.as_str())),
                        ("impact", EventValue::from("blocking")),
                    ],
                );
            }
            SchemaImpact::NonBlocking => {
                println!(
                    "Schema Change: Pipeline={} impact=non_blocking change_id={} ddl={} — \
                     continue (safe apply)",
                    pipeline.name, change.change_id, change.summary
                );
            }
            SchemaImpact::Unaffecting => {
                println!(
                    "Schema Change: Pipeline={} impact=unaffecting change_id={} ddl={} — \
                     continue",
                    pipeline.name, change.change_id, change.summary
                );
            }
        }
    }
    Ok(())
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
        sync_health: "ok".to_string(),
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
pub async fn sync_incremental(store: &PlatformStore) -> Result<(), RuntimeError> {
    run_incremental_sync(store, SyncInvocation::OneShot)
        .await
        .map(|_| ())
}

/// Continuous Incremental Capture → Affect Analysis → Delivery inside `migraloop run`.
///
/// Idle-polls only when caught up or when no Deployments are applied yet so compose
/// can start before `apply`. While there is pending Source work, cycles continue
/// immediately (bounded windows still apply). Errors are logged and retried —
/// Observability metrics keep serving on the same single active instance (issue #145).
pub async fn run_continuous_incremental_sync(platform_store_url: String) {
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
        let cycle = async {
            let store = PlatformStore::open(&platform_store_url)
                .await
                .map_err(|err| RuntimeError::Failed(err.to_string()))?;
            run_incremental_sync(&store, SyncInvocation::ContinuousCycle).await
        };
        match cycle.await {
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

/// Supervise continuous Sync so a panic does not leave `/metrics` up with Sync dead.
pub async fn supervise_continuous_incremental_sync(platform_store_url: String) {
    loop {
        let url = platform_store_url.clone();
        match tokio::spawn(async move { run_continuous_incremental_sync(url).await }).await {
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
pub async fn run_incremental_sync(
    store: &PlatformStore,
    invocation: SyncInvocation,
) -> Result<SyncCycleOutcome, RuntimeError> {
    ensure_store_session_healthy(store).await?;

    // Serialize Incremental Capture writers: continuous `run` + one-shot `sync`
    // (Lab / catch-up) must not multi-write the same Deployment (ADR-0005).
    let _sync_lock = store
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
            SyncInvocation::ContinuousCycle => Ok(SyncCycleOutcome::Idle),
        };
    }

    let pipelines = store
        .list_pipelines()
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    let fail_after = sync_fail_after_changes();
    let max_poison_attempts = poison_max_attempts();
    let queue_capacity = sync_queue_capacity();
    let downstream_delay = delivery_delay_ms().is_some();
    let injected_schema_changes =
        load_injected_schema_changes().map_err(|err| RuntimeError::Failed(err.to_string()))?;
    let mut applied_this_run: u32 = 0;
    let mut progressed = false;
    let quiet = matches!(invocation, SyncInvocation::ContinuousCycle);

    for deployment in &deployments {
        let mut deployment_pipelines: Vec<_> = pipelines
            .iter()
            .filter(|p| p.deployment_name == deployment.name)
            .cloned()
            .collect();
        if deployment_pipelines.is_empty() {
            continue;
        }

        let mut tables = BTreeSet::new();
        for pipeline in &deployment_pipelines {
            for (schema, table) in pipeline_base_table_refs(pipeline) {
                tables.insert((schema, table));
            }
        }

        // ADR-0021: fail-fast Source Prerequisites before Incremental Capture.
        // LogMiner-backed capture (contract or OCI) is opened once per Deployment.
        let capture = if deployment.source.kind.eq_ignore_ascii_case("oracle") {
            let source_tables: Vec<String> = tables.iter().map(|(_, t)| t.clone()).collect();
            ensure_oracle_source_prerequisites(&deployment.source, &source_tables)?;
            Some(open_deployment_incremental_capture(&deployment.source)?)
        } else {
            None
        };
        if let Some(ref capture) = capture {
            if !quiet {
                println!(
                    "Incremental Capture: mechanism={}",
                    capture.mechanism_label()
                );
            }
        }

        // Resume from durable Platform Store checkpoint (inclusive SCN). Initial Load sets
        // checkpoint = low-watermark-1 so the first Incremental still covers the ADR-0004
        // overlap window. Inclusive resume plus change-id dedupe keeps same-SCN siblings
        // visible after a mid-SCN stop or bounded window (issue #143). Prefer duplicates
        // over gaps: Deliver each change before durable Base/checkpoint/change-id
        // persistence so a Delivery failure can retry.
        let mongo = mongo_target_from_deployment(deployment)?;

        for (schema, table) in tables {
            let (dataset, base_rows) = store
                .get_base_rows(&table, Some(&deployment.name))
                .await
                .map_err(|err| RuntimeError::Failed(err.to_string()))?;

            let Some(low_watermark_i64) = dataset.capture_low_watermark else {
                return Err(RuntimeError::Failed(format!(
                    "cannot start Incremental Capture for {table} without low-watermark overlap \
                     (cutover watermark missing; re-run Initial Load via `migraloop apply`)"
                )));
            };
            let low_watermark = CapturePosition::from_i64(low_watermark_i64).ok_or_else(|| {
                RuntimeError::Failed(format!(
                    "invalid low-watermark for Base Dataset {table}: {low_watermark_i64}"
                ))
            })?;

            let mut resume_from = match dataset.capture_checkpoint {
                Some(cp) => CapturePosition::from_i64(cp).ok_or_else(|| {
                    RuntimeError::Failed(format!(
                        "invalid capture checkpoint for Base Dataset {table}: {cp}"
                    ))
                })?,
                None => low_watermark,
            };

            let Some(capture) = &capture else {
                return Err(RuntimeError::Failed(format!(
                    "Incremental Capture requires an Oracle Source System (LogMiner); \
                     got kind={}",
                    deployment.source.kind
                )));
            };

            let supported_names: BTreeSet<String> =
                dataset.columns.iter().map(|c| c.name.clone()).collect();
            let source_columns = source_columns_for_pipeline(deployment, &schema, &table)?;
            let configured_tz = source_timezone_opt(deployment);
            let mut rows: Vec<serde_json::Map<String, serde_json::Value>> =
                base_rows.into_iter().map(|r| r.data).collect();
            let mut sync_applied = dataset.sync_applied_changes;

            for pipeline in &deployment_pipelines {
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

            let checkpoint_before = dataset
                .capture_checkpoint
                .unwrap_or(low_watermark_i64.saturating_sub(1));
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
                    store.replace_base_dataset(&caught_up, &rows)
                        .await
                        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
                    set_delivery_lag_for_table(
                        store,
                        &deployment_pipelines,
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
                let fetched_full_window = queue_depth >= queue_capacity;
                let reported_lag = pending_at_window_start as i32;
                // Backpressure is the bounded window under Downstream delay or a
                // full queue (capture cannot pull more until the window drains).
                if (downstream_delay && fetched_full_window) || fetched_full_window {
                    println!(
                        "Backpressure: queue_depth={queue_depth} capacity={queue_capacity} \
                         lag={reported_lag}"
                    );
                    emit_event(
                        "backpressure",
                        &[
                            ("table", EventValue::from(table.as_str())),
                            ("queue_depth", EventValue::from(queue_depth)),
                            ("capacity", EventValue::from(queue_capacity)),
                            ("lag", EventValue::from(reported_lag)),
                            ("deployment", EventValue::from(deployment.name.as_str())),
                        ],
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
                                &mut deployment_pipelines,
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
                            store.replace_base_dataset(&updated, &rows)
                                .await
                                .map_err(|err| RuntimeError::Failed(err.to_string()))?;
                            store.record_applied_source_changes(
                                                                &deployment.name,
                                &schema,
                                &table,
                                &[(
                                    schema_change.change_id.clone(),
                                    schema_change.position.as_i64(),
                                )],
                            )
                            .await
                            .map_err(|err| RuntimeError::Failed(err.to_string()))?;
                            set_delivery_lag_for_table(
                                store,
                                &deployment_pipelines,
                                &table,
                                lag,
                            )
                            .await?;
                            applied_this_run += 1;
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

                            apply_change_events_to_base_rows(
                                &mut rows,
                                std::slice::from_ref(change),
                                &supported_names,
                                &source_columns,
                                configured_tz,
                            )?;

                            // Delivery before durable checkpoint so retries prefer duplicate applies.
                            for pipeline in &deployment_pipelines {
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
                                                    &mongo,
                                                    &pipeline.target_collection,
                                                    &document,
                                                    max_poison_attempts,
                                                )
                                                .await
                                                {
                                                    Ok(upserted) => {
                                                        store.update_pipeline_delivery_progress_with_lag(
                                                                                                                        &pipeline.deployment_name,
                                                            &pipeline.name,
                                                            "delivered",
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
                                                        store.update_pipeline_delivery_lag(
                                                                                                                        &pipeline.deployment_name,
                                                            &pipeline.name,
                                                            lag,
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
                                                    &mongo,
                                                    &pipeline.target_collection,
                                                    &identity,
                                                    max_poison_attempts,
                                                )
                                                .await
                                                {
                                                    Ok(deleted) => {
                                                        store.update_pipeline_delivery_progress_with_lag(
                                                                                                                        &pipeline.deployment_name,
                                                            &pipeline.name,
                                                            "delivered",
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
                                                        store.update_pipeline_delivery_lag(
                                                                                                                        &pipeline.deployment_name,
                                                            &pipeline.name,
                                                            lag,
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
                                        let mut last_error = String::new();
                                        let mut succeeded = false;
                                        for attempt in 1..=max_poison_attempts {
                                            apply_delivery_delay().await;
                                            match maintain_transform_pipeline_for_change(
                                                store,
                                                pipeline,
                                                &mongo,
                                                &table,
                                                &rows,
                                                change,
                                                pre_apply.as_ref(),
                                            )
                                            .await
                                            {
                                                Ok(()) => {
                                                    succeeded = true;
                                                    break;
                                                }
                                                Err(err) => {
                                                    last_error = err.to_string();
                                                    if attempt < max_poison_attempts {
                                                        eprintln!(
                                                            "Delivery retry {attempt}/{max_poison_attempts} \
                                                             failed: {last_error}"
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                        if !succeeded {
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
                                                max_poison_attempts,
                                                &last_error,
                                            )
                                            .await?;
                                        }
                                        store.update_pipeline_delivery_lag(
                                                                                                        &pipeline.deployment_name,
                                            &pipeline.name,
                                            lag,
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

                            store.replace_base_dataset(&updated, &rows)
                                .await
                                .map_err(|err| RuntimeError::Failed(err.to_string()))?;
                            store.record_applied_source_changes(
                                                                &deployment.name,
                                &schema,
                                &table,
                                &[(change.change_id.clone(), change.position.as_i64())],
                            )
                            .await
                            .map_err(|err| RuntimeError::Failed(err.to_string()))?;
                            set_delivery_lag_for_table(
                                store,
                                &deployment_pipelines,
                                &table,
                                lag,
                            )
                            .await?;

                            applied_this_run += 1;
                            println!(
                                "Incremental Capture: Base Dataset {table} applied change_id={} \
                                 checkpoint={current_checkpoint} lag={lag} rows={}",
                                change.change_id, updated.row_count
                            );
                        }
                    }

                    if let Some(limit) = fail_after {
                        if applied_this_run >= limit {
                            let current_checkpoint = item.position().as_i64();
                            return Err(RuntimeError::Failed(format!(
                                "simulated process kill after {limit} durable checkpoint(s) \
                                 (MIGRALOOP_SYNC_FAIL_AFTER_CHANGES); resume from Platform Store \
                                 checkpoint={current_checkpoint}"
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
                progressed = true;
            }
        }
    }

    if !quiet {
        println!("Incremental Capture and Delivery complete");
    }
    Ok(if progressed {
        SyncCycleOutcome::Progressed
    } else {
        SyncCycleOutcome::Idle
    })
}
