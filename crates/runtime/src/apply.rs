//! Apply / table-level Initial Load / Delivery start for the Deployment runtime (#208).
//!
//! Owns Deployment apply (including Pipeline add / revision Change), table-level
//! Initial Load (chunking, throttle, pause/resume progress), and Direct/Transform
//! Delivery start used by apply and lifecycle resume. Incremental Sync lives in
//! [`crate::incremental`]. Cutover watermark / hand-off facts live in
//! [`crate::cutover`] (ADR-0004). Callers keep learning Operator Deployment verbs
//! (`apply` / `apply_with_options` / `apply_with_engines`) — no parallel orchestration seam.

use std::collections::BTreeSet;

use migraloop_capture::{
    CapturePosition, InitialLoadChunkOptions, SourceColumn, SourceEngine,
};
use migraloop_delivery::{ManagedFieldAs, TargetEngine};
use migraloop_platform_store::{
    BaseColumn, BaseDataset, Deployment, DerivedDataset, OmittedColumn, Pipeline, PlatformStore,
};
use migraloop_transform::{
    evaluate_transform_with_bases, infer_derived_columns, initial_maintenance_state,
    MaintenanceStateBlob, TransformOp,
};
use migraloop_types::output_identity_key;

use crate::apply_options::ApplyOptions;
use crate::cutover::{handoff_from_low_watermark, handoff_from_optional_low_watermark};
use crate::observability::{emit_event, EventValue};
use crate::{
    delivery_document_for_row, ensure_source_prerequisites, ensure_store_session_healthy,
    load_secondary_bases_and_columns_for_pipeline, pipeline_base_table_refs, pipeline_has_target,
    pipeline_references_table, source_columns_for_pipeline, source_engine_from_connection,
    source_timezone_opt, target_engine_from_deployment, transform_ops_from_pipeline,
    validate_pipeline_managed_fields, RuntimeError,
};

fn base_columns_from_source(columns: &[&SourceColumn]) -> Vec<BaseColumn> {
    columns.iter().map(|c| c.column_shape()).collect()
}

async fn sync_base_datasets_for_pipelines<S: SourceEngine>(
    store: &PlatformStore,
    deployment: &Deployment,
    pipelines: &[Pipeline],
    options: &ApplyOptions,
    source: &S,
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
                ensure_base_primary_key(
                    store,
                    deployment,
                    &schema,
                    &table,
                    configured_tz,
                    source,
                )
                .await?;
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
            options,
            source,
        )
        .await?;
    }

    Ok(())
}

async fn run_chunked_initial_load<S: SourceEngine>(
    store: &PlatformStore,
    deployment: &Deployment,
    pipelines: &[Pipeline],
    schema: &str,
    table: &str,
    configured_tz: Option<&str>,
    existing: Option<&BaseDataset>,
    options: &ApplyOptions,
    source: &S,
) -> Result<(), RuntimeError> {
    let deployment_name = &deployment.name;
    let chunk_size = options.initial_load.chunk_size;
    let rate_limit = options.initial_load.rows_per_sec;
    let pause_after = options.initial_load.pause_after_chunks;
    let store_delay = options.initial_load.store_delay_ms;

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
                        data_type: shape.data_type,
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

async fn ensure_base_primary_key<S: SourceEngine>(
    store: &PlatformStore,
    deployment: &Deployment,
    source_schema: &str,
    source_table: &str,
    configured_timezone: Option<&str>,
    source: &S,
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

/// Deliver Pipelines. `reconcile_deletes` removes Target identities that disappeared
/// (used for revision rebuild and resume catch-up). When `ignore_paused` is true,
/// Delivery runs even if the Pipeline is still marked paused (revision transition).
pub(crate) async fn deliver_pipelines_with_options<S: SourceEngine, T: TargetEngine>(
    store: &PlatformStore,
    deployment: &Deployment,
    pipelines: &[&Pipeline],
    reconcile_deletes: bool,
    ignore_paused: bool,
    source: &S,
    target: &T,
) -> Result<(), RuntimeError> {
    let needs_delivery = pipelines
        .iter()
        .any(|p| pipeline_has_target(p) && (ignore_paused || !p.paused));
    if !needs_delivery {
        return Ok(());
    }

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
                    source,
                    target,
                    reconcile_deletes,
                )
                .await?;
            }
            "transform" => {
                deliver_transform_pipeline_with_options(
                    store,
                    deployment,
                    pipeline,
                    source,
                    target,
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
pub(crate) async fn deliver_direct_pipeline_with_options<S: SourceEngine, T: TargetEngine>(
    store: &PlatformStore,
    deployment: &Deployment,
    pipeline: &Pipeline,
    source: &S,
    target: &T,
    reconcile_deletes: bool,
) -> Result<(), RuntimeError> {
    let (dataset, rows) = store
        .get_base_rows(&pipeline.source_table, Some(&pipeline.deployment_name))
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    let source_columns =
        source_columns_for_pipeline(source, &pipeline.source_schema, &pipeline.source_table)?;
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

pub(crate) async fn deliver_transform_pipeline_with_options<S: SourceEngine, T: TargetEngine>(
    store: &PlatformStore,
    deployment: &Deployment,
    pipeline: &Pipeline,
    source: &S,
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
        source_columns_for_pipeline(source, &pipeline.source_schema, &pipeline.source_table)?;
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

/// Derived columns via the transform schema-inference interface (no TransformOp walk here).
pub(crate) fn derived_columns_for_ops(
    base_columns: &[BaseColumn],
    ops: &[TransformOp],
    derived_rows: &[serde_json::Map<String, serde_json::Value>],
    secondary_columns: &[BaseColumn],
) -> Vec<BaseColumn> {
    // OutputColumn / BaseColumn are both ColumnShape after #182 — no remap.
    infer_derived_columns(ops, base_columns, secondary_columns, derived_rows)
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
/// Thin wrapper that builds [`ApplyOptions`] from the temporary env compat shim.
/// Prefer [`apply_with_options`] with typed options for Lab / RQG / in-process
/// tests (#200).
pub async fn apply(
    store: &PlatformStore,
    deployment: Deployment,
    pipelines: Vec<Pipeline>,
) -> Result<(), RuntimeError> {
    apply_with_options(store, deployment, pipelines, ApplyOptions::from_env_compat()).await
}

/// Apply a Deployment with typed [`ApplyOptions`] (issue #200).
///
/// Factory path: constructs v1 Source/Target adapters via
/// [`source_engine_from_connection`] / [`target_engine_from_deployment`]. Kind
/// selection stays inside those factories (#206). Prefer
/// [`apply_with_engines`] for Fake / pre-built adapters.
pub async fn apply_with_options(
    store: &PlatformStore,
    deployment: Deployment,
    pipelines: Vec<Pipeline>,
    options: ApplyOptions,
) -> Result<(), RuntimeError> {
    let source = source_engine_from_connection(&deployment.source)?;
    let target = target_engine_from_deployment(&deployment)?;
    apply_with_engines(store, deployment, pipelines, options, &source, &target).await
}

/// Apply a Deployment with caller-injected Source/Target engines (issue #206).
///
/// Seam for Fake Source/Target (and any pre-built adapters): skips factory kind
/// selection — Initial Load, Managed validation, prerequisites, and Delivery use
/// the injected engines. Default CLI `apply` keeps using [`apply_with_options`]
/// with no Operator-visible change.
pub async fn apply_with_engines<S: SourceEngine, T: TargetEngine>(
    store: &PlatformStore,
    deployment: Deployment,
    mut pipelines: Vec<Pipeline>,
    options: ApplyOptions,
    source: &S,
    target: &T,
) -> Result<(), RuntimeError> {
    ensure_store_session_healthy(store).await?;

    // ADR-0021: fail-fast Source Prerequisites before discovery / Initial Load.
    // Deployment-only apply (no Pipeline tables) does not open capture yet.
    // Capability comes from the engine (factory or injected) — no kind gate (#206).
    let source_tables = pipeline_source_tables(&pipelines);
    if !source_tables.is_empty() {
        ensure_source_prerequisites(source, &source_tables)?;
    }

    // Apply-time Managed validation before Initial Load / Delivery so unsafe NUMBER
    // and unsupported Managed inputs fail configure-time (ADR-0018 / ADR-0023).
    for pipeline in &pipelines {
        let source_columns = source_columns_for_pipeline(
            source,
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
    sync_base_datasets_for_pipelines(store, &deployment, &pipelines, &options, source).await?;

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
            source,
            target,
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
    deliver_pipelines_with_options(
        store,
        &deployment,
        &to_deliver,
        false,
        false,
        source,
        target,
    )
    .await?;

    println!("Deployment applied: {}", deployment.name);
    Ok(())
}
