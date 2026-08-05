//! Pipeline lifecycle (pause / resume / remove), Source Alignment Check, Drift Check,
//! and status inventory reads (issue #155 / ADR-0007).
//!
//! Source Alignment Check and Drift Check stay distinct Operator verbs. Shared
//! budget / field-subset equality / mismatch-collect helpers live in
//! [`crate::verify_repair`] (issue #183) so isomorphic internals are fixed once.
//!
//! Change (Pipeline revision) remains inside [`crate::apply`]. These verbs use a
//! Platform Store session — callers do not sequence URL-shaped store CRUD.

use std::collections::{BTreeMap, BTreeSet};

use migraloop_capture::{AlignmentCheckSample, SourceEngine};
use migraloop_delivery::{DeliveryDocument, TargetEngine};
use migraloop_platform_store::{
    BaseDataset, Deployment, DerivedDataset, Pipeline, PlatformStore, PlatformStoreHealth,
    QuarantinedChange, SchemaChangeImpact,
};

use crate::verify_repair::{
    collect_mismatched_repairs, detect_status, document_fields_match, effective_max_rows,
    maps_equal_on_keys, persisted_status,
};
use crate::{
    deliver_direct_pipeline_with_options, deliver_transform_pipeline_with_options,
    delivery_document_for_row, ensure_store_session_healthy, identity_key, pipeline_base_table_refs,
    pipeline_has_target, source_engine_from_connection, source_timezone_opt,
    target_document_identity_key, target_engine_from_deployment, RuntimeError,
};

/// Default Source Alignment Check read budget (resource gate; not a full slam).
pub const DEFAULT_ALIGNMENT_MAX_ROWS: u32 = 1000;

/// Default Drift Check identity budget (resource gate; not a full slam).
pub const DEFAULT_DRIFT_MAX_ROWS: u32 = 1000;

/// Snapshot of Operator-visible inventory reads for `status` (and metrics).
///
/// Formatting stays in the CLI adapter; this type concentrates Platform Store
/// session reads so clap does not own store CRUD sequencing.
#[derive(Debug, Clone)]
pub struct StatusInventory {
    pub health: PlatformStoreHealth,
    /// When health is Healthy but Platform Store Guardrails fail (ADR-0010).
    pub guardrail_error: Option<String>,
    /// Observed free-disk bytes when known (metrics / status); `None` if unobserved.
    pub free_disk_bytes: Option<u64>,
    /// Whether free-disk is under the warn threshold (ADR-0010 warn-only).
    pub disk_warn: bool,
    pub deployments: Vec<Deployment>,
    pub pipelines: Vec<Pipeline>,
    pub bases: Vec<BaseDataset>,
    pub derived: Vec<DerivedDataset>,
    pub quarantines: Vec<QuarantinedChange>,
    pub schema_impacts: Vec<SchemaChangeImpact>,
}

/// Load Base Dataset rows for Operator `base` inspect (session verb).
pub async fn inspect_base_rows(
    store: &PlatformStore,
    table: &str,
    deployment_name: Option<&str>,
) -> Result<(BaseDataset, Vec<migraloop_platform_store::BaseRow>), RuntimeError> {
    ensure_store_session_healthy(store).await?;
    store
        .get_base_rows(table, deployment_name)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))
}

/// Load Derived Dataset rows for Operator `derived` inspect (session verb).
pub async fn inspect_derived_rows(
    store: &PlatformStore,
    pipeline_name: &str,
    deployment_name: Option<&str>,
) -> Result<(DerivedDataset, Vec<migraloop_platform_store::DerivedRow>), RuntimeError> {
    ensure_store_session_healthy(store).await?;
    store
        .get_derived_rows(pipeline_name, deployment_name)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))
}

/// Resolve a Target Binding and list Target documents for Operator `target` inspect.
///
/// Uses the Target engine interface — CLI formats only.
pub async fn inspect_target_documents(
    store: &PlatformStore,
    collection: &str,
    deployment_name: Option<&str>,
) -> Result<(Deployment, Pipeline, Vec<serde_json::Value>), RuntimeError> {
    ensure_store_session_healthy(store).await?;
    let pipelines = store
        .list_pipelines()
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    let matching: Vec<_> = pipelines
        .into_iter()
        .filter(|p| {
            p.target_collection == collection
                && deployment_name
                    .map(|name| p.deployment_name == name)
                    .unwrap_or(true)
        })
        .collect();
    let pipeline = match matching.as_slice() {
        [] => {
            return Err(RuntimeError::Failed(format!(
                "no Pipeline Target Binding found for collection {collection}"
            )));
        }
        [only] => only.clone(),
        many => {
            return Err(RuntimeError::Failed(format!(
                "multiple Pipelines bind collection {collection} across Deployments {}; \
                 pass --deployment to disambiguate",
                many.iter()
                    .map(|p| p.deployment_name.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
    };
    let deployments = store
        .list_deployments()
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    let deployment = deployments
        .into_iter()
        .find(|d| d.name == pipeline.deployment_name)
        .ok_or_else(|| {
            RuntimeError::Failed(format!(
                "Deployment {} not found for Target inspect",
                pipeline.deployment_name
            ))
        })?;
    let target = target_engine_from_deployment(&deployment)?;
    let documents = target
        .list_documents(collection)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    Ok((deployment, pipeline, documents))
}

/// Load status inventory from a Platform Store URL.
///
/// Open failures map to [`PlatformStoreHealth::Unreachable`] (same Operator-visible
/// contract as the former CLI `health` path) so `status` can print without the
/// clap adapter owning store CRUD.
pub async fn status_inventory_from_url(
    database_url: &str,
) -> Result<StatusInventory, RuntimeError> {
    match PlatformStore::open(database_url).await {
        Ok(store) => status_inventory(&store).await,
        Err(err) => Ok(StatusInventory {
            health: PlatformStoreHealth::Unreachable {
                reason: err.to_string(),
            },
            guardrail_error: None,
            free_disk_bytes: None,
            disk_warn: false,
            deployments: Vec::new(),
            pipelines: Vec::new(),
            bases: Vec::new(),
            derived: Vec::new(),
            quarantines: Vec::new(),
            schema_impacts: Vec::new(),
        }),
    }
}

/// Load status inventory through one Platform Store session.
///
/// When the store is healthy, settings guardrails are checked (recorded on the
/// snapshot — not auto-pause) and disk pressure is warn-only metadata.
pub async fn status_inventory(store: &PlatformStore) -> Result<StatusInventory, RuntimeError> {
    let health = store.health().await;
    let mut guardrail_error = None;
    let mut free_disk_bytes = None;
    let mut disk_warn = false;
    let mut deployments = Vec::new();
    let mut pipelines = Vec::new();
    let mut bases = Vec::new();
    let mut derived = Vec::new();
    let mut quarantines = Vec::new();
    let mut schema_impacts = Vec::new();

    if matches!(health, PlatformStoreHealth::Healthy { .. }) {
        match store.probe_settings().await {
            Ok(settings) => {
                if let Err(err) = migraloop_platform_store::check_store_settings(&settings) {
                    guardrail_error = Some(err.to_string());
                }
            }
            Err(err) => {
                guardrail_error = Some(err.to_string());
            }
        }

        // Inventory lists stay available for metrics even when guardrails fail;
        // `status` formatting returns early on `guardrail_error` (ADR-0010 hard path).
        let resources = store
            .probe_resources()
            .await
            .map_err(|err| RuntimeError::Failed(err.to_string()))?;
        free_disk_bytes = resources.free_disk_bytes;
        disk_warn = resources.disk_warn;

        deployments = store
            .list_deployments()
            .await
            .map_err(|err| RuntimeError::Failed(err.to_string()))?;
        pipelines = store
            .list_pipelines()
            .await
            .map_err(|err| RuntimeError::Failed(err.to_string()))?;
        bases = store
            .list_base_datasets()
            .await
            .map_err(|err| RuntimeError::Failed(err.to_string()))?;
        derived = store
            .list_derived_datasets()
            .await
            .map_err(|err| RuntimeError::Failed(err.to_string()))?;
        quarantines = store
            .list_quarantined_changes(None)
            .await
            .map_err(|err| RuntimeError::Failed(err.to_string()))?;
        schema_impacts = store
            .list_schema_change_impacts(None)
            .await
            .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    }

    Ok(StatusInventory {
        health,
        guardrail_error,
        free_disk_bytes,
        disk_warn,
        deployments,
        pipelines,
        bases,
        derived,
        quarantines,
        schema_impacts,
    })
}

async fn resolve_named_pipeline(
    store: &PlatformStore,
    pipeline_name: &str,
    deployment_name: Option<&str>,
) -> Result<Pipeline, RuntimeError> {
    let pipelines = store
        .list_pipelines()
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    let matching: Vec<_> = pipelines
        .into_iter()
        .filter(|p| {
            p.name == pipeline_name
                && deployment_name
                    .map(|name| p.deployment_name == name)
                    .unwrap_or(true)
        })
        .collect();
    match matching.as_slice() {
        [] => Err(RuntimeError::Failed(format!(
            "Pipeline {pipeline_name} not found{}",
            deployment_name
                .map(|d| format!(" in Deployment {d}"))
                .unwrap_or_default()
        ))),
        [only] => Ok(only.clone()),
        many => Err(RuntimeError::Failed(format!(
            "multiple Pipelines named {pipeline_name} across Deployments {}; \
             pass --deployment to disambiguate",
            many.iter()
                .map(|p| p.deployment_name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

/// Pause a Pipeline's Delivery/processing (ADR-0007). Durable Base/checkpoint retained.
pub async fn pause_pipeline(
    store: &PlatformStore,
    pipeline_name: &str,
    deployment_name: Option<&str>,
) -> Result<(), RuntimeError> {
    ensure_store_session_healthy(store).await?;
    let pipeline = resolve_named_pipeline(store, pipeline_name, deployment_name).await?;
    if pipeline.paused {
        println!(
            "Pipeline {} already paused (Deployment {})",
            pipeline.name, pipeline.deployment_name
        );
        return Ok(());
    }
    store
        .set_pipeline_paused(&pipeline.deployment_name, &pipeline.name, true)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    println!(
        "Pipeline {} paused (Deployment {}) — Delivery/processing stopped; \
         durable Base/checkpoint state retained",
        pipeline.name, pipeline.deployment_name
    );
    Ok(())
}

/// Resume a paused Pipeline and catch up Delivery from durable Base/Derived state.
pub async fn resume_pipeline(
    store: &PlatformStore,
    pipeline_name: &str,
    deployment_name: Option<&str>,
) -> Result<(), RuntimeError> {
    ensure_store_session_healthy(store).await?;
    let pipeline = resolve_named_pipeline(store, pipeline_name, deployment_name).await?;
    if !pipeline.paused {
        println!(
            "Pipeline {} is not paused (Deployment {})",
            pipeline.name, pipeline.deployment_name
        );
        return Ok(());
    }

    store
        .resume_pipeline(&pipeline.deployment_name, &pipeline.name)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    let deployments = store
        .list_deployments()
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    let deployment = deployments
        .into_iter()
        .find(|d| d.name == pipeline.deployment_name)
        .ok_or_else(|| {
            RuntimeError::Failed(format!(
                "Deployment {} not found for Pipeline resume",
                pipeline.deployment_name
            ))
        })?;

    // Catch up Delivery from durable Base/Derived state accumulated while paused.
    if pipeline_has_target(&pipeline) {
        let source = source_engine_from_connection(&deployment.source)?;
        let target = target_engine_from_deployment(&deployment)?;
        match pipeline.mode.as_str() {
            "direct" => {
                deliver_direct_pipeline_with_options(
                    store,
                    &deployment,
                    &pipeline,
                    &source,
                    &target,
                    true,
                )
                .await?;
            }
            "transform" => {
                deliver_transform_pipeline_with_options(
                    store,
                    &deployment,
                    &pipeline,
                    &source,
                    &target,
                    true,
                )
                .await?;
            }
            other => {
                return Err(RuntimeError::Failed(format!(
                    "unsupported pipeline.mode {other:?} for resume catch-up Delivery"
                )));
            }
        }
    }

    println!(
        "Pipeline {} resumed (Deployment {}) — Delivery continues from durable state",
        pipeline.name, pipeline.deployment_name
    );
    Ok(())
}

/// Remove a Pipeline; prune Base Datasets no longer referenced (Shared Bases kept).
pub async fn remove_pipeline(
    store: &PlatformStore,
    pipeline_name: &str,
    deployment_name: Option<&str>,
) -> Result<(), RuntimeError> {
    ensure_store_session_healthy(store).await?;
    let pipeline = resolve_named_pipeline(store, pipeline_name, deployment_name).await?;

    // Keep Shared Bases still referenced by remaining Pipelines; prune only tables
    // no longer referenced (same capture-scope rule as apply — ADR-0019 / ADR-0007).
    // Keep-set is computed before remove so the deleted Pipeline is excluded.
    let remaining = store
        .list_pipelines()
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?
        .into_iter()
        .filter(|p| {
            p.deployment_name == pipeline.deployment_name && p.name != pipeline.name
        })
        .collect::<Vec<_>>();
    let mut keep = BTreeSet::new();
    for remaining_pipeline in &remaining {
        for (schema, table) in pipeline_base_table_refs(remaining_pipeline) {
            keep.insert((schema, table));
        }
    }
    let keep_tables: Vec<(String, String)> = keep.into_iter().collect();
    store
        .remove_pipeline(
            &pipeline.deployment_name,
            &pipeline.name,
            &keep_tables,
        )
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    println!(
        "Pipeline {} removed (Deployment {}) — Delivery/processing stopped; \
         Shared Base Datasets kept when still referenced",
        pipeline.name, pipeline.deployment_name
    );
    Ok(())
}

fn supported_row_projection(
    row: &serde_json::Map<String, serde_json::Value>,
    supported: &BTreeSet<String>,
) -> serde_json::Map<String, serde_json::Value> {
    row.iter()
        .filter(|(name, _)| supported.contains(name.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

fn base_identity_key(
    row: &serde_json::Map<String, serde_json::Value>,
    primary_key: &[String],
) -> Option<String> {
    if primary_key.is_empty() {
        return None;
    }
    let mut parts = Vec::with_capacity(primary_key.len());
    for col in primary_key {
        let value = row.get(col)?;
        parts.push(identity_key(value));
    }
    Some(parts.join("|"))
}

/// Source Alignment Check — repair Base from Source reads (never write Source).
pub async fn source_alignment_check(
    store: &PlatformStore,
    table: Option<&str>,
    deployment: Option<&str>,
    max_rows: u32,
) -> Result<(), RuntimeError> {
    ensure_store_session_healthy(store).await?;
    let max_rows = effective_max_rows(max_rows, DEFAULT_ALIGNMENT_MAX_ROWS);

    let deployments = store
        .list_deployments()
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    if deployments.is_empty() {
        return Err(RuntimeError::Failed(
            "no Deployments applied; run `migraloop apply` first".to_string(),
        ));
    }

    let bases = store
        .list_base_datasets()
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    let targets: Vec<BaseDataset> = bases
        .into_iter()
        .filter(|base| {
            table
                .map(|t| base.source_table.eq_ignore_ascii_case(t))
                .unwrap_or(true)
                && deployment
                    .map(|d| base.deployment_name == d)
                    .unwrap_or(true)
        })
        .collect();
    if targets.is_empty() {
        return Err(RuntimeError::Failed(match (table, deployment) {
            (Some(t), Some(d)) => {
                format!("no Base Dataset found for table {t} in Deployment {d}")
            }
            (Some(t), None) => format!("no Base Dataset found for table {t}"),
            (None, Some(d)) => format!("no Base Datasets found for Deployment {d}"),
            (None, None) => "no Base Datasets found; run `migraloop apply` first".to_string(),
        }));
    }

    for base in targets {
        let deployment = deployments
            .iter()
            .find(|d| d.name == base.deployment_name)
            .ok_or_else(|| {
                RuntimeError::Failed(format!(
                    "Deployment {} missing for Base Dataset {}",
                    base.deployment_name, base.source_table
                ))
            })?;
        align_one_base(store, deployment, &base, max_rows).await?;
    }
    Ok(())
}

async fn align_one_base(
    store: &PlatformStore,
    deployment: &Deployment,
    base: &BaseDataset,
    max_rows: u32,
) -> Result<(), RuntimeError> {
    if base.primary_key.is_empty() {
        return Err(RuntimeError::Failed(format!(
            "Base Dataset {} has no primary key for Source Alignment Check",
            base.source_table
        )));
    }

    let source = source_engine_from_connection(&deployment.source)?;
    let configured_tz = source_timezone_opt(deployment);
    let sample: AlignmentCheckSample = source
        .alignment_check_read(
            &base.source_schema,
            &base.source_table,
            max_rows,
            configured_tz,
        )
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    let (_, base_rows) = store
        .get_base_rows(&base.source_table, Some(&base.deployment_name))
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    let supported: BTreeSet<String> = if base.columns.is_empty() {
        sample
            .columns
            .iter()
            .filter(|c| c.supported)
            .map(|c| c.name.clone())
            .collect()
    } else {
        base.columns.iter().map(|c| c.name.clone()).collect()
    };

    let mut base_by_id: BTreeMap<String, serde_json::Map<String, serde_json::Value>> =
        BTreeMap::new();
    for row in &base_rows {
        let Some(key) = base_identity_key(&row.data, &base.primary_key) else {
            continue;
        };
        base_by_id.insert(key, row.data.clone());
    }

    let mut repaired: BTreeMap<String, serde_json::Map<String, serde_json::Value>> =
        BTreeMap::new();
    let mut mismatched = 0i32;
    let mut repaired_count = 0i32;
    let mut checked_ids: BTreeSet<String> = BTreeSet::new();

    for source_row in &sample.rows {
        let source_as_map: serde_json::Map<String, serde_json::Value> = source_row
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let source_map = supported_row_projection(&source_as_map, &supported);
        let Some(key) = base_identity_key(&source_map, &base.primary_key) else {
            continue;
        };
        checked_ids.insert(key.clone());
        match base_by_id.get(&key) {
            Some(existing) if maps_equal_on_keys(existing, &source_map, &supported) => {
                repaired.insert(key, existing.clone());
            }
            Some(_) | None => {
                mismatched += 1;
                repaired_count += 1;
                repaired.insert(key, source_map);
            }
        }
    }

    // Rows outside the gated Source window: keep when truncated; drop when full read.
    for (key, row) in &base_by_id {
        if checked_ids.contains(key) {
            continue;
        }
        if sample.truncated {
            repaired.insert(key.clone(), row.clone());
        } else {
            mismatched += 1;
            repaired_count += 1;
            // Source no longer has this identity — remove from Base (never write Source).
        }
    }

    let mut rows: Vec<serde_json::Map<String, serde_json::Value>> =
        repaired.into_values().collect();
    // Stable ordinal order by primary key for inspectability.
    rows.sort_by(|a, b| {
        let ka = base_identity_key(a, &base.primary_key).unwrap_or_default();
        let kb = base_identity_key(b, &base.primary_key).unwrap_or_default();
        ka.cmp(&kb)
    });

    let alignment_status = persisted_status(sample.truncated, "aligned");
    let checked = sample.rows.len() as i32;
    let updated = BaseDataset {
        deployment_name: base.deployment_name.clone(),
        source_table: base.source_table.clone(),
        source_schema: base.source_schema.clone(),
        status: base.status.clone(),
        primary_key: base.primary_key.clone(),
        columns: base.columns.clone(),
        omitted_columns: base.omitted_columns.clone(),
        row_count: rows.len() as i32,
        sync_applied_changes: base.sync_applied_changes,
        sync_health: base.sync_health.clone(),
        capture_low_watermark: base.capture_low_watermark,
        capture_checkpoint: base.capture_checkpoint,
        sync_lag: base.sync_lag,
        source_alignment: alignment_status.to_string(),
        source_alignment_checked_rows: checked,
        source_alignment_mismatched_rows: mismatched,
        initial_load_cursor: None,
    };

    store
        .record_source_alignment_progress(&updated, &rows)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    let truncated_note = if sample.truncated {
        " truncated=true"
    } else {
        ""
    };
    let detect = detect_status(mismatched, sample.truncated, "aligned", "misaligned");
    println!(
        "Source Alignment Check: {} status={detect} checked={checked} \
         mismatched={mismatched} repaired={repaired_count} maxRows={max_rows}{truncated_note} \
         (Base repaired from Source reads; Source not written)",
        base.source_table
    );
    Ok(())
}

/// Drift Check — compare Managed fields to platform expected dataset; auto-repair Target.
pub async fn drift_check(
    store: &PlatformStore,
    pipeline_name: Option<&str>,
    deployment: Option<&str>,
    max_rows: u32,
) -> Result<(), RuntimeError> {
    ensure_store_session_healthy(store).await?;
    let max_rows = effective_max_rows(max_rows, DEFAULT_DRIFT_MAX_ROWS);

    let deployments = store
        .list_deployments()
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    if deployments.is_empty() {
        return Err(RuntimeError::Failed(
            "no Deployments applied; run `migraloop apply` first".to_string(),
        ));
    }

    let pipelines = store
        .list_pipelines()
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    let targets: Vec<Pipeline> = pipelines
        .into_iter()
        .filter(|p| {
            pipeline_has_target(p)
                && pipeline_name.map(|n| p.name == n).unwrap_or(true)
                && deployment.map(|d| p.deployment_name == d).unwrap_or(true)
        })
        .collect();
    if targets.is_empty() {
        return Err(RuntimeError::Failed(match (pipeline_name, deployment) {
            (Some(n), Some(d)) => {
                format!("no Pipeline with Target Binding named {n} in Deployment {d}")
            }
            (Some(n), None) => format!("no Pipeline with Target Binding named {n}"),
            (None, Some(d)) => {
                format!("no Pipelines with Target Binding found for Deployment {d}")
            }
            (None, None) => {
                "no Pipelines with Target Binding found; run `migraloop apply` first".to_string()
            }
        }));
    }

    for pipeline in targets {
        let deployment = deployments
            .iter()
            .find(|d| d.name == pipeline.deployment_name)
            .ok_or_else(|| {
                RuntimeError::Failed(format!(
                    "Deployment {} missing for Pipeline {}",
                    pipeline.deployment_name, pipeline.name
                ))
            })?;
        drift_one_pipeline(store, deployment, &pipeline, max_rows).await?;
    }
    Ok(())
}

async fn drift_one_pipeline(
    store: &PlatformStore,
    deployment: &Deployment,
    pipeline: &Pipeline,
    max_rows: u32,
) -> Result<(), RuntimeError> {
    ensure_drift_baseline_ready(store, pipeline).await?;

    let target = target_engine_from_deployment(deployment)?;
    let (expected_docs, truncated) =
        expected_delivery_documents_for_drift(store, pipeline, max_rows).await?;

    let target_docs = target
        .list_documents(&pipeline.target_collection)
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    let mut target_by_id: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for doc in target_docs {
        if let Some(key) = target_document_identity_key(&doc) {
            target_by_id.insert(key, doc);
        }
    }

    let expected_items = expected_docs.iter().map(|expected| {
        let key = identity_key(&expected.identity);
        (key, expected)
    });
    let outcome = collect_mismatched_repairs(
        expected_items,
        &target_by_id,
        |expected, target_doc| {
            let managed_keys: Vec<&str> =
                expected.managed_fields.keys().map(|k| k.as_str()).collect();
            document_fields_match(target_doc, &expected.managed_fields, &managed_keys)
        },
        |expected| expected.clone(),
    );
    let mismatched = outcome.mismatched;
    let repaired_count = outcome.repaired;
    let repair_docs = outcome.repairs;

    if !repair_docs.is_empty() {
        target
            .upsert_managed(&pipeline.target_collection, &repair_docs)
            .await
            .map_err(|err| RuntimeError::Failed(err.to_string()))?;
    }

    let checked = expected_docs.len() as i32;
    let drift_status = persisted_status(truncated, "ok");
    store
        .record_drift_outcome(
            &pipeline.deployment_name,
            &pipeline.name,
            drift_status,
            checked,
            mismatched,
        )
        .await
        .map_err(|err| RuntimeError::Failed(err.to_string()))?;

    let truncated_note = if truncated { " truncated=true" } else { "" };
    let detect = detect_status(mismatched, truncated, "ok", "drifted");
    println!(
        "Drift Check: Pipeline {} status={detect} checked={checked} \
         mismatched={mismatched} repaired={repaired_count} maxRows={max_rows}{truncated_note} \
         (Managed fields auto-repaired; non-Managed Target fields ignored)",
        pipeline.name
    );
    Ok(())
}

async fn ensure_drift_baseline_ready(
    store: &PlatformStore,
    pipeline: &Pipeline,
) -> Result<(), RuntimeError> {
    match pipeline.mode.as_str() {
        "direct" => {
            let bases = store
                .list_base_datasets()
                .await
                .map_err(|err| RuntimeError::Failed(err.to_string()))?;
            let base = bases
                .iter()
                .find(|b| {
                    b.deployment_name == pipeline.deployment_name
                        && b.source_table.eq_ignore_ascii_case(&pipeline.source_table)
                })
                .ok_or_else(|| {
                    RuntimeError::Failed(format!(
                        "no Base Dataset for Pipeline {} source table {}",
                        pipeline.name, pipeline.source_table
                    ))
                })?;
            if base.source_alignment == "unknown" {
                return Err(RuntimeError::Failed(format!(
                    "Drift Check refuses Pipeline {}: Base {} Source Alignment is unknown; \
                     run `migraloop align --table {}` first so Base is a trusted Drift baseline",
                    pipeline.name, base.source_table, base.source_table
                )));
            }
            Ok(())
        }
        "transform" => {
            let derived = store
                .list_derived_datasets()
                .await
                .map_err(|err| RuntimeError::Failed(err.to_string()))?;
            let dataset = derived.iter().find(|d| {
                d.deployment_name == pipeline.deployment_name && d.pipeline_name == pipeline.name
            });
            match dataset {
                Some(d) if !d.status.is_empty() => Ok(()),
                _ => Err(RuntimeError::Failed(format!(
                    "Drift Check refuses Pipeline {}: Derived Dataset not materialized yet",
                    pipeline.name
                ))),
            }
        }
        other => Err(RuntimeError::Failed(format!(
            "unsupported pipeline.mode {other:?} for Drift Check"
        ))),
    }
}

async fn expected_delivery_documents_for_drift(
    store: &PlatformStore,
    pipeline: &Pipeline,
    max_rows: u32,
) -> Result<(Vec<DeliveryDocument>, bool), RuntimeError> {
    let mut documents = match pipeline.mode.as_str() {
        "direct" => {
            let (dataset, rows) = store
                .get_base_rows(&pipeline.source_table, Some(&pipeline.deployment_name))
                .await
                .map_err(|err| RuntimeError::Failed(err.to_string()))?;
            if dataset.primary_key.is_empty() {
                return Err(RuntimeError::Failed(format!(
                    "Base Dataset {} has no primary key for Drift Check Output Identity",
                    pipeline.source_table
                )));
            }
            // Drift reads the platform expected dataset only — no extra Source load
            // (alignment already established the Base baseline).
            let mut docs = Vec::with_capacity(rows.len());
            for row in &rows {
                docs.push(delivery_document_for_row(
                    &row.data,
                    &dataset.primary_key,
                    &dataset.columns,
                    pipeline,
                )?);
            }
            docs
        }
        "transform" => {
            if pipeline.output_identity.is_empty() {
                return Err(RuntimeError::Failed(format!(
                    "Transform Pipeline {} requires outputIdentity for Drift Check",
                    pipeline.name
                )));
            }
            let (dataset, rows) = store
                .get_derived_rows(&pipeline.name, Some(&pipeline.deployment_name))
                .await
                .map_err(|err| RuntimeError::Failed(err.to_string()))?;
            let mut docs = Vec::with_capacity(rows.len());
            for row in &rows {
                docs.push(delivery_document_for_row(
                    &row.data,
                    &dataset.output_identity,
                    &dataset.columns,
                    pipeline,
                )?);
            }
            docs
        }
        other => {
            return Err(RuntimeError::Failed(format!(
                "unsupported pipeline.mode {other:?} for Drift Check"
            )));
        }
    };

    documents.sort_by(|a, b| identity_key(&a.identity).cmp(&identity_key(&b.identity)));
    let truncated = documents.len() > max_rows as usize;
    if truncated {
        documents.truncate(max_rows as usize);
    }
    Ok((documents, truncated))
}


