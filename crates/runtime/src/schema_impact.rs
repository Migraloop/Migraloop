//! Schema Change impact / pause seam inside Incremental Sync (ADR-0009 / issue #176).
//!
//! Distinct from Poison quarantine (ADR-0015) and Backpressure (ADR-0020):
//! blocking Source DDL pauses the *affected* Pipeline(s) after impact
//! classification — not blanket pause, and not poison quarantine.

use std::collections::BTreeSet;

use migraloop_capture::{
    classify_schema_impact, PipelineSchemaDeps, SchemaChangeEvent, SchemaImpact,
};
use migraloop_delivery::ManagedFieldAs;
use migraloop_platform_store::{BaseDataset, Pipeline, PlatformStore, SchemaChangeImpact};
use migraloop_transform::{parse_transform_steps, used_base_fields, TransformOp};

use crate::observability::{emit_event, EventValue};
use crate::{pipeline_base_table_refs, pipeline_references_table, transform_ops_from_pipeline, RuntimeError};

/// Dependency columns for Schema Change impact classification.
pub(crate) fn pipeline_schema_deps(pipeline: &Pipeline, dataset: &BaseDataset) -> PipelineSchemaDeps {
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
pub(crate) async fn apply_schema_change_impacts(
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
                    .mark_schema_impact(&record)
                    .await
                    .map_err(|err| RuntimeError::Failed(err.to_string()))?;
                pipeline.paused = true;
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
