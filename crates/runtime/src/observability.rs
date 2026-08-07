//! Observability Surface assembly (ADR-0008 / issue #174).
//!
//! Typed Sync Health / Delivery Health (plus lag, quarantine, schema-impact
//! blockers, and ADR-0010 disk-warn) are assembled here from a status inventory
//! snapshot. Operator CLI formats narrative from this assembly; Prometheus
//! scrapes derive the same failure / lag / disk-warn facts. Component pressure
//! summaries (ADR-0031 / issue #249) use the same stable names as Lab reports
//! and Capacity Estimate.

use std::collections::BTreeMap;

use migraloop_platform_store::{BaseDataset, Pipeline, PlatformStoreHealth};

use crate::capacity::{
    assemble_component_pressure_from_surface, ComponentPressure, ComponentPressureOverrides,
};
use crate::lifecycle::StatusInventory;

/// Typed Sync Health for a Base Dataset.
///
/// Derived from capture/apply progress and failures — not only store placeholders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncHealth {
    /// No Incremental progress yet (Initial Load / never synced).
    Unknown,
    /// Caught up (lag == 0) after Incremental progress.
    Ok,
    /// Source backlog remains (lag > 0) — catching up / under backpressure.
    Lagging,
    /// Durable capture/apply failure signal.
    Failed,
}

impl SyncHealth {
    pub fn as_str(self) -> &'static str {
        match self {
            SyncHealth::Unknown => "unknown",
            SyncHealth::Ok => "ok",
            SyncHealth::Lagging => "lagging",
            SyncHealth::Failed => "failed",
        }
    }
}

/// Typed Delivery Health for a Pipeline Target Binding.
///
/// Values match Operator-visible `status` labels:
/// `paused` | `unhealthy` | `ok` | `pending` | `unknown`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryHealth {
    Paused,
    Unhealthy,
    Ok,
    Pending,
    Unknown,
}

impl DeliveryHealth {
    pub fn as_str(self) -> &'static str {
        match self {
            DeliveryHealth::Paused => "paused",
            DeliveryHealth::Unhealthy => "unhealthy",
            DeliveryHealth::Ok => "ok",
            DeliveryHealth::Pending => "pending",
            DeliveryHealth::Unknown => "unknown",
        }
    }
}

/// Sync Health observation for one Base Dataset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BaseSyncObservation {
    pub deployment_name: String,
    pub source_table: String,
    pub health: SyncHealth,
    pub applied_changes: i32,
    pub lag: i32,
    pub checkpoint: Option<i64>,
}

/// Delivery Health observation for one Pipeline with a Target Binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineDeliveryObservation {
    pub deployment_name: String,
    pub pipeline_name: String,
    pub health: DeliveryHealth,
    pub delivery_status: String,
    pub applied_changes: i32,
    pub lag: i32,
    pub quarantined: usize,
    pub schema_blocking: usize,
    pub paused: bool,
}

/// Runtime-facing Observability Surface assembled from durable inventory.
///
/// CLI `status` and Prometheus `/metrics` must agree on these health / failure /
/// lag / disk-warn / component-pressure facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservabilitySurface {
    pub store_health: PlatformStoreHealth,
    pub guardrail_error: Option<String>,
    pub free_disk_bytes: Option<u64>,
    /// ADR-0010 warn-only — never implies Pipeline pause.
    pub disk_warn: bool,
    pub sync: Vec<BaseSyncObservation>,
    pub delivery: Vec<PipelineDeliveryObservation>,
    pub quarantined_total: usize,
    pub schema_blocking_total: usize,
    /// Alertable failures: active quarantines + blocking Schema Change impacts.
    pub failure_count: usize,
    /// Per-component pressure (app / source / platform_store / target) — ADR-0031.
    pub component_pressure: Vec<ComponentPressure>,
}

/// Assemble typed Sync / Delivery Health (+ lag, quarantine, schema-impact, disk-warn)
/// without component pressure (used by Capacity Estimate to avoid recursion).
pub(crate) fn assemble_observability_core(inventory: &StatusInventory) -> ObservabilitySurface {
    let sync: Vec<BaseSyncObservation> = inventory
        .bases
        .iter()
        .map(|base| BaseSyncObservation {
            deployment_name: base.deployment_name.clone(),
            source_table: base.source_table.clone(),
            health: derive_sync_health(base),
            applied_changes: base.sync_applied_changes,
            lag: base.sync_lag,
            checkpoint: base.capture_checkpoint,
        })
        .collect();

    let mut delivery = Vec::new();
    for pipeline in &inventory.pipelines {
        if pipeline.target_collection.is_empty() {
            continue;
        }
        let quarantined = inventory
            .quarantines
            .iter()
            .filter(|q| {
                q.deployment_name == pipeline.deployment_name && q.pipeline_name == pipeline.name
            })
            .count();
        let schema_blocking = inventory
            .schema_impacts
            .iter()
            .filter(|s| {
                s.deployment_name == pipeline.deployment_name
                    && s.pipeline_name == pipeline.name
                    && s.impact == "blocking"
            })
            .count();
        delivery.push(PipelineDeliveryObservation {
            deployment_name: pipeline.deployment_name.clone(),
            pipeline_name: pipeline.name.clone(),
            health: derive_delivery_health(pipeline, quarantined),
            delivery_status: pipeline.delivery_status.clone(),
            applied_changes: pipeline.delivery_applied_changes,
            lag: pipeline.delivery_lag,
            quarantined,
            schema_blocking,
            paused: pipeline.paused,
        });
    }

    let quarantined_total = inventory.quarantines.len();
    let schema_blocking_total = inventory
        .schema_impacts
        .iter()
        .filter(|s| s.impact == "blocking")
        .count();

    ObservabilitySurface {
        store_health: inventory.health.clone(),
        guardrail_error: inventory.guardrail_error.clone(),
        free_disk_bytes: inventory.free_disk_bytes,
        disk_warn: inventory.disk_warn,
        sync,
        delivery,
        quarantined_total,
        schema_blocking_total,
        failure_count: quarantined_total + schema_blocking_total,
        component_pressure: Vec::new(),
    }
}

/// Assemble typed Sync / Delivery Health (+ lag, quarantine, schema-impact, disk-warn,
/// component pressure).
pub fn assemble_observability_surface(inventory: &StatusInventory) -> ObservabilitySurface {
    let mut surface = assemble_observability_core(inventory);
    surface.component_pressure = assemble_component_pressure_from_surface(
        &surface,
        &ComponentPressureOverrides::default(),
    );
    surface
}

/// Derive Sync Health from lag / progress / durable failure — beyond `unknown`/`ok`.
pub(crate) fn derive_sync_health(base: &BaseDataset) -> SyncHealth {
    let stored = base.sync_health.as_str();
    if stored.eq_ignore_ascii_case("failed") {
        return SyncHealth::Failed;
    }
    if base.sync_lag > 0 {
        return SyncHealth::Lagging;
    }
    // Initial Load may set cutover checkpoint without Incremental progress — stay unknown
    // until applied changes / incremental status / durable ok|lagging label.
    if base.sync_applied_changes > 0
        || base.status == "incremental"
        || stored.eq_ignore_ascii_case("ok")
        || stored.eq_ignore_ascii_case("lagging")
    {
        // lag == 0: caught up (including after a prior lagging window drained).
        return SyncHealth::Ok;
    }
    SyncHealth::Unknown
}

/// Derive Delivery Health from pause / quarantine / delivery_status.
pub(crate) fn derive_delivery_health(
    pipeline: &Pipeline,
    active_quarantines_for_pipeline: usize,
) -> DeliveryHealth {
    if pipeline.paused {
        DeliveryHealth::Paused
    } else if active_quarantines_for_pipeline > 0 {
        DeliveryHealth::Unhealthy
    } else {
        match pipeline.delivery_status.as_str() {
            "delivered" => DeliveryHealth::Ok,
            "pending" => DeliveryHealth::Pending,
            _ => DeliveryHealth::Unknown,
        }
    }
}

/// Persistable Sync Health label for Incremental progress writes.
pub(crate) fn sync_health_label_for_progress(sync_lag: i32) -> &'static str {
    if sync_lag > 0 {
        SyncHealth::Lagging.as_str()
    } else {
        SyncHealth::Ok.as_str()
    }
}

/// Persistable Sync Health label for durable capture/apply failure.
pub(crate) fn sync_health_label_failed() -> &'static str {
    SyncHealth::Failed.as_str()
}

/// Prometheus text exposition derived from the same Observability assembly as `status`.
pub fn render_prometheus_metrics(surface: &ObservabilitySurface) -> String {
    let mut out = String::new();
    out.push_str(
        "# HELP migraloop_sync_lag Sync Health lag (pending Source changes not yet applied to Base).\n",
    );
    out.push_str("# TYPE migraloop_sync_lag gauge\n");
    for base in &surface.sync {
        out.push_str(&format!(
            "migraloop_sync_lag{{deployment=\"{}\",table=\"{}\"}} {}\n",
            prom_label(&base.deployment_name),
            prom_label(&base.source_table),
            base.lag
        ));
    }

    out.push_str(
        "# HELP migraloop_sync_applied_changes Sync Health applied change count for a Base Dataset.\n",
    );
    out.push_str("# TYPE migraloop_sync_applied_changes gauge\n");
    for base in &surface.sync {
        out.push_str(&format!(
            "migraloop_sync_applied_changes{{deployment=\"{}\",table=\"{}\"}} {}\n",
            prom_label(&base.deployment_name),
            prom_label(&base.source_table),
            base.applied_changes
        ));
    }

    out.push_str("# HELP migraloop_delivery_lag Delivery Health lag (pending Delivery work).\n");
    out.push_str("# TYPE migraloop_delivery_lag gauge\n");
    for pipeline in &surface.delivery {
        out.push_str(&format!(
            "migraloop_delivery_lag{{deployment=\"{}\",pipeline=\"{}\"}} {}\n",
            prom_label(&pipeline.deployment_name),
            prom_label(&pipeline.pipeline_name),
            pipeline.lag
        ));
    }

    out.push_str(
        "# HELP migraloop_delivery_applied_changes Delivery Health applied change count for a Pipeline.\n",
    );
    out.push_str("# TYPE migraloop_delivery_applied_changes gauge\n");
    for pipeline in &surface.delivery {
        out.push_str(&format!(
            "migraloop_delivery_applied_changes{{deployment=\"{}\",pipeline=\"{}\"}} {}\n",
            prom_label(&pipeline.deployment_name),
            prom_label(&pipeline.pipeline_name),
            pipeline.applied_changes
        ));
    }

    out.push_str("# HELP migraloop_pipeline_paused Whether a Pipeline is paused (1) or not (0).\n");
    out.push_str("# TYPE migraloop_pipeline_paused gauge\n");
    for pipeline in &surface.delivery {
        out.push_str(&format!(
            "migraloop_pipeline_paused{{deployment=\"{}\",pipeline=\"{}\"}} {}\n",
            prom_label(&pipeline.deployment_name),
            prom_label(&pipeline.pipeline_name),
            if pipeline.paused { 1 } else { 0 }
        ));
    }

    out.push_str(
        "# HELP migraloop_quarantined_changes Alertable count of active Poison Change quarantines per Pipeline.\n",
    );
    out.push_str("# TYPE migraloop_quarantined_changes gauge\n");
    for pipeline in &surface.delivery {
        out.push_str(&format!(
            "migraloop_quarantined_changes{{deployment=\"{}\",pipeline=\"{}\"}} {}\n",
            prom_label(&pipeline.deployment_name),
            prom_label(&pipeline.pipeline_name),
            pipeline.quarantined
        ));
    }

    out.push_str(
        "# HELP migraloop_failures Alertable failure gauge (active quarantines + blocking Schema Change impacts).\n",
    );
    out.push_str("# TYPE migraloop_failures gauge\n");
    out.push_str(&format!("migraloop_failures {}\n", surface.failure_count));

    // Platform Store resource signals (ADR-0010): warn-only disk threshold.
    out.push_str(
        "# HELP migraloop_platform_store_disk_free_bytes Free bytes on the Platform Store data volume when known (-1 if unknown).\n",
    );
    out.push_str("# TYPE migraloop_platform_store_disk_free_bytes gauge\n");
    let free_metric = surface.free_disk_bytes.map(|b| b as i64).unwrap_or(-1);
    out.push_str(&format!(
        "migraloop_platform_store_disk_free_bytes {free_metric}\n"
    ));
    out.push_str(
        "# HELP migraloop_platform_store_disk_warn Whether Platform Store free disk is below the warn threshold (1) or not (0). Warn-only — never auto-pauses Pipelines.\n",
    );
    out.push_str("# TYPE migraloop_platform_store_disk_warn gauge\n");
    out.push_str(&format!(
        "migraloop_platform_store_disk_warn {}\n",
        if surface.disk_warn { 1 } else { 0 }
    ));

    // Component pressure (ADR-0031): same stable names as Lab reports / Capacity Estimate.
    out.push_str(
        "# HELP migraloop_component_pressure Coarse 0–100 pressure for app, Source, Platform Store, or Target.\n",
    );
    out.push_str("# TYPE migraloop_component_pressure gauge\n");
    for comp in &surface.component_pressure {
        out.push_str(&format!(
            "migraloop_component_pressure{{component=\"{}\"}} {}\n",
            prom_label(&comp.component),
            comp.pressure
        ));
    }
    out.push_str(
        "# HELP migraloop_component_saturated Whether a component is saturated (1) or not (0). Source/Platform Store/Target saturation means infra-saturated evidence.\n",
    );
    out.push_str("# TYPE migraloop_component_saturated gauge\n");
    for comp in &surface.component_pressure {
        out.push_str(&format!(
            "migraloop_component_saturated{{component=\"{}\"}} {}\n",
            prom_label(&comp.component),
            if comp.saturated { 1 } else { 0 }
        ));
    }

    out
}

fn prom_label(value: &str) -> String {
    value
        .chars()
        .map(|c| match c {
            '\\' => "\\\\".to_string(),
            '"' => "\\\"".to_string(),
            '\n' => "\\n".to_string(),
            other => other.to_string(),
        })
        .collect()
}

/// Emit one structured JSON operator event line (stdout).
pub fn emit_event(event: &str, fields: &[(&str, EventValue)]) {
    let mut map = BTreeMap::new();
    map.insert("event".to_string(), EventValue::Str(event.to_string()));
    for (k, v) in fields {
        map.insert((*k).to_string(), v.clone());
    }
    match serde_json::to_string(&map) {
        Ok(json) => println!("{json}"),
        Err(err) => eprintln!("structured log encode failed for event={event}: {err}"),
    }
}

#[derive(Clone, Debug)]
pub enum EventValue {
    Str(String),
    Int(i64),
    Bool(bool),
}

impl serde::Serialize for EventValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            EventValue::Str(s) => serializer.serialize_str(s),
            EventValue::Int(n) => serializer.serialize_i64(*n),
            EventValue::Bool(b) => serializer.serialize_bool(*b),
        }
    }
}

impl From<&str> for EventValue {
    fn from(value: &str) -> Self {
        EventValue::Str(value.to_string())
    }
}

impl From<String> for EventValue {
    fn from(value: String) -> Self {
        EventValue::Str(value)
    }
}

impl From<i64> for EventValue {
    fn from(value: i64) -> Self {
        EventValue::Int(value)
    }
}

impl From<i32> for EventValue {
    fn from(value: i32) -> Self {
        EventValue::Int(i64::from(value))
    }
}

impl From<usize> for EventValue {
    fn from(value: usize) -> Self {
        EventValue::Int(value as i64)
    }
}

impl From<u64> for EventValue {
    fn from(value: u64) -> Self {
        EventValue::Int(value as i64)
    }
}

impl From<bool> for EventValue {
    fn from(value: bool) -> Self {
        EventValue::Bool(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use migraloop_platform_store::{
        QuarantinedChange, SchemaChangeImpact,
    };

    fn empty_base(sync_health: &str, sync_lag: i32, applied: i32) -> BaseDataset {
        BaseDataset {
            deployment_name: "dep".into(),
            source_table: "CUSTOMERS".into(),
            source_schema: "APP".into(),
            status: if applied > 0 {
                "incremental".into()
            } else {
                "initial_load_complete".into()
            },
            primary_key: vec!["ID".into()],
            columns: vec![],
            omitted_columns: vec![],
            row_count: 0,
            sync_applied_changes: applied,
            sync_health: sync_health.into(),
            capture_low_watermark: Some(1000),
            capture_checkpoint: if applied > 0 { Some(1001) } else { None },
            sync_lag,
            source_alignment: "unknown".into(),
            source_alignment_checked_rows: 0,
            source_alignment_mismatched_rows: 0,
            initial_load_cursor: None,
        }
    }

    fn sample_pipeline(paused: bool, delivery_status: &str, delivery_lag: i32) -> Pipeline {
        Pipeline {
            deployment_name: "dep".into(),
            name: "customers".into(),
            mode: "direct".into(),
            source_table: "CUSTOMERS".into(),
            source_schema: "APP".into(),
            target_collection: "customers".into(),
            delivery_status: delivery_status.into(),
            delivery_applied_changes: 1,
            delivery_lag,
            paused,
            description: String::new(),
            field_mappings: Default::default(),
            output_identity: vec![],
            transform_json: None,
            drift_status: "unknown".into(),
            drift_checked_rows: 0,
            drift_mismatched_rows: 0,
        }
    }

    fn inventory_with(
        bases: Vec<BaseDataset>,
        pipelines: Vec<Pipeline>,
        quarantines: Vec<QuarantinedChange>,
        schema_impacts: Vec<SchemaChangeImpact>,
        disk_warn: bool,
    ) -> StatusInventory {
        StatusInventory {
            health: PlatformStoreHealth::Healthy { schema_version: 1 },
            guardrail_error: None,
            free_disk_bytes: Some(512 * 1024 * 1024),
            disk_warn,
            deployments: vec![],
            pipelines,
            bases,
            derived: vec![],
            quarantines,
            schema_impacts,
        }
    }

    #[test]
    fn sync_health_lags_beyond_placeholder_ok() {
        // Store may still say "ok" while lag is durable — assembly must surface lagging.
        let health = derive_sync_health(&empty_base("ok", 12, 3));
        assert_eq!(health, SyncHealth::Lagging);
        assert_eq!(health.as_str(), "lagging");
    }

    #[test]
    fn sync_health_ok_when_caught_up_after_progress() {
        assert_eq!(
            derive_sync_health(&empty_base("lagging", 0, 5)),
            SyncHealth::Ok
        );
    }

    #[test]
    fn sync_health_failed_from_durable_failure_label() {
        assert_eq!(
            derive_sync_health(&empty_base("failed", 0, 2)),
            SyncHealth::Failed
        );
        // Failure wins over lag so Operators see the durable apply fault.
        assert_eq!(
            derive_sync_health(&empty_base("failed", 9, 2)),
            SyncHealth::Failed
        );
    }

    #[test]
    fn sync_health_unknown_before_incremental_progress() {
        assert_eq!(
            derive_sync_health(&empty_base("unknown", 0, 0)),
            SyncHealth::Unknown
        );
    }

    #[test]
    fn sync_health_unknown_when_only_cutover_checkpoint_present() {
        // Initial Load establishes cutover checkpoint without Incremental applies.
        let mut base = empty_base("unknown", 0, 0);
        base.capture_checkpoint = Some(999);
        assert_eq!(derive_sync_health(&base), SyncHealth::Unknown);
    }

    #[test]
    fn delivery_health_unhealthy_when_quarantined() {
        let pipeline = sample_pipeline(false, "delivered", 0);
        assert_eq!(derive_delivery_health(&pipeline, 1), DeliveryHealth::Unhealthy);
    }

    #[test]
    fn delivery_health_paused_beats_quarantine() {
        let pipeline = sample_pipeline(true, "delivered", 0);
        assert_eq!(derive_delivery_health(&pipeline, 2), DeliveryHealth::Paused);
    }

    #[test]
    fn assembly_produces_sync_delivery_lag_quarantine_schema_disk_warn() {
        let quarantine = QuarantinedChange {
            deployment_name: "dep".into(),
            pipeline_name: "customers".into(),
            source_schema: "APP".into(),
            source_table: "CUSTOMERS".into(),
            change_id: "c1".into(),
            capture_position: 1,
            output_identity: serde_json::json!(1),
            stage: "delivery".into(),
            attempts: 3,
            last_error: "boom".into(),
            status: "active".into(),
        };
        let blocking = SchemaChangeImpact {
            deployment_name: "dep".into(),
            pipeline_name: "customers".into(),
            source_schema: "APP".into(),
            source_table: "CUSTOMERS".into(),
            change_id: "ddl1".into(),
            capture_position: 2,
            ddl_summary: "ALTER TABLE".into(),
            impact: "blocking".into(),
            status: "open".into(),
        };
        let inventory = inventory_with(
            vec![empty_base("ok", 7, 2)],
            vec![sample_pipeline(true, "delivered", 4)],
            vec![quarantine],
            vec![blocking],
            true,
        );

        let surface = assemble_observability_surface(&inventory);
        assert!(surface.disk_warn, "disk-warn must remain warn-only signal");
        assert_eq!(surface.free_disk_bytes, Some(512 * 1024 * 1024));
        assert_eq!(surface.sync.len(), 1);
        assert_eq!(surface.sync[0].health, SyncHealth::Lagging);
        assert_eq!(surface.sync[0].lag, 7);
        assert_eq!(surface.delivery.len(), 1);
        assert_eq!(surface.delivery[0].health, DeliveryHealth::Paused);
        assert_eq!(surface.delivery[0].lag, 4);
        assert_eq!(surface.delivery[0].quarantined, 1);
        assert_eq!(surface.delivery[0].schema_blocking, 1);
        assert_eq!(surface.quarantined_total, 1);
        assert_eq!(surface.schema_blocking_total, 1);
        assert_eq!(surface.failure_count, 2);
        assert_eq!(surface.component_pressure.len(), 4);
        assert!(
            surface
                .component_pressure
                .iter()
                .any(|c| c.component == "platform_store" && c.saturated),
            "disk-warn must saturate platform_store pressure"
        );
    }

    #[test]
    fn prometheus_agrees_with_assembly_failure_lag_disk_warn() {
        let inventory = inventory_with(
            vec![empty_base("ok", 9, 1)],
            vec![sample_pipeline(false, "delivered", 3)],
            vec![QuarantinedChange {
                deployment_name: "dep".into(),
                pipeline_name: "customers".into(),
                source_schema: "APP".into(),
                source_table: "CUSTOMERS".into(),
                change_id: "c1".into(),
                capture_position: 1,
                output_identity: serde_json::json!(1),
                stage: "delivery".into(),
                attempts: 2,
                last_error: "err".into(),
                status: "active".into(),
            }],
            vec![],
            true,
        );
        let surface = assemble_observability_surface(&inventory);
        let text = render_prometheus_metrics(&surface);
        assert!(text.contains("migraloop_sync_lag{deployment=\"dep\",table=\"CUSTOMERS\"} 9"));
        assert!(
            text.contains("migraloop_delivery_lag{deployment=\"dep\",pipeline=\"customers\"} 3")
        );
        assert!(text.contains(
            "migraloop_quarantined_changes{deployment=\"dep\",pipeline=\"customers\"} 1"
        ));
        assert!(text.contains("migraloop_failures 1"));
        assert!(text.contains("migraloop_platform_store_disk_warn 1"));
        // Disk warn must not imply pause in metrics.
        assert!(
            text.contains("migraloop_pipeline_paused{deployment=\"dep\",pipeline=\"customers\"} 0")
        );
        assert!(
            text.contains("migraloop_component_pressure{component=\"platform_store\"}"),
            "Prometheus must expose component pressure: {text}"
        );
        assert!(
            text.contains("migraloop_component_saturated{component=\"platform_store\"} 1"),
            "Prometheus must expose component saturated: {text}"
        );
    }

    #[test]
    fn progress_label_writes_lagging_or_ok() {
        assert_eq!(sync_health_label_for_progress(5), "lagging");
        assert_eq!(sync_health_label_for_progress(0), "ok");
    }
}
