//! Component pressure + Capacity Estimate (ADR-0031 / issue #249).
//!
//! Stable names are shared by Lab Scenario reports, live Observability Surface /
//! Prometheus, and the Operator Capacity Estimate command. Estimates are
//! advisory only — they never mutate Source System or Target System configuration.

use crate::lifecycle::StatusInventory;
use crate::observability::{assemble_observability_core, ObservabilitySurface};

/// Stable component id: app process / runtime path.
pub const COMPONENT_APP: &str = "app";
/// Stable component id: Source System.
pub const COMPONENT_SOURCE: &str = "source";
/// Stable component id: Platform Store.
pub const COMPONENT_PLATFORM_STORE: &str = "platform_store";
/// Stable component id: Target System.
pub const COMPONENT_TARGET: &str = "target";

/// Ordered component pressure names (Lab + live Observability + Capacity Estimate).
pub const COMPONENT_PRESSURE_NAMES: [&str; 4] = [
    COMPONENT_APP,
    COMPONENT_SOURCE,
    COMPONENT_PLATFORM_STORE,
    COMPONENT_TARGET,
];

/// Coarse reference QPS used when no component is saturated (ADR-0031 Direct floor scale).
pub const CAPACITY_REFERENCE_E2E_QPS: f64 = 100_000.0;

/// Lag at/above this maps Source/Target to saturated (infra — resize Lab / advise Operator).
const LAG_SATURATION_THRESHOLD: i32 = 10_000;

/// Per-component pressure summary with stable name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComponentPressure {
    pub component: String,
    /// Coarse 0–100 pressure (higher = hotter).
    pub pressure: u8,
    pub saturated: bool,
}

/// Optional Test/Lab override for one component (injectable signals).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComponentPressureOverride {
    pub pressure: u8,
    pub saturated: bool,
}

/// Optional overrides for all four components (contract twin / Lab inject).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ComponentPressureOverrides {
    pub app: Option<ComponentPressureOverride>,
    pub source: Option<ComponentPressureOverride>,
    pub platform_store: Option<ComponentPressureOverride>,
    pub target: Option<ComponentPressureOverride>,
}

impl ComponentPressureOverrides {
    /// Parse `component=pressure:saturated,...` (saturated is `1`/`true`/`yes` or `0`/`false`/`no`).
    ///
    /// Example: `source=95:1,platform_store=10:0`
    pub fn parse(spec: &str) -> Result<Self, String> {
        let mut out = Self::default();
        for part in spec.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let (name, rest) = part
                .split_once('=')
                .ok_or_else(|| format!("invalid component pressure override `{part}` (want name=pressure:saturated)"))?;
            let (pressure_s, sat_s) = rest
                .split_once(':')
                .ok_or_else(|| format!("invalid component pressure override `{part}` (want pressure:saturated)"))?;
            let pressure: u8 = pressure_s.trim().parse().map_err(|_| {
                format!("invalid pressure in override `{part}` (want 0–100 integer)")
            })?;
            if pressure > 100 {
                return Err(format!(
                    "invalid pressure in override `{part}` (want 0–100 integer)"
                ));
            }
            let saturated = parse_bool_token(sat_s.trim()).ok_or_else(|| {
                format!("invalid saturated flag in override `{part}` (want 0/1/true/false)")
            })?;
            let slot = match name.trim() {
                COMPONENT_APP => &mut out.app,
                COMPONENT_SOURCE => &mut out.source,
                COMPONENT_PLATFORM_STORE => &mut out.platform_store,
                COMPONENT_TARGET => &mut out.target,
                other => {
                    return Err(format!(
                        "unknown component `{other}` (want app|source|platform_store|target)"
                    ));
                }
            };
            *slot = Some(ComponentPressureOverride {
                pressure,
                saturated,
            });
        }
        Ok(out)
    }

    fn get(&self, component: &str) -> Option<ComponentPressureOverride> {
        match component {
            COMPONENT_APP => self.app,
            COMPONENT_SOURCE => self.source,
            COMPONENT_PLATFORM_STORE => self.platform_store,
            COMPONENT_TARGET => self.target,
            _ => None,
        }
    }
}

fn parse_bool_token(s: &str) -> Option<bool> {
    match s.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Some(true),
        "0" | "false" | "no" => Some(false),
        _ => None,
    }
}

/// Capacity Estimate: limiting component + coarse max e2e Managed Delivery QPS.
///
/// Never mutates Source/Target database configuration — advisory only (ADR-0031).
#[derive(Debug, Clone, PartialEq)]
pub struct CapacityEstimate {
    pub limiting_component: String,
    pub max_e2e_qps: f64,
    pub components: Vec<ComponentPressure>,
    /// True when Source, Platform Store, or Target is saturated (infra — resize, not product fail).
    pub infra_saturated: bool,
}

/// Assemble the four component pressure summaries from inventory (+ optional overrides).
pub fn assemble_component_pressure(
    inventory: &StatusInventory,
    overrides: &ComponentPressureOverrides,
) -> Vec<ComponentPressure> {
    // Use core assembly (no pressure) to avoid recurse through public Observability assemble.
    let surface = assemble_observability_core(inventory);
    assemble_component_pressure_from_surface(&surface, overrides)
}

/// Same assembly from an already-built Observability Surface (status / metrics path).
pub fn assemble_component_pressure_from_surface(
    surface: &ObservabilitySurface,
    overrides: &ComponentPressureOverrides,
) -> Vec<ComponentPressure> {
    COMPONENT_PRESSURE_NAMES
        .iter()
        .map(|name| {
            if let Some(over) = overrides.get(name) {
                return ComponentPressure {
                    component: (*name).to_string(),
                    pressure: over.pressure.min(100),
                    saturated: over.saturated,
                };
            }
            match *name {
                COMPONENT_APP => derive_app_pressure(surface),
                COMPONENT_SOURCE => derive_source_pressure(surface),
                COMPONENT_PLATFORM_STORE => derive_platform_store_pressure(surface),
                COMPONENT_TARGET => derive_target_pressure(surface),
                _ => ComponentPressure {
                    component: (*name).to_string(),
                    pressure: 0,
                    saturated: false,
                },
            }
        })
        .collect()
}

fn derive_platform_store_pressure(surface: &ObservabilitySurface) -> ComponentPressure {
    if surface.disk_warn {
        return ComponentPressure {
            component: COMPONENT_PLATFORM_STORE.to_string(),
            pressure: 90,
            saturated: true,
        };
    }
    let pressure = match surface.free_disk_bytes {
        Some(free) if free < 2 * 1024 * 1024 * 1024 => 40,
        Some(_) => 10,
        None => 5,
    };
    ComponentPressure {
        component: COMPONENT_PLATFORM_STORE.to_string(),
        pressure,
        saturated: false,
    }
}

fn lag_to_pressure(lag: i32) -> (u8, bool) {
    if lag >= LAG_SATURATION_THRESHOLD {
        (95, true)
    } else if lag >= 1_000 {
        (85, false)
    } else if lag >= 100 {
        (60, false)
    } else if lag >= 1 {
        (30, false)
    } else {
        (5, false)
    }
}

fn derive_source_pressure(surface: &ObservabilitySurface) -> ComponentPressure {
    let max_lag = surface.sync.iter().map(|s| s.lag).max().unwrap_or(0);
    let (mut pressure, saturated) = lag_to_pressure(max_lag);
    if surface
        .sync
        .iter()
        .any(|s| s.health == crate::observability::SyncHealth::Failed)
    {
        pressure = pressure.max(80);
    }
    if saturated {
        pressure = pressure.max(95);
    }
    ComponentPressure {
        component: COMPONENT_SOURCE.to_string(),
        pressure,
        saturated,
    }
}

fn derive_target_pressure(surface: &ObservabilitySurface) -> ComponentPressure {
    let max_lag = surface.delivery.iter().map(|d| d.lag).max().unwrap_or(0);
    let (mut pressure, saturated) = lag_to_pressure(max_lag);
    if surface
        .delivery
        .iter()
        .any(|d| d.health == crate::observability::DeliveryHealth::Unhealthy)
    {
        pressure = pressure.max(70);
    }
    if saturated {
        pressure = pressure.max(95);
    }
    ComponentPressure {
        component: COMPONENT_TARGET.to_string(),
        pressure,
        saturated,
    }
}

fn derive_app_pressure(surface: &ObservabilitySurface) -> ComponentPressure {
    let mut pressure: u8 = 5;
    if surface.failure_count > 0 {
        pressure = pressure.max(40);
    }
    let sync_hot = surface.sync.iter().any(|s| s.lag > 0);
    let delivery_hot = surface.delivery.iter().any(|d| d.lag > 0);
    let store_hot = surface.disk_warn;
    // When both Sync and Delivery show backlog but Platform Store is not disk-warned,
    // attribute medium pressure to the app path (processing / Affect / queues).
    if sync_hot && delivery_hot && !store_hot {
        pressure = pressure.max(55);
    }
    // App saturation is product-side (not infra-saturated).
    let saturated = pressure >= 95;
    ComponentPressure {
        component: COMPONENT_APP.to_string(),
        pressure,
        saturated,
    }
}

/// Whether evidence is infra-saturated (Source / Platform Store / Target only).
pub fn infra_saturated(components: &[ComponentPressure]) -> bool {
    components.iter().any(|c| {
        c.saturated
            && matches!(
                c.component.as_str(),
                COMPONENT_SOURCE | COMPONENT_PLATFORM_STORE | COMPONENT_TARGET
            )
    })
}

/// Build a Capacity Estimate from component pressure summaries.
pub fn estimate_capacity(components: &[ComponentPressure]) -> CapacityEstimate {
    let infra = infra_saturated(components);
    let limiting = components
        .iter()
        .max_by(|a, b| {
            a.pressure
                .cmp(&b.pressure)
                .then_with(|| component_tiebreak(&a.component).cmp(&component_tiebreak(&b.component)))
        })
        .cloned()
        .unwrap_or(ComponentPressure {
            component: COMPONENT_APP.to_string(),
            pressure: 0,
            saturated: false,
        });

    let max_e2e_qps = if infra {
        0.0
    } else {
        let headroom = (100.0 - f64::from(limiting.pressure)) / 100.0;
        (CAPACITY_REFERENCE_E2E_QPS * headroom).max(0.0)
    };

    CapacityEstimate {
        limiting_component: limiting.component,
        max_e2e_qps,
        components: components.to_vec(),
        infra_saturated: infra,
    }
}

/// Tie-break when pressures are equal: prefer infra components for Operator guidance.
fn component_tiebreak(name: &str) -> u8 {
    match name {
        COMPONENT_PLATFORM_STORE => 0,
        COMPONENT_SOURCE => 1,
        COMPONENT_TARGET => 2,
        COMPONENT_APP => 3,
        _ => 4,
    }
}

/// Assemble pressure + Capacity Estimate from inventory in one step.
pub fn capacity_estimate_from_inventory(
    inventory: &StatusInventory,
    overrides: &ComponentPressureOverrides,
) -> CapacityEstimate {
    let components = assemble_component_pressure(inventory, overrides);
    estimate_capacity(&components)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lifecycle::StatusInventory;
    use crate::observability::{
        assemble_observability_core, BaseSyncObservation, DeliveryHealth,
        PipelineDeliveryObservation, SyncHealth,
    };
    use migraloop_platform_store::{BaseDataset, Pipeline, PlatformStoreHealth};

    fn empty_inventory(disk_warn: bool, sync_lag: i32, delivery_lag: i32) -> StatusInventory {
        StatusInventory {
            health: PlatformStoreHealth::Healthy { schema_version: 1 },
            guardrail_error: None,
            free_disk_bytes: Some(512 * 1024 * 1024),
            disk_warn,
            deployments: vec![],
            pipelines: vec![Pipeline {
                deployment_name: "dep".into(),
                name: "customers".into(),
                mode: "direct".into(),
                source_table: "CUSTOMERS".into(),
                source_schema: "APP".into(),
                target_collection: "customers".into(),
                delivery_status: "delivered".into(),
                delivery_applied_changes: 1,
                delivery_lag,
                paused: false,
                description: String::new(),
                field_mappings: Default::default(),
                output_identity: vec![],
                transform_json: None,
                drift_status: "unknown".into(),
                drift_checked_rows: 0,
                drift_mismatched_rows: 0,
            }],
            bases: vec![BaseDataset {
                deployment_name: "dep".into(),
                source_table: "CUSTOMERS".into(),
                source_schema: "APP".into(),
                status: "incremental".into(),
                primary_key: vec!["ID".into()],
                columns: vec![],
                omitted_columns: vec![],
                row_count: 1,
                sync_applied_changes: 1,
                sync_health: "ok".into(),
                capture_low_watermark: Some(1),
                capture_checkpoint: Some(2),
                sync_lag,
                source_alignment: "unknown".into(),
                source_alignment_checked_rows: 0,
                source_alignment_mismatched_rows: 0,
                initial_load_cursor: None,
            }],
            derived: vec![],
            quarantines: vec![],
            schema_impacts: vec![],
        }
    }

    #[test]
    fn stable_names_cover_four_components() {
        assert_eq!(
            COMPONENT_PRESSURE_NAMES,
            [
                COMPONENT_APP,
                COMPONENT_SOURCE,
                COMPONENT_PLATFORM_STORE,
                COMPONENT_TARGET
            ]
        );
    }

    #[test]
    fn disk_warn_saturates_platform_store_and_limits_capacity() {
        let inventory = empty_inventory(true, 0, 0);
        let estimate = capacity_estimate_from_inventory(&inventory, &ComponentPressureOverrides::default());
        assert!(estimate.infra_saturated);
        let store = estimate
            .components
            .iter()
            .find(|c| c.component == COMPONENT_PLATFORM_STORE)
            .expect("platform_store");
        assert!(store.saturated);
        assert_eq!(estimate.limiting_component, COMPONENT_PLATFORM_STORE);
        assert_eq!(estimate.max_e2e_qps, 0.0);
    }

    #[test]
    fn override_selects_limiting_component_without_mutating_config() {
        let inventory = empty_inventory(false, 0, 0);
        let overrides = ComponentPressureOverrides::parse("source=95:1,target=10:0")
            .expect("parse overrides");
        let estimate = capacity_estimate_from_inventory(&inventory, &overrides);
        assert!(estimate.infra_saturated);
        assert_eq!(estimate.limiting_component, COMPONENT_SOURCE);
        assert_eq!(estimate.max_e2e_qps, 0.0);
        let source = estimate
            .components
            .iter()
            .find(|c| c.component == COMPONENT_SOURCE)
            .unwrap();
        assert_eq!(source.pressure, 95);
        assert!(source.saturated);
    }

    #[test]
    fn healthy_inventory_reports_headroom_qps() {
        let inventory = empty_inventory(false, 0, 0);
        let estimate = capacity_estimate_from_inventory(&inventory, &ComponentPressureOverrides::default());
        assert!(!estimate.infra_saturated);
        assert!(estimate.max_e2e_qps > 0.0);
        assert!(estimate.max_e2e_qps <= CAPACITY_REFERENCE_E2E_QPS);
        assert_eq!(estimate.components.len(), 4);
    }

    #[test]
    fn high_source_lag_raises_source_pressure() {
        let inventory = empty_inventory(false, 12_000, 0);
        let components =
            assemble_component_pressure(&inventory, &ComponentPressureOverrides::default());
        let source = components
            .iter()
            .find(|c| c.component == COMPONENT_SOURCE)
            .unwrap();
        assert!(source.saturated);
        assert!(infra_saturated(&components));
    }

    #[test]
    fn surface_assembly_includes_same_pressure_names() {
        let inventory = empty_inventory(false, 50, 50);
        let surface = assemble_observability_core(&inventory);
        // Names must match Capacity Estimate / live Observability vocabulary.
        let pressure = assemble_component_pressure_from_surface(
            &surface,
            &ComponentPressureOverrides::default(),
        );
        let names: Vec<&str> = pressure.iter().map(|c| c.component.as_str()).collect();
        assert_eq!(
            names,
            vec![
                COMPONENT_APP,
                COMPONENT_SOURCE,
                COMPONENT_PLATFORM_STORE,
                COMPONENT_TARGET
            ]
        );
        // Keep observation types linked so refactors cannot drop Sync/Delivery from surface.
        let _sync: &BaseSyncObservation = &surface.sync[0];
        let _del: &PipelineDeliveryObservation = &surface.delivery[0];
        assert_eq!(_sync.health, SyncHealth::Lagging);
        assert_eq!(_del.health, DeliveryHealth::Ok);
    }
}
