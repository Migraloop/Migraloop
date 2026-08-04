//! Lab Scenario recipe types and on-disk loaders (ADR-0025 / issues #157, #173).
//!
//! `recipe.yaml` is the runner interface for workload steps, optional typed
//! `product_path` steps, checks, and equal-weight metric thresholds. Adapters
//! implement seeds/escapes/asserts; they must not duplicate threshold constants
//! or the common prepare→apply→mutate→sync path when `product_path` is set.

use std::fs;
use std::path::Path;

use serde::Deserialize;

use crate::CliError;

use super::registered_scenario_ids;

#[derive(Debug, Deserialize)]
pub(crate) struct ScenarioRecipe {
    pub(crate) id: String,
    pub(crate) summary: String,
    pub(crate) namespace: ScenarioRecipeNamespace,
    #[serde(default = "default_deployment_config")]
    pub(crate) deployment_config: String,
    pub(crate) workload: ScenarioRecipeWorkload,
    pub(crate) checks: ScenarioRecipeChecks,
    /// Fail-able metric axes (equal weight with correctness). Live runner interface.
    #[serde(default)]
    pub(crate) thresholds: ScenarioRecipeThresholds,
}

pub(crate) fn default_deployment_config() -> String {
    "deployment.yaml".to_string()
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
pub(crate) struct ScenarioRecipeNamespace {
    pub(crate) source_tables: Vec<String>,
    pub(crate) target_collections: Vec<String>,
    pub(crate) deployment: String,
    /// Pipeline identities inside the Scenario Namespace (authoring metadata).
    #[serde(default)]
    pub(crate) pipelines: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ScenarioRecipeWorkload {
    pub(crate) concurrency: String,
    #[serde(default)]
    pub(crate) steps: Vec<String>,
    /// Optional typed product-path steps executed by the shared runner (#173).
    /// When absent, the Scenario still uses a full `adapt_*` adapter.
    #[serde(default)]
    pub(crate) product_path: Option<ScenarioRecipeProductPath>,
}

/// Shared product-path vocabulary driven from recipe data (not only prose `steps`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ProductPathStepKind {
    PrepareNamespace,
    ProductApply,
    Mutate,
    ProductSync,
    Assert,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ScenarioRecipeProductPath {
    pub(crate) steps: Vec<ProductPathStepKind>,
    #[serde(default)]
    pub(crate) apply: ProductPathApplyOpts,
    #[serde(default)]
    pub(crate) sync: ProductPathSyncOpts,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ProductPathApplyOpts {
    #[serde(default = "default_true")]
    pub(crate) require_initial_load: bool,
    #[serde(default)]
    pub(crate) require_delivery: bool,
    #[serde(default)]
    pub(crate) require_derived: bool,
}

impl Default for ProductPathApplyOpts {
    fn default() -> Self {
        Self {
            require_initial_load: true,
            require_delivery: false,
            require_derived: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ProductPathSyncOpts {
    #[serde(default = "default_true")]
    pub(crate) require_logminer: bool,
}

impl Default for ProductPathSyncOpts {
    fn default() -> Self {
        Self {
            require_logminer: true,
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ScenarioRecipeChecks {
    #[serde(default)]
    pub(crate) correctness: Vec<String>,
}

#[derive(Debug, Default, Clone, Deserialize)]
pub(crate) struct ScenarioRecipeThresholds {
    pub(crate) max_settle_ms: Option<u128>,
    pub(crate) max_lag: Option<i32>,
    pub(crate) max_duration_ms: Option<u128>,
    pub(crate) min_rows_per_s: Option<f64>,
}

/// Validate optional `workload.product_path` when present (US18 / #173).
pub(crate) fn validate_product_path(
    path_display: &str,
    product_path: &ScenarioRecipeProductPath,
) -> Result<(), CliError> {
    if product_path.steps.is_empty() {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {path_display} workload.product_path.steps must be non-empty"
        )));
    }
    let has_prepare = product_path
        .steps
        .iter()
        .any(|s| *s == ProductPathStepKind::PrepareNamespace);
    let has_assert = product_path
        .steps
        .iter()
        .any(|s| *s == ProductPathStepKind::Assert);
    if !has_prepare || !has_assert {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {path_display} workload.product_path.steps must include \
             `prepare_namespace` and `assert`"
        )));
    }
    let has_apply = product_path
        .steps
        .iter()
        .any(|s| *s == ProductPathStepKind::ProductApply);
    let has_sync = product_path
        .steps
        .iter()
        .any(|s| *s == ProductPathStepKind::ProductSync);
    if !has_apply && !has_sync {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {path_display} workload.product_path.steps must include \
             at least one of `product_apply` or `product_sync` (real product path)"
        )));
    }
    // Real product path only (ADR-0025 / ADR-0028): product_sync must require LogMiner.
    if has_sync && !product_path.sync.require_logminer {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {path_display} workload.product_path.sync.require_logminer \
             must be true when `product_sync` is present (no mock Sync path)"
        )));
    }
    Ok(())
}

pub(crate) fn load_recipe(path: &Path) -> Result<ScenarioRecipe, CliError> {
    let raw = fs::read_to_string(path).map_err(|err| {
        CliError::Failed(format!(
            "failed to read Lab Scenario recipe {}: {err}",
            path.display()
        ))
    })?;
    let recipe: ScenarioRecipe = serde_yaml::from_str(&raw).map_err(|err| {
        CliError::Failed(format!(
            "failed to parse Lab Scenario recipe {}: {err}",
            path.display()
        ))
    })?;
    if recipe.id.is_empty() || recipe.summary.is_empty() {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {} must set non-empty `id` and `summary`",
            path.display()
        )));
    }
    if recipe.namespace.source_tables.is_empty()
        || recipe.namespace.target_collections.is_empty()
        || recipe.namespace.deployment.is_empty()
    {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {} must declare namespace.source_tables, \
             target_collections, and deployment",
            path.display()
        )));
    }
    match recipe.workload.concurrency.as_str() {
        "serial" | "parallel" => {}
        other => {
            return Err(CliError::Failed(format!(
                "Lab Scenario recipe {} workload.concurrency must be \
                 `serial` or `parallel` (got `{other}`)",
                path.display()
            )));
        }
    }
    if recipe.workload.steps.is_empty() {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {} must declare workload.steps",
            path.display()
        )));
    }
    if recipe.checks.correctness.is_empty() {
        return Err(CliError::Failed(format!(
            "Lab Scenario recipe {} must declare checks.correctness",
            path.display()
        )));
    }
    if let Some(product_path) = &recipe.workload.product_path {
        validate_product_path(&path.display().to_string(), product_path)?;
    }
    Ok(recipe)
}

/// Selectable catalog: registered runners that have `recipe.yaml` + deployment
/// config under `lab_dir/scenarios/<id>/`. Summaries come from the recipe file.
pub(crate) fn load_selectable_catalog(lab_dir: &Path) -> Result<Vec<(String, String)>, CliError> {
    Ok(load_selectable_recipes(lab_dir)?
        .into_iter()
        .map(|recipe| (recipe.id, recipe.summary))
        .collect())
}

/// Load complete selectable Scenario recipes (id matches directory + deployment config present).
pub(crate) fn load_selectable_recipes(lab_dir: &Path) -> Result<Vec<ScenarioRecipe>, CliError> {
    let mut recipes = Vec::new();
    for id in registered_scenario_ids() {
        let scenario_dir = lab_dir.join("scenarios").join(id);
        let recipe_path = scenario_dir.join("recipe.yaml");
        if !recipe_path.is_file() {
            // Not selectable yet — recipe package incomplete (feature-time authoring in progress).
            continue;
        }
        let recipe = load_recipe(&recipe_path)?;
        if recipe.id != *id {
            return Err(CliError::Failed(format!(
                "Lab Scenario recipe {} has id `{}` but lives under scenarios/{id}/ \
                 (directory name must match recipe id)",
                recipe_path.display(),
                recipe.id
            )));
        }
        let deployment_path = scenario_dir.join(&recipe.deployment_config);
        if !deployment_path.is_file() {
            return Err(CliError::Failed(format!(
                "Lab Scenario `{id}` recipe references missing deployment config {} \
                 (expected under lab/scenarios/{id}/)",
                deployment_path.display()
            )));
        }
        recipes.push(recipe);
    }
    Ok(recipes)
}
