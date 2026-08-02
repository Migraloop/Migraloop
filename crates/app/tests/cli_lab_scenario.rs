//! Operator-visible seam: Lab Scenario list / run / remove (Namespace cleanup).
//!
//! Agreed seam (issues #60–#66, #63, #84, #85, #86 / PRD #55): CLI Lab Scenario commands.
//! Always-on tests cover catalog listing from on-disk `recipe.yaml` packages
//! (including bulk-load, rt-project, rt-filter, idempotent-redelivery),
//! shipped-capability coverage visibility, help surface, one-at-a-time rejection,
//! refusal of non-Lab / production engine bindings, Namespace cleanup control
//! surface, and CLI-seam bulk-load correctness-fail / metrics-fail probes
//! (`MIGRALOOP_LAB_SCENARIO_OUTCOME_PROBE`). Full Scenario run / re-run / remove
//! against the Lab Fixture (including leftover Namespace naming on `lab status`)
//! is ignored by default (Docker + Instant Client) — not a Release Quality Gate.

use std::fs;
use std::path::Path;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn bin() -> String {
    env!("CARGO_BIN_EXE_migraloop").to_string()
}

fn lab_dir() -> String {
    let manifest = env!("CARGO_MANIFEST_DIR");
    format!("{manifest}/../../lab")
}

/// Isolated `--lab-dir` with a stub `compose.yaml` so parallel lock tests do not
/// race on `lab/.migraloop-scenario.lock`.
fn temp_lab_dir() -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("temp lab dir");
    fs::write(dir.path().join("compose.yaml"), "name: migraloop-lab-test\n")
        .expect("write stub compose.yaml");
    let path = dir.path().to_string_lossy().into_owned();
    (dir, path)
}

/// Minimal selectable Scenario package (`recipe.yaml` + `deployment.yaml`) for
/// always-on CLI probes that use an isolated `--lab-dir`.
fn write_minimal_scenario_package(lab: &Path, id: &str) {
    write_scenario_package(lab, id, &format!("test recipe for {id}"));
}

fn write_scenario_package(lab: &Path, id: &str, summary: &str) {
    write_scenario_package_with_deployment(
        lab,
        id,
        summary,
        &format!("apiVersion: migraloop.dev/v1\nkind: Deployment\nmetadata:\n  name: lab-{id}\n"),
    );
}

/// Full Deployment shape with Source/Target bindings (for Lab engine isolation probes).
fn lab_fixture_deployment_yaml(id: &str, source_host: &str, target_host: &str) -> String {
    format!(
        r#"apiVersion: migraloop.dev/v1
kind: Deployment
metadata:
  name: lab-{id}
spec:
  source:
    kind: oracle
    host: {source_host}
    port: 1521
    database: FREEPDB1
    username: SYNC_USER
    password:
      fromEnv: ORACLE_PASSWORD
  target:
    kind: mongodb
    host: {target_host}
    port: 27017
    database: lab
    username: migraloop
    password:
      fromEnv: MONGO_PASSWORD
  pipelines:
    - name: lab-test
      mode: direct
      source:
        table: LAB_TEST
      target:
        collection: lab_test
"#
    )
}

fn write_scenario_package_with_deployment(lab: &Path, id: &str, summary: &str, deployment: &str) {
    let scenario_dir = lab.join("scenarios").join(id);
    fs::create_dir_all(&scenario_dir).expect("scenario dir");
    fs::write(scenario_dir.join("deployment.yaml"), deployment).expect("deployment.yaml");
    fs::write(
        scenario_dir.join("recipe.yaml"),
        format!(
            r#"id: {id}
summary: {summary}
namespace:
  source_tables: [LAB_TEST]
  target_collections: [lab_test]
  deployment: lab-{id}
  pipelines: [lab-test]
deployment_config: deployment.yaml
workload:
  concurrency: serial
  steps:
    - prepare Namespace
    - apply via real product path
checks:
  correctness:
    - Managed outcomes match recipe expectations
"#
        ),
    )
    .expect("recipe.yaml");
}

fn temp_lab_dir_with_recipes(ids: &[&str]) -> (tempfile::TempDir, String) {
    let (dir, path) = temp_lab_dir();
    for id in ids {
        write_minimal_scenario_package(dir.path(), id);
    }
    (dir, path)
}

#[tokio::test]
async fn lab_help_lists_scenario() {
    let help = Command::new(bin())
        .args(["lab", "--help"])
        .output()
        .expect("run lab --help");
    assert!(
        help.status.success(),
        "lab --help failed: {}",
        String::from_utf8_lossy(&help.stderr)
    );
    let stdout = String::from_utf8_lossy(&help.stdout);
    assert!(
        stdout.contains("scenario"),
        "lab --help should list `scenario`, got:\n{stdout}"
    );
}

#[tokio::test]
async fn lab_scenario_list_includes_direct_pipeline() {
    let list = Command::new(bin())
        .args(["lab", "scenario", "list", "--lab-dir", &lab_dir()])
        .output()
        .expect("run lab scenario list");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&list.stdout),
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(list.status.success(), "lab scenario list failed:\n{out}");
    assert!(
        out.contains("Lab Scenarios:") || out.contains("direct-pipeline"),
        "expected catalog header / direct-pipeline, got:\n{out}"
    );
    assert!(
        out.contains("direct-pipeline"),
        "catalog must list direct-pipeline, got:\n{out}"
    );
}

#[tokio::test]
async fn lab_scenario_list_includes_transform_pipeline() {
    let list = Command::new(bin())
        .args(["lab", "scenario", "list", "--lab-dir", &lab_dir()])
        .output()
        .expect("run lab scenario list");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&list.stdout),
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(list.status.success(), "lab scenario list failed:\n{out}");
    assert!(
        out.contains("transform-pipeline"),
        "catalog must list multi-table Transform Pipeline Scenario, got:\n{out}"
    );
}

#[tokio::test]
async fn lab_scenario_list_includes_concurrent_source_workload() {
    let list = Command::new(bin())
        .args(["lab", "scenario", "list", "--lab-dir", &lab_dir()])
        .output()
        .expect("run lab scenario list");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&list.stdout),
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(list.status.success(), "lab scenario list failed:\n{out}");
    assert!(
        out.contains("concurrent-source-workload"),
        "catalog must list intra-Scenario concurrent Source workload Scenario, got:\n{out}"
    );
}

#[tokio::test]
async fn lab_scenario_list_includes_bulk_load() {
    let list = Command::new(bin())
        .args(["lab", "scenario", "list", "--lab-dir", &lab_dir()])
        .output()
        .expect("run lab scenario list");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&list.stdout),
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(list.status.success(), "lab scenario list failed:\n{out}");
    assert!(
        out.contains("bulk-load"),
        "catalog must list bulk-load Lab Scenario (~100k Source inserts), got:\n{out}"
    );
}

#[tokio::test]
async fn lab_scenario_list_includes_rt_project() {
    let list = Command::new(bin())
        .args(["lab", "scenario", "list", "--lab-dir", &lab_dir()])
        .output()
        .expect("run lab scenario list");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&list.stdout),
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(list.status.success(), "lab scenario list failed:\n{out}");
    assert!(
        out.contains("rt-project"),
        "catalog must list Rich Transform project Scenario, got:\n{out}"
    );
    assert!(
        out.to_ascii_lowercase().contains("project"),
        "rt-project summary should mention project, got:\n{out}"
    );
}

#[tokio::test]
async fn lab_scenario_list_includes_rt_filter() {
    let list = Command::new(bin())
        .args(["lab", "scenario", "list", "--lab-dir", &lab_dir()])
        .output()
        .expect("run lab scenario list");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&list.stdout),
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(list.status.success(), "lab scenario list failed:\n{out}");
    assert!(
        out.contains("rt-filter"),
        "catalog must list Rich Transform filter Scenario, got:\n{out}"
    );
    assert!(
        out.to_ascii_lowercase().contains("filter"),
        "rt-filter summary should mention filter, got:\n{out}"
    );
}

#[tokio::test]
async fn lab_scenario_list_includes_idempotent_redelivery() {
    let list = Command::new(bin())
        .args(["lab", "scenario", "list", "--lab-dir", &lab_dir()])
        .output()
        .expect("run lab scenario list");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&list.stdout),
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(list.status.success(), "lab scenario list failed:\n{out}");
    assert!(
        out.contains("idempotent-redelivery"),
        "catalog must list idempotent-redelivery Lab Scenario (#86), got:\n{out}"
    );
    let lower = out.to_ascii_lowercase();
    assert!(
        lower.contains("idempotent") || lower.contains("duplicate-safe") || lower.contains("re-delivery"),
        "idempotent-redelivery summary should mention idempotent/duplicate-safe re-delivery, got:\n{out}"
    );
}

#[tokio::test]
async fn lab_scenario_list_includes_pause_resume() {
    let list = Command::new(bin())
        .args(["lab", "scenario", "list", "--lab-dir", &lab_dir()])
        .output()
        .expect("run lab scenario list");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&list.stdout),
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(list.status.success(), "lab scenario list failed:\n{out}");
    assert!(
        out.contains("pause-resume"),
        "catalog must list pause-resume Lab Scenario (#19), got:\n{out}"
    );
    let lower = out.to_ascii_lowercase();
    assert!(
        lower.contains("pause") && lower.contains("resume"),
        "pause-resume summary should mention pause and resume, got:\n{out}"
    );
}

#[tokio::test]
async fn lab_scenario_list_includes_remove_pipeline() {
    let list = Command::new(bin())
        .args(["lab", "scenario", "list", "--lab-dir", &lab_dir()])
        .output()
        .expect("run lab scenario list");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&list.stdout),
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(list.status.success(), "lab scenario list failed:\n{out}");
    assert!(
        out.contains("remove-pipeline"),
        "catalog must list remove-pipeline Lab Scenario (#20), got:\n{out}"
    );
    let lower = out.to_ascii_lowercase();
    assert!(
        lower.contains("remove") && (lower.contains("shared") || lower.contains("delivery")),
        "remove-pipeline summary should mention remove and Shared Base/Delivery, got:\n{out}"
    );
}

#[tokio::test]
async fn lab_scenario_list_includes_change_pipeline() {
    let list = Command::new(bin())
        .args(["lab", "scenario", "list", "--lab-dir", &lab_dir()])
        .output()
        .expect("run lab scenario list");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&list.stdout),
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(list.status.success(), "lab scenario list failed:\n{out}");
    assert!(
        out.contains("change-pipeline"),
        "catalog must list change-pipeline Lab Scenario (#21), got:\n{out}"
    );
    let lower = out.to_ascii_lowercase();
    assert!(
        lower.contains("revision")
            && (lower.contains("rebuild") || lower.contains("metadata")),
        "change-pipeline summary should mention revision rebuild / metadata-only, got:\n{out}"
    );
}

#[tokio::test]
async fn lab_scenario_list_includes_poison_quarantine() {
    let list = Command::new(bin())
        .args(["lab", "scenario", "list", "--lab-dir", &lab_dir()])
        .output()
        .expect("run lab scenario list");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&list.stdout),
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(list.status.success(), "lab scenario list failed:\n{out}");
    assert!(
        out.contains("poison-quarantine"),
        "catalog must list poison-quarantine Lab Scenario (#22), got:\n{out}"
    );
    let lower = out.to_ascii_lowercase();
    assert!(
        lower.contains("quarantine")
            && (lower.contains("poison") || lower.contains("unhealthy")),
        "poison-quarantine summary should mention quarantine/poison/unhealthy, got:\n{out}"
    );
}

#[tokio::test]
async fn lab_scenario_list_includes_schema_change_pause() {
    let list = Command::new(bin())
        .args(["lab", "scenario", "list", "--lab-dir", &lab_dir()])
        .output()
        .expect("run lab scenario list");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&list.stdout),
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(list.status.success(), "lab scenario list failed:\n{out}");
    assert!(
        out.contains("schema-change-pause"),
        "catalog must list schema-change-pause Lab Scenario (#23), got:\n{out}"
    );
    let lower = out.to_ascii_lowercase();
    assert!(
        (lower.contains("schema") || lower.contains("ddl"))
            && (lower.contains("pause") || lower.contains("warn")),
        "schema-change-pause summary should mention schema/DDL warn+pause, got:\n{out}"
    );
}

#[tokio::test]
async fn lab_scenario_list_includes_source_alignment() {
    let list = Command::new(bin())
        .args(["lab", "scenario", "list", "--lab-dir", &lab_dir()])
        .output()
        .expect("run lab scenario list");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&list.stdout),
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(list.status.success(), "lab scenario list failed:\n{out}");
    assert!(
        out.contains("source-alignment"),
        "catalog must list source-alignment Lab Scenario (#24), got:\n{out}"
    );
    let lower = out.to_ascii_lowercase();
    assert!(
        lower.contains("align")
            && (lower.contains("repair") || lower.contains("resource") || lower.contains("max-rows")),
        "source-alignment summary should mention align/repair/resource-gate, got:\n{out}"
    );
}

#[tokio::test]
async fn lab_scenario_list_includes_drift_check() {
    let list = Command::new(bin())
        .args(["lab", "scenario", "list", "--lab-dir", &lab_dir()])
        .output()
        .expect("run lab scenario list");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&list.stdout),
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(list.status.success(), "lab scenario list failed:\n{out}");
    assert!(
        out.contains("drift-check"),
        "catalog must list drift-check Lab Scenario (#25), got:\n{out}"
    );
    let lower = out.to_ascii_lowercase();
    assert!(
        lower.contains("drift")
            && (lower.contains("managed") || lower.contains("repair") || lower.contains("auto")),
        "drift-check summary should mention Managed drift/repair, got:\n{out}"
    );
}

/// Issue #66: gaps / catalog-complete status must be visible on `scenario list`.
#[tokio::test]
async fn lab_scenario_list_reports_catalog_complete_for_shipped_capabilities() {
    let list = Command::new(bin())
        .args(["lab", "scenario", "list", "--lab-dir", &lab_dir()])
        .output()
        .expect("run lab scenario list");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&list.stdout),
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(list.status.success(), "lab scenario list failed:\n{out}");
    assert!(
        out.contains("Catalog coverage: complete for shipped capabilities"),
        "repo Lab catalog must claim complete only when shipped capabilities are covered, got:\n{out}"
    );
    assert!(
        out.contains("COVERAGE.md") || out.contains("ADR-0025"),
        "list should point operators at coverage policy, got:\n{out}"
    );
    assert!(
        !out.contains("Catalog coverage: incomplete"),
        "repo Lab must not report incomplete shipped coverage, got:\n{out}"
    );
}

#[tokio::test]
async fn lab_scenario_list_shows_gaps_when_shipped_scenarios_missing() {
    let dir = tempfile::tempdir().expect("temp lab dir");
    let lab = dir.path();
    fs::write(lab.join("compose.yaml"), "name: migraloop-lab-coverage-gaps\n")
        .expect("write stub compose.yaml");
    // Only one shipped Scenario package — gaps for the rest must be listed.
    write_scenario_package(lab, "direct-pipeline", "partial catalog probe");

    let list = Command::new(bin())
        .args([
            "lab",
            "scenario",
            "list",
            "--lab-dir",
            &lab.to_string_lossy(),
        ])
        .output()
        .expect("run lab scenario list");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&list.stdout),
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(list.status.success(), "lab scenario list failed:\n{out}");
    assert!(
        out.contains("Catalog coverage: incomplete"),
        "partial catalog must surface incomplete coverage, got:\n{out}"
    );
    assert!(
        out.contains("missing:") && out.to_ascii_lowercase().contains("project"),
        "gaps must name missing Rich Transform project coverage, got:\n{out}"
    );
    assert!(
        out.contains("Do not claim catalog-complete"),
        "incomplete catalog must refuse catalog-complete claim, got:\n{out}"
    );
}

/// CLI seam (#65): `scenario list` reflects recipe.yaml packages under `--lab-dir`,
/// not a hardcoded summary table divorced from the Scenario catalog on disk.
#[tokio::test]
async fn lab_scenario_list_reads_recipe_summaries_from_lab_dir() {
    let dir = tempfile::tempdir().expect("temp lab dir");
    let lab = dir.path();
    fs::write(lab.join("compose.yaml"), "name: migraloop-lab-recipe-list\n")
        .expect("write stub compose.yaml");
    write_scenario_package(
        lab,
        "direct-pipeline",
        "FEATURE-TIME-AUTHORING-PROBE Direct Pipeline recipe",
    );

    let list = Command::new(bin())
        .args([
            "lab",
            "scenario",
            "list",
            "--lab-dir",
            &lab.to_string_lossy(),
        ])
        .output()
        .expect("run lab scenario list");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&list.stdout),
        String::from_utf8_lossy(&list.stderr)
    );
    assert!(list.status.success(), "lab scenario list failed:\n{out}");
    assert!(
        out.contains("FEATURE-TIME-AUTHORING-PROBE"),
        "list must use recipe.yaml summary from --lab-dir, got:\n{out}"
    );
    assert!(
        out.contains("direct-pipeline"),
        "list must include the recipe id, got:\n{out}"
    );
    // Other registered runners without a recipe package under this lab-dir are not selectable.
    assert!(
        !out.contains("bulk-load"),
        "list must not invent catalog entries without recipe.yaml, got:\n{out}"
    );
    assert!(
        !out.contains("transform-pipeline"),
        "list must not invent catalog entries without recipe.yaml, got:\n{out}"
    );
}

/// CLI-seam metrics-fail: threshold failure fails the Scenario while correctness would pass.
#[tokio::test]
async fn lab_scenario_bulk_load_threshold_fail_via_cli_probe() {
    let (_tmp, lab) = temp_lab_dir_with_recipes(&["bulk-load"]);
    let run = Command::new(bin())
        .env("MIGRALOOP_LAB_SCENARIO_OUTCOME_PROBE", "threshold-fail")
        .args(["lab", "scenario", "run", "bulk-load", "--lab-dir", &lab])
        .output()
        .expect("run bulk-load threshold-fail probe");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(
        !run.status.success(),
        "threshold-fail probe must fail the Scenario run, got:\n{out}"
    );
    assert!(
        out.contains("Lab Scenario: FAIL"),
        "expected FAIL report, got:\n{out}"
    );
    assert!(
        out.contains("correctness=pass"),
        "metrics-fail must keep correctness=pass, got:\n{out}"
    );
    assert!(
        out.contains("thresholds=fail"),
        "expected thresholds=fail, got:\n{out}"
    );
    assert!(
        out.contains("lag=") && out.contains("duration_ms=") && out.contains("rows_per_s="),
        "expected lag/throughput/duration metrics, got:\n{out}"
    );
    assert!(
        out.contains("Lab Scenario threshold failed"),
        "US36 must name threshold failure, got:\n{out}"
    );
    assert!(
        out.contains("namespace=left in place"),
        "failed run must keep Namespace, got:\n{out}"
    );
}

/// CLI-seam correctness-fail: row-level miss fails even when metrics would pass.
#[tokio::test]
async fn lab_scenario_bulk_load_correctness_fail_via_cli_probe() {
    let (_tmp, lab) = temp_lab_dir_with_recipes(&["bulk-load"]);
    let run = Command::new(bin())
        .env("MIGRALOOP_LAB_SCENARIO_OUTCOME_PROBE", "correctness-fail")
        .args(["lab", "scenario", "run", "bulk-load", "--lab-dir", &lab])
        .output()
        .expect("run bulk-load correctness-fail probe");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(
        !run.status.success(),
        "correctness-fail probe must fail the Scenario run, got:\n{out}"
    );
    assert!(
        out.contains("Lab Scenario: FAIL"),
        "expected FAIL report, got:\n{out}"
    );
    assert!(
        out.contains("correctness=fail"),
        "expected correctness=fail, got:\n{out}"
    );
    assert!(
        out.contains("thresholds=pass"),
        "correctness-fail must keep thresholds=pass when metrics would pass, got:\n{out}"
    );
    assert!(
        out.contains("Lab Scenario correctness failed"),
        "US36 must name correctness failure, got:\n{out}"
    );
    assert!(
        out.contains("detail=correctness:"),
        "expected correctness detail, got:\n{out}"
    );
}

#[tokio::test]
async fn lab_scenario_run_unknown_id_fails() {
    let run = Command::new(bin())
        .args([
            "lab",
            "scenario",
            "run",
            "not-a-real-scenario",
            "--lab-dir",
            &lab_dir(),
        ])
        .output()
        .expect("run unknown scenario");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(
        !run.status.success(),
        "unknown scenario should fail, got success:\n{out}"
    );
    assert!(
        out.to_ascii_lowercase().contains("unknown")
            || out.contains("not-a-real-scenario")
            || out.contains("Lab Scenario"),
        "expected clear unknown-scenario error, got:\n{out}"
    );
}

/// US44 / issue #85: Lab Scenario run refuses non-Lab / production-looking Source hosts.
#[tokio::test]
async fn lab_scenario_run_rejects_non_lab_source_engine() {
    let (tmp, lab) = temp_lab_dir();
    write_scenario_package_with_deployment(
        tmp.path(),
        "direct-pipeline",
        "isolation probe — production Source",
        &lab_fixture_deployment_yaml("direct-pipeline", "prod-oracle.example.com", "127.0.0.1"),
    );

    let run = Command::new(bin())
        .args(["lab", "scenario", "run", "direct-pipeline", "--lab-dir", &lab])
        .output()
        .expect("run with non-Lab Source");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );

    assert!(
        !run.status.success(),
        "non-Lab Source must fail the Scenario run, got:\n{out}"
    );
    let lower = out.to_ascii_lowercase();
    assert!(
        lower.contains("lab")
            && (lower.contains("refused")
                || lower.contains("reject")
                || lower.contains("production")
                || lower.contains("non-lab")
                || lower.contains("lab-provisioned")
                || lower.contains("fixture")),
        "error must make Lab isolation rule obvious, got:\n{out}"
    );
    assert!(
        out.contains("prod-oracle.example.com"),
        "rejection should name the non-Lab Source host, got:\n{out}"
    );
    assert!(
        !lower.contains("docker") && !lower.contains("compose"),
        "engine isolation must fail before Fixture/Docker probes, got:\n{out}"
    );
}

/// US44 / issue #85: Lab Scenario run refuses non-Lab / production-looking Target hosts.
#[tokio::test]
async fn lab_scenario_run_rejects_non_lab_target_engine() {
    let (tmp, lab) = temp_lab_dir();
    write_scenario_package_with_deployment(
        tmp.path(),
        "direct-pipeline",
        "isolation probe — production Target",
        &lab_fixture_deployment_yaml("direct-pipeline", "127.0.0.1", "prod-mongo.example.com"),
    );

    let run = Command::new(bin())
        .args(["lab", "scenario", "run", "direct-pipeline", "--lab-dir", &lab])
        .output()
        .expect("run with non-Lab Target");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );

    assert!(
        !run.status.success(),
        "non-Lab Target must fail the Scenario run, got:\n{out}"
    );
    let lower = out.to_ascii_lowercase();
    assert!(
        lower.contains("lab")
            && (lower.contains("refused")
                || lower.contains("reject")
                || lower.contains("production")
                || lower.contains("non-lab")
                || lower.contains("lab-provisioned")
                || lower.contains("fixture")),
        "error must make Lab isolation rule obvious, got:\n{out}"
    );
    assert!(
        out.contains("prod-mongo.example.com"),
        "rejection should name the non-Lab Target host, got:\n{out}"
    );
}

/// US44 / issue #85: disposable Lab Fixture engine bindings still pass the isolation guard.
#[tokio::test]
async fn lab_scenario_run_allows_lab_fixture_engines_past_isolation_guard() {
    let (tmp, lab) = temp_lab_dir();
    write_scenario_package_with_deployment(
        tmp.path(),
        "direct-pipeline",
        "isolation probe — Lab Fixture engines",
        &lab_fixture_deployment_yaml("direct-pipeline", "127.0.0.1", "127.0.0.1"),
    );

    let run = Command::new(bin())
        .args(["lab", "scenario", "run", "direct-pipeline", "--lab-dir", &lab])
        .output()
        .expect("run with Lab Fixture engines");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );

    let lower = out.to_ascii_lowercase();
    assert!(
        !lower.contains("prod-")
            && !lower.contains("customer/production")
            && !(lower.contains("refused") && lower.contains("engine")),
        "Lab Fixture engines must not hit the isolation refusal, got:\n{out}"
    );
    // Without Docker the run still fails later on Fixture readiness — that is expected.
    assert!(
        !run.status.success(),
        "stub lab-dir without Docker should still fail after the isolation guard, got:\n{out}"
    );
    assert!(
        lower.contains("docker")
            || lower.contains("compose")
            || lower.contains("fixture")
            || lower.contains("not ready")
            || lower.contains("ready"),
        "expected Fixture readiness failure after isolation guard, got:\n{out}"
    );
}

#[tokio::test]
async fn lab_scenario_run_rejects_when_another_is_active() {
    let (_tmp, lab) = temp_lab_dir_with_recipes(&["direct-pipeline"]);
    let lock_path = format!("{lab}/.migraloop-scenario.lock");
    let pid = std::process::id();
    let started = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    fs::write(
        &lock_path,
        format!(
            "{{\"scenario\":\"direct-pipeline\",\"pid\":{pid},\"started_at_unix\":{started}}}\n"
        ),
    )
    .expect("write scenario lock");

    let run = Command::new(bin())
        .args(["lab", "scenario", "run", "direct-pipeline", "--lab-dir", &lab])
        .output()
        .expect("run with active lock");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );

    assert!(
        !run.status.success(),
        "second Scenario run should be rejected while one is active, got:\n{out}"
    );
    assert!(
        out.contains("rejected") || out.to_ascii_lowercase().contains("active"),
        "expected one-at-a-time rejection message, got:\n{out}"
    );
    assert!(
        out.contains("direct-pipeline"),
        "rejection should name the active Scenario, got:\n{out}"
    );
}

#[tokio::test]
async fn lab_scenario_help_lists_remove() {
    let help = Command::new(bin())
        .args(["lab", "scenario", "--help"])
        .output()
        .expect("run lab scenario --help");
    assert!(
        help.status.success(),
        "lab scenario --help failed: {}",
        String::from_utf8_lossy(&help.stderr)
    );
    let stdout = String::from_utf8_lossy(&help.stdout);
    assert!(
        stdout.contains("remove"),
        "lab scenario --help should list `remove`, got:\n{stdout}"
    );
}

#[tokio::test]
async fn lab_scenario_run_help_lists_auto_remove() {
    let help = Command::new(bin())
        .args(["lab", "scenario", "run", "--help"])
        .output()
        .expect("run lab scenario run --help");
    assert!(
        help.status.success(),
        "lab scenario run --help failed: {}",
        String::from_utf8_lossy(&help.stderr)
    );
    let stdout = String::from_utf8_lossy(&help.stdout);
    assert!(
        stdout.contains("--auto-remove"),
        "lab scenario run --help should list --auto-remove, got:\n{stdout}"
    );
}

#[tokio::test]
async fn lab_scenario_remove_unknown_id_fails() {
    let remove = Command::new(bin())
        .args([
            "lab",
            "scenario",
            "remove",
            "not-a-real-scenario",
            "--lab-dir",
            &lab_dir(),
        ])
        .output()
        .expect("remove unknown scenario");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&remove.stdout),
        String::from_utf8_lossy(&remove.stderr)
    );
    assert!(
        !remove.status.success(),
        "unknown scenario remove should fail, got success:\n{out}"
    );
    assert!(
        out.to_ascii_lowercase().contains("unknown")
            || out.contains("not-a-real-scenario")
            || out.contains("Lab Scenario"),
        "expected clear unknown-scenario error, got:\n{out}"
    );
}

#[tokio::test]
async fn lab_scenario_remove_rejects_when_another_is_active() {
    let (_tmp, lab) = temp_lab_dir_with_recipes(&["direct-pipeline"]);
    let lock_path = format!("{lab}/.migraloop-scenario.lock");
    let pid = std::process::id();
    let started = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    fs::write(
        &lock_path,
        format!(
            "{{\"scenario\":\"direct-pipeline\",\"pid\":{pid},\"started_at_unix\":{started}}}\n"
        ),
    )
    .expect("write scenario lock");

    let remove = Command::new(bin())
        .args([
            "lab",
            "scenario",
            "remove",
            "direct-pipeline",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("remove with active lock");
    let out = format!(
        "{}{}",
        String::from_utf8_lossy(&remove.stdout),
        String::from_utf8_lossy(&remove.stderr)
    );

    assert!(
        !remove.status.success(),
        "scenario remove should be rejected while a run is active, got:\n{out}"
    );
    assert!(
        out.contains("rejected") || out.to_ascii_lowercase().contains("active"),
        "expected one-at-a-time rejection message, got:\n{out}"
    );
}

/// Full Direct Pipeline Lab Scenario against Docker Lab Fixture + Instant Client.
///
/// ```bash
/// export LD_LIBRARY_PATH=/path/to/instantclient
/// cargo test -p migraloop-app --test cli_lab_scenario -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "requires Docker Compose Lab Fixture + Oracle Instant Client; not part of Release Quality Gate"]
async fn lab_scenario_direct_pipeline_run_and_inspect() {
    let lab = lab_dir();
    let store_url = "postgres://migraloop:migraloop@127.0.0.1:5432/migraloop";

    let down_first = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output()
        .expect("lab down cleanup");
    assert!(
        down_first.status.success(),
        "lab down (cleanup) failed: {}",
        String::from_utf8_lossy(&down_first.stderr)
    );

    let up = Command::new(bin())
        .args(["lab", "up", "--lab-dir", &lab])
        .output()
        .expect("lab up");
    let up_out = format!(
        "{}{}",
        String::from_utf8_lossy(&up.stdout),
        String::from_utf8_lossy(&up.stderr)
    );
    assert!(up.status.success(), "lab up failed:\n{up_out}");

    let run = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args(["lab", "scenario", "run", "direct-pipeline", "--lab-dir", &lab])
        .output()
        .expect("lab scenario run");
    let run_out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(
        run.status.success(),
        "lab scenario run failed:\n{run_out}"
    );
    assert!(
        run_out.contains("Lab Scenario: PASS"),
        "expected Scenario PASS, got:\n{run_out}"
    );
    assert!(
        run_out.contains("duration_ms="),
        "expected duration metric, got:\n{run_out}"
    );
    assert!(
        run_out.contains("rows_per_s=")
            || run_out.contains("throughput")
            || run_out.contains("rows_applied="),
        "expected rows/throughput metric, got:\n{run_out}"
    );
    assert!(
        run_out.to_ascii_lowercase().contains("logminer")
            || run_out.contains("Incremental Capture"),
        "Scenario must use real capture path, got:\n{run_out}"
    );

    // Namespace left in place: Base / Target still inspectable after the run.
    let base = Command::new(bin())
        .args([
            "base",
            "--platform-store-url",
            store_url,
            "--table",
            "LAB_DP_CUSTOMERS",
        ])
        .output()
        .expect("base inspect");
    let base_out = String::from_utf8_lossy(&base.stdout);
    assert!(
        base.status.success(),
        "base inspect failed: {}",
        String::from_utf8_lossy(&base.stderr)
    );
    assert!(
        base_out.contains("Alicia") && base_out.contains("Carol") && !base_out.contains("Bob"),
        "Base must reflect insert/update/delete after Scenario, got:\n{base_out}"
    );

    let target = Command::new(bin())
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "target",
            "--platform-store-url",
            store_url,
            "--collection",
            "lab_dp_customers",
        ])
        .output()
        .expect("target inspect");
    let target_out = String::from_utf8_lossy(&target.stdout);
    assert!(
        target.status.success(),
        "target inspect failed: {}",
        String::from_utf8_lossy(&target.stderr)
    );
    assert!(
        target_out.contains("Alicia")
            && target_out.contains("Carol")
            && !target_out.contains("Bob"),
        "Target Managed outcomes must match Scenario workload, got:\n{target_out}"
    );

    // Concurrent rejection while we fake an active lock after a finished run still works.
    // (Finished run releases the lock; Namespace remains.)
    let lock_path = format!("{lab}/.migraloop-scenario.lock");
    assert!(
        !std::path::Path::new(&lock_path).exists(),
        "finished Scenario must release the active-run lock (Namespace stays)"
    );
    assert!(
        run_out.contains("namespace=left in place")
            || run_out.to_ascii_lowercase().contains("left in place"),
        "default completed run must keep Namespace, got:\n{run_out}"
    );

    // Issue #84: after a finished run, lab status names the leftover Namespace
    // (and not an active run) without forcing operators to guess from Deployment lines.
    let leftover_status = Command::new(bin())
        .args(["lab", "status", "--lab-dir", &lab])
        .output()
        .expect("lab status after scenario keep");
    let leftover_out = format!(
        "{}{}",
        String::from_utf8_lossy(&leftover_status.stdout),
        String::from_utf8_lossy(&leftover_status.stderr)
    );
    assert!(
        leftover_status.status.success(),
        "lab status after Scenario keep failed:\n{leftover_out}"
    );
    assert!(
        leftover_out.contains("Scenario run: (none)"),
        "finished Scenario must not report an active run, got:\n{leftover_out}"
    );
    assert!(
        leftover_out.contains("Scenario Namespace leftover: direct-pipeline"),
        "lab status must name leftover Scenario Namespace, got:\n{leftover_out}"
    );

    // Re-run same Scenario: full Namespace remove before recreate must succeed.
    let rerun = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args(["lab", "scenario", "run", "direct-pipeline", "--lab-dir", &lab])
        .output()
        .expect("lab scenario re-run");
    let rerun_out = format!(
        "{}{}",
        String::from_utf8_lossy(&rerun.stdout),
        String::from_utf8_lossy(&rerun.stderr)
    );
    assert!(
        rerun.status.success(),
        "lab scenario re-run should wipe Namespace then succeed:\n{rerun_out}"
    );
    assert!(
        rerun_out.contains("Lab Scenario: PASS"),
        "expected re-run PASS, got:\n{rerun_out}"
    );

    // Manual remove clears Namespace without starting a run.
    let remove = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "lab",
            "scenario",
            "remove",
            "direct-pipeline",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("lab scenario remove");
    let remove_out = format!(
        "{}{}",
        String::from_utf8_lossy(&remove.stdout),
        String::from_utf8_lossy(&remove.stderr)
    );
    assert!(
        remove.status.success(),
        "lab scenario remove failed:\n{remove_out}"
    );
    assert!(
        remove_out.to_ascii_lowercase().contains("removed")
            || remove_out.contains("Namespace"),
        "manual remove should report Namespace removal, got:\n{remove_out}"
    );

    let base_gone = Command::new(bin())
        .args([
            "base",
            "--platform-store-url",
            store_url,
            "--table",
            "LAB_DP_CUSTOMERS",
        ])
        .output()
        .expect("base after remove");
    let base_gone_out = format!(
        "{}{}",
        String::from_utf8_lossy(&base_gone.stdout),
        String::from_utf8_lossy(&base_gone.stderr)
    );
    assert!(
        !base_gone.status.success()
            || (!base_gone_out.contains("Alicia") && !base_gone_out.contains("Carol")),
        "Base Namespace rows should be gone after manual remove, got:\n{base_gone_out}"
    );

    // Opt-in auto-remove: completed run deletes Namespace.
    let auto = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "lab",
            "scenario",
            "run",
            "direct-pipeline",
            "--auto-remove",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("lab scenario run --auto-remove");
    let auto_out = format!(
        "{}{}",
        String::from_utf8_lossy(&auto.stdout),
        String::from_utf8_lossy(&auto.stderr)
    );
    assert!(
        auto.status.success(),
        "lab scenario run --auto-remove failed:\n{auto_out}"
    );
    assert!(
        auto_out.contains("Lab Scenario: PASS"),
        "expected auto-remove run PASS, got:\n{auto_out}"
    );
    assert!(
        auto_out.contains("namespace=removed")
            || auto_out.to_ascii_lowercase().contains("auto-remove"),
        "auto-remove run should report Namespace removed, got:\n{auto_out}"
    );

    let base_auto = Command::new(bin())
        .args([
            "base",
            "--platform-store-url",
            store_url,
            "--table",
            "LAB_DP_CUSTOMERS",
        ])
        .output()
        .expect("base after auto-remove");
    let base_auto_out = format!(
        "{}{}",
        String::from_utf8_lossy(&base_auto.stdout),
        String::from_utf8_lossy(&base_auto.stderr)
    );
    assert!(
        !base_auto.status.success()
            || (!base_auto_out.contains("Alicia") && !base_auto_out.contains("Carol")),
        "Base Namespace rows should be gone after --auto-remove, got:\n{base_auto_out}"
    );

    let _ = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output();
}

/// Full multi-table Transform Pipeline Lab Scenario against Docker Lab Fixture + Instant Client.
///
/// ```bash
/// export LD_LIBRARY_PATH=/path/to/instantclient
/// cargo test -p migraloop-app --test cli_lab_scenario -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "requires Docker Compose Lab Fixture + Oracle Instant Client; not part of Release Quality Gate"]
async fn lab_scenario_transform_pipeline_run_and_inspect() {
    let lab = lab_dir();
    let store_url = "postgres://migraloop:migraloop@127.0.0.1:5432/migraloop";

    let down_first = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output()
        .expect("lab down cleanup");
    assert!(
        down_first.status.success(),
        "lab down (cleanup) failed: {}",
        String::from_utf8_lossy(&down_first.stderr)
    );

    let up = Command::new(bin())
        .args(["lab", "up", "--lab-dir", &lab])
        .output()
        .expect("lab up");
    let up_out = format!(
        "{}{}",
        String::from_utf8_lossy(&up.stdout),
        String::from_utf8_lossy(&up.stderr)
    );
    assert!(up.status.success(), "lab up failed:\n{up_out}");

    let run = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "lab",
            "scenario",
            "run",
            "transform-pipeline",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("lab scenario run transform-pipeline");
    let run_out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(
        run.status.success(),
        "lab scenario run transform-pipeline failed:\n{run_out}"
    );
    assert!(
        run_out.contains("Lab Scenario: PASS"),
        "expected Scenario PASS, got:\n{run_out}"
    );
    assert!(
        run_out.contains("duration_ms="),
        "expected duration metric, got:\n{run_out}"
    );
    assert!(
        run_out.contains("rows_per_s=")
            || run_out.contains("throughput")
            || run_out.contains("rows_applied="),
        "expected rows/throughput metric, got:\n{run_out}"
    );
    assert!(
        run_out.contains("correctness=pass"),
        "expected correctness metric alongside operational metrics, got:\n{run_out}"
    );
    assert!(
        run_out.to_ascii_lowercase().contains("logminer")
            || run_out.contains("Incremental Capture"),
        "Scenario must use real capture path, got:\n{run_out}"
    );
    assert!(
        run_out.contains("namespace=left in place")
            || run_out.to_ascii_lowercase().contains("left in place"),
        "default completed run must keep Namespace, got:\n{run_out}"
    );

    // Namespace left in place: Base / Derived / Target still inspectable after the run.
    let customers_base = Command::new(bin())
        .args([
            "base",
            "--platform-store-url",
            store_url,
            "--table",
            "LAB_TP_CUSTOMERS",
        ])
        .output()
        .expect("customers base inspect");
    let customers_base_out = String::from_utf8_lossy(&customers_base.stdout);
    assert!(
        customers_base.status.success(),
        "customers base inspect failed: {}",
        String::from_utf8_lossy(&customers_base.stderr)
    );
    assert!(
        customers_base_out.contains("Alicia")
            && customers_base_out.contains("Carol")
            && !customers_base_out.contains("Bob"),
        "customers Base must reflect multi-table workload, got:\n{customers_base_out}"
    );

    let orders_base = Command::new(bin())
        .args([
            "base",
            "--platform-store-url",
            store_url,
            "--table",
            "LAB_TP_ORDERS",
        ])
        .output()
        .expect("orders base inspect");
    assert!(
        orders_base.status.success(),
        "orders base inspect failed: {}",
        String::from_utf8_lossy(&orders_base.stderr)
    );

    let derived = Command::new(bin())
        .args([
            "derived",
            "--platform-store-url",
            store_url,
            "--pipeline",
            "lab-tp-order-totals",
        ])
        .output()
        .expect("derived inspect");
    let derived_out = String::from_utf8_lossy(&derived.stdout);
    assert!(
        derived.status.success(),
        "derived inspect failed: {}",
        String::from_utf8_lossy(&derived.stderr)
    );
    assert!(
        (derived_out.contains("35") || derived_out.contains("35.00"))
            && (derived_out.contains("50") || derived_out.contains("50.00")),
        "Derived Managed totals must match Transform workload (35 and 50), got:\n{derived_out}"
    );

    let customers_target = Command::new(bin())
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "target",
            "--platform-store-url",
            store_url,
            "--collection",
            "lab_tp_customers",
        ])
        .output()
        .expect("customers target inspect");
    let customers_target_out = String::from_utf8_lossy(&customers_target.stdout);
    assert!(
        customers_target.status.success(),
        "customers target inspect failed: {}",
        String::from_utf8_lossy(&customers_target.stderr)
    );
    assert!(
        customers_target_out.contains("Alicia")
            && customers_target_out.contains("Carol")
            && !customers_target_out.contains("Bob"),
        "customers Target Managed outcomes must match Scenario workload, got:\n{customers_target_out}"
    );

    let totals_target = Command::new(bin())
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "target",
            "--platform-store-url",
            store_url,
            "--collection",
            "lab_tp_order_totals",
        ])
        .output()
        .expect("order totals target inspect");
    let totals_target_out = String::from_utf8_lossy(&totals_target.stdout);
    assert!(
        totals_target.status.success(),
        "order totals target inspect failed: {}",
        String::from_utf8_lossy(&totals_target.stderr)
    );
    assert!(
        (totals_target_out.contains("35") || totals_target_out.contains("35.00"))
            && (totals_target_out.contains("50") || totals_target_out.contains("50.00")),
        "order totals Target must Deliver Derived Managed outcomes, got:\n{totals_target_out}"
    );

    let lock_path = format!("{lab}/.migraloop-scenario.lock");
    assert!(
        !std::path::Path::new(&lock_path).exists(),
        "finished Scenario must release the active-run lock (Namespace stays)"
    );

    // Manual remove clears multi-table Namespace without starting a run.
    let remove = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "lab",
            "scenario",
            "remove",
            "transform-pipeline",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("lab scenario remove transform-pipeline");
    let remove_out = format!(
        "{}{}",
        String::from_utf8_lossy(&remove.stdout),
        String::from_utf8_lossy(&remove.stderr)
    );
    assert!(
        remove.status.success(),
        "lab scenario remove transform-pipeline failed:\n{remove_out}"
    );

    let _ = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output();
}

/// Full intra-Scenario concurrent Source workload Lab Scenario against Docker Lab
/// Fixture + Instant Client (issue #64).
///
/// ```bash
/// export LD_LIBRARY_PATH=/path/to/instantclient
/// cargo test -p migraloop-app --test cli_lab_scenario -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "requires Docker Compose Lab Fixture + Oracle Instant Client; not part of Release Quality Gate"]
async fn lab_scenario_concurrent_source_workload_run_and_inspect() {
    let lab = lab_dir();
    let store_url = "postgres://migraloop:migraloop@127.0.0.1:5432/migraloop";

    let down_first = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output()
        .expect("lab down cleanup");
    assert!(
        down_first.status.success(),
        "lab down (cleanup) failed: {}",
        String::from_utf8_lossy(&down_first.stderr)
    );

    let up = Command::new(bin())
        .args(["lab", "up", "--lab-dir", &lab])
        .output()
        .expect("lab up");
    let up_out = format!(
        "{}{}",
        String::from_utf8_lossy(&up.stdout),
        String::from_utf8_lossy(&up.stderr)
    );
    assert!(up.status.success(), "lab up failed:\n{up_out}");

    let run = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "lab",
            "scenario",
            "run",
            "concurrent-source-workload",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("lab scenario run concurrent-source-workload");
    let run_out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(
        run.status.success(),
        "lab scenario run concurrent-source-workload failed:\n{run_out}"
    );
    assert!(
        run_out.contains("Lab Scenario: PASS"),
        "expected Scenario PASS, got:\n{run_out}"
    );
    assert!(
        run_out.contains("correctness=pass"),
        "expected correctness metric alongside operational metrics, got:\n{run_out}"
    );
    assert!(
        run_out.contains("thresholds=pass"),
        "expected equal-weight threshold metric, got:\n{run_out}"
    );
    assert!(
        run_out.contains("settle_ms=") && run_out.contains("max_settle_ms="),
        "expected settle threshold metrics, got:\n{run_out}"
    );
    assert!(
        run_out.contains("duration_ms="),
        "expected duration metric, got:\n{run_out}"
    );
    assert!(
        run_out.contains("rows_per_s=")
            || run_out.contains("throughput")
            || run_out.contains("rows_applied="),
        "expected rows/throughput metric, got:\n{run_out}"
    );
    assert!(
        run_out.to_ascii_lowercase().contains("logminer")
            || run_out.contains("Incremental Capture"),
        "Scenario must use real capture path, got:\n{run_out}"
    );
    assert!(
        run_out.contains("parallel")
            || run_out.to_ascii_lowercase().contains("concurrent"),
        "Scenario output should describe intra-Scenario concurrent Source workload, got:\n{run_out}"
    );
    assert!(
        run_out.contains("namespace=left in place")
            || run_out.to_ascii_lowercase().contains("left in place"),
        "default completed run must keep Namespace, got:\n{run_out}"
    );

    let customers_base = Command::new(bin())
        .args([
            "base",
            "--platform-store-url",
            store_url,
            "--table",
            "LAB_CW_CUSTOMERS",
        ])
        .output()
        .expect("customers base inspect");
    let customers_base_out = String::from_utf8_lossy(&customers_base.stdout);
    assert!(
        customers_base.status.success(),
        "customers base inspect failed: {}",
        String::from_utf8_lossy(&customers_base.stderr)
    );
    assert!(
        customers_base_out.contains("Alicia")
            && customers_base_out.contains("Carol")
            && !customers_base_out.contains("Bob"),
        "customers Base must reflect concurrent workload, got:\n{customers_base_out}"
    );

    let derived = Command::new(bin())
        .args([
            "derived",
            "--platform-store-url",
            store_url,
            "--pipeline",
            "lab-cw-order-totals",
        ])
        .output()
        .expect("derived inspect");
    let derived_out = String::from_utf8_lossy(&derived.stdout);
    assert!(
        derived.status.success(),
        "derived inspect failed: {}",
        String::from_utf8_lossy(&derived.stderr)
    );
    assert!(
        (derived_out.contains("35") || derived_out.contains("35.00"))
            && (derived_out.contains("50") || derived_out.contains("50.00")),
        "Derived Managed totals must match concurrent workload (35 and 50), got:\n{derived_out}"
    );

    let totals_target = Command::new(bin())
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "target",
            "--platform-store-url",
            store_url,
            "--collection",
            "lab_cw_order_totals",
        ])
        .output()
        .expect("order totals target inspect");
    let totals_target_out = String::from_utf8_lossy(&totals_target.stdout);
    assert!(
        totals_target.status.success(),
        "order totals target inspect failed: {}",
        String::from_utf8_lossy(&totals_target.stderr)
    );
    assert!(
        (totals_target_out.contains("35") || totals_target_out.contains("35.00"))
            && (totals_target_out.contains("50") || totals_target_out.contains("50.00")),
        "order totals Target must Deliver Derived Managed outcomes, got:\n{totals_target_out}"
    );

    let lock_path = format!("{lab}/.migraloop-scenario.lock");
    assert!(
        !std::path::Path::new(&lock_path).exists(),
        "finished Scenario must release the active-run lock (Namespace stays)"
    );

    // Cross-Scenario concurrency still forbidden while faking an active lock.
    let pid = std::process::id();
    let started = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs();
    fs::write(
        &lock_path,
        format!(
            "{{\"scenario\":\"concurrent-source-workload\",\"pid\":{pid},\"started_at_unix\":{started}}}\n"
        ),
    )
    .expect("write scenario lock");
    let rejected = Command::new(bin())
        .args([
            "lab",
            "scenario",
            "run",
            "direct-pipeline",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("run while concurrent scenario lock held");
    let rejected_out = format!(
        "{}{}",
        String::from_utf8_lossy(&rejected.stdout),
        String::from_utf8_lossy(&rejected.stderr)
    );
    let _ = fs::remove_file(&lock_path);
    assert!(
        !rejected.status.success(),
        "cross-Scenario concurrency must still be rejected, got:\n{rejected_out}"
    );

    let remove = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "lab",
            "scenario",
            "remove",
            "concurrent-source-workload",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("lab scenario remove concurrent-source-workload");
    let remove_out = format!(
        "{}{}",
        String::from_utf8_lossy(&remove.stdout),
        String::from_utf8_lossy(&remove.stderr)
    );
    assert!(
        remove.status.success(),
        "lab scenario remove concurrent-source-workload failed:\n{remove_out}"
    );

    let _ = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output();
}

/// Full bulk-load Lab Scenario (~100k Source inserts + metric thresholds) against
/// Docker Lab Fixture + Instant Client (issue #63).
///
/// ```bash
/// export LD_LIBRARY_PATH=/path/to/instantclient
/// cargo test -p migraloop-app --test cli_lab_scenario -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "requires Docker Compose Lab Fixture + Oracle Instant Client; not part of Release Quality Gate"]
async fn lab_scenario_bulk_load_run_and_inspect() {
    let lab = lab_dir();
    let store_url = "postgres://migraloop:migraloop@127.0.0.1:5432/migraloop";

    let down_first = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output()
        .expect("lab down cleanup");
    assert!(
        down_first.status.success(),
        "lab down (cleanup) failed: {}",
        String::from_utf8_lossy(&down_first.stderr)
    );

    let up = Command::new(bin())
        .args(["lab", "up", "--lab-dir", &lab])
        .output()
        .expect("lab up");
    let up_out = format!(
        "{}{}",
        String::from_utf8_lossy(&up.stdout),
        String::from_utf8_lossy(&up.stderr)
    );
    assert!(up.status.success(), "lab up failed:\n{up_out}");

    let run = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args(["lab", "scenario", "run", "bulk-load", "--lab-dir", &lab])
        .output()
        .expect("lab scenario run bulk-load");
    let run_out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(
        run.status.success(),
        "lab scenario run bulk-load failed:\n{run_out}"
    );
    assert!(
        run_out.contains("Lab Scenario: PASS"),
        "expected Scenario PASS, got:\n{run_out}"
    );
    assert!(
        run_out.contains("correctness=pass"),
        "expected correctness alongside metrics, got:\n{run_out}"
    );
    assert!(
        run_out.contains("thresholds=pass"),
        "expected equal-weight thresholds, got:\n{run_out}"
    );
    assert!(
        run_out.contains("lag=") && run_out.contains("max_lag="),
        "expected lag metric + threshold, got:\n{run_out}"
    );
    assert!(
        run_out.contains("duration_ms=") && run_out.contains("max_duration_ms="),
        "expected duration metric + threshold, got:\n{run_out}"
    );
    assert!(
        run_out.contains("rows_per_s=") && run_out.contains("min_rows_per_s="),
        "expected throughput metric + threshold, got:\n{run_out}"
    );
    assert!(
        run_out.contains("100000") || run_out.contains("100k") || run_out.contains("rows_applied="),
        "expected ~100k-scale bulk volume signal, got:\n{run_out}"
    );
    assert!(
        run_out.contains("namespace=left in place")
            || run_out.to_ascii_lowercase().contains("left in place"),
        "default completed run must keep Namespace, got:\n{run_out}"
    );

    let base = Command::new(bin())
        .args([
            "base",
            "--platform-store-url",
            store_url,
            "--table",
            "LAB_BL_ITEMS",
        ])
        .output()
        .expect("base inspect");
    let base_out = String::from_utf8_lossy(&base.stdout);
    assert!(
        base.status.success(),
        "base inspect failed: {}",
        String::from_utf8_lossy(&base.stderr)
    );
    assert!(
        base_out.contains("rows=100000"),
        "Base must retain bulk-load Namespace rows, got:\n{base_out}"
    );

    let lock_path = format!("{lab}/.migraloop-scenario.lock");
    assert!(
        !std::path::Path::new(&lock_path).exists(),
        "finished Scenario must release the active-run lock (Namespace stays)"
    );

    // Re-run: wipe Namespace then recreate (keep/re-run semantics).
    let rerun = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args(["lab", "scenario", "run", "bulk-load", "--lab-dir", &lab])
        .output()
        .expect("lab scenario re-run bulk-load");
    let rerun_out = format!(
        "{}{}",
        String::from_utf8_lossy(&rerun.stdout),
        String::from_utf8_lossy(&rerun.stderr)
    );
    assert!(
        rerun.status.success(),
        "lab scenario re-run bulk-load should wipe Namespace then succeed:\n{rerun_out}"
    );
    assert!(
        rerun_out.contains("Lab Scenario: PASS"),
        "expected re-run PASS, got:\n{rerun_out}"
    );

    let remove = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "lab",
            "scenario",
            "remove",
            "bulk-load",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("lab scenario remove bulk-load");
    let remove_out = format!(
        "{}{}",
        String::from_utf8_lossy(&remove.stdout),
        String::from_utf8_lossy(&remove.stderr)
    );
    assert!(
        remove.status.success(),
        "lab scenario remove bulk-load failed:\n{remove_out}"
    );

    let _ = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output();
}

/// Full Rich Transform `project` Lab Scenario against Docker Lab Fixture + Instant Client
/// (issue #66).
///
/// ```bash
/// export LD_LIBRARY_PATH=/path/to/instantclient
/// cargo test -p migraloop-app --test cli_lab_scenario -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "requires Docker Compose Lab Fixture + Oracle Instant Client; not part of Release Quality Gate"]
async fn lab_scenario_rt_project_run_and_inspect() {
    let lab = lab_dir();
    let store_url = "postgres://migraloop:migraloop@127.0.0.1:5432/migraloop";

    let down_first = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output()
        .expect("lab down cleanup");
    assert!(
        down_first.status.success(),
        "lab down (cleanup) failed: {}",
        String::from_utf8_lossy(&down_first.stderr)
    );

    let up = Command::new(bin())
        .args(["lab", "up", "--lab-dir", &lab])
        .output()
        .expect("lab up");
    let up_out = format!(
        "{}{}",
        String::from_utf8_lossy(&up.stdout),
        String::from_utf8_lossy(&up.stderr)
    );
    assert!(up.status.success(), "lab up failed:\n{up_out}");

    let run = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args(["lab", "scenario", "run", "rt-project", "--lab-dir", &lab])
        .output()
        .expect("lab scenario run rt-project");
    let run_out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(
        run.status.success(),
        "lab scenario run rt-project failed:\n{run_out}"
    );
    assert!(
        run_out.contains("Lab Scenario: PASS"),
        "expected Scenario PASS, got:\n{run_out}"
    );
    assert!(
        run_out.to_ascii_lowercase().contains("logminer")
            || run_out.contains("Incremental Capture"),
        "Scenario must use real capture path, got:\n{run_out}"
    );

    let derived = Command::new(bin())
        .args([
            "derived",
            "--platform-store-url",
            store_url,
            "--pipeline",
            "lab-rp-customers",
        ])
        .output()
        .expect("derived inspect");
    let derived_out = String::from_utf8_lossy(&derived.stdout);
    assert!(
        derived.status.success(),
        "derived inspect failed: {}",
        String::from_utf8_lossy(&derived.stderr)
    );
    assert!(
        derived_out.contains("Alicia")
            && derived_out.contains("Carol")
            && !derived_out.contains("Bob"),
        "projected Derived Managed NAME outcomes, got:\n{derived_out}"
    );
    assert!(
        !derived_out.contains("\"EMAIL\"") && !derived_out.contains("\"email\""),
        "project must omit EMAIL from Derived Managed shape, got:\n{derived_out}"
    );

    let remove = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "lab",
            "scenario",
            "remove",
            "rt-project",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("lab scenario remove rt-project");
    assert!(
        remove.status.success(),
        "lab scenario remove rt-project failed: {}",
        String::from_utf8_lossy(&remove.stderr)
    );

    let _ = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output();
}

/// Full Rich Transform `filter` Lab Scenario against Docker Lab Fixture + Instant Client
/// (issue #66).
#[tokio::test]
#[ignore = "requires Docker Compose Lab Fixture + Oracle Instant Client; not part of Release Quality Gate"]
async fn lab_scenario_rt_filter_run_and_inspect() {
    let lab = lab_dir();
    let store_url = "postgres://migraloop:migraloop@127.0.0.1:5432/migraloop";

    let down_first = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output()
        .expect("lab down cleanup");
    assert!(
        down_first.status.success(),
        "lab down (cleanup) failed: {}",
        String::from_utf8_lossy(&down_first.stderr)
    );

    let up = Command::new(bin())
        .args(["lab", "up", "--lab-dir", &lab])
        .output()
        .expect("lab up");
    let up_out = format!(
        "{}{}",
        String::from_utf8_lossy(&up.stdout),
        String::from_utf8_lossy(&up.stderr)
    );
    assert!(up.status.success(), "lab up failed:\n{up_out}");

    let run = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args(["lab", "scenario", "run", "rt-filter", "--lab-dir", &lab])
        .output()
        .expect("lab scenario run rt-filter");
    let run_out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(
        run.status.success(),
        "lab scenario run rt-filter failed:\n{run_out}"
    );
    assert!(
        run_out.contains("Lab Scenario: PASS"),
        "expected Scenario PASS, got:\n{run_out}"
    );

    let derived = Command::new(bin())
        .args([
            "derived",
            "--platform-store-url",
            store_url,
            "--pipeline",
            "lab-rf-customers",
        ])
        .output()
        .expect("derived inspect");
    let derived_out = String::from_utf8_lossy(&derived.stdout);
    assert!(
        derived.status.success(),
        "derived inspect failed: {}",
        String::from_utf8_lossy(&derived.stderr)
    );
    assert!(
        derived_out.contains("Alicia")
            && derived_out.contains("Bob")
            && derived_out.contains("Carol")
            && !derived_out.contains("Dana"),
        "filtered Derived Managed NAME outcomes (ACTIVE flip + inactive exclude), got:\n{derived_out}"
    );

    let remove = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "lab",
            "scenario",
            "remove",
            "rt-filter",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("lab scenario remove rt-filter");
    assert!(
        remove.status.success(),
        "lab scenario remove rt-filter failed: {}",
        String::from_utf8_lossy(&remove.stderr)
    );

    let _ = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output();
}

/// Full idempotent re-delivery Lab Scenario against Docker Lab Fixture + Instant Client
/// (issue #86 / PRD #55 US49).
///
/// ```bash
/// export LD_LIBRARY_PATH=/path/to/instantclient
/// cargo test -p migraloop-app --test cli_lab_scenario -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "requires Docker Compose Lab Fixture + Oracle Instant Client; not part of Release Quality Gate"]
async fn lab_scenario_idempotent_redelivery_run_and_inspect() {
    let lab = lab_dir();
    let store_url = "postgres://migraloop:migraloop@127.0.0.1:5432/migraloop";

    let down_first = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output()
        .expect("lab down cleanup");
    assert!(
        down_first.status.success(),
        "lab down (cleanup) failed: {}",
        String::from_utf8_lossy(&down_first.stderr)
    );

    let up = Command::new(bin())
        .args(["lab", "up", "--lab-dir", &lab])
        .output()
        .expect("lab up");
    let up_out = format!(
        "{}{}",
        String::from_utf8_lossy(&up.stdout),
        String::from_utf8_lossy(&up.stderr)
    );
    assert!(up.status.success(), "lab up failed:\n{up_out}");

    let run = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "lab",
            "scenario",
            "run",
            "idempotent-redelivery",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("lab scenario run idempotent-redelivery");
    let run_out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(
        run.status.success(),
        "lab scenario run idempotent-redelivery failed:\n{run_out}"
    );
    assert!(
        run_out.contains("Lab Scenario: PASS"),
        "expected Scenario PASS, got:\n{run_out}"
    );
    assert!(
        run_out.contains("correctness=pass") || run_out.contains("correctness checks passed"),
        "expected correctness pass, got:\n{run_out}"
    );
    assert!(
        run_out.to_ascii_lowercase().contains("logminer")
            || run_out.contains("Incremental Capture"),
        "Scenario must use real capture path, got:\n{run_out}"
    );
    assert!(
        run_out.to_ascii_lowercase().contains("re-delivery")
            || run_out.to_ascii_lowercase().contains("duplicate-safe"),
        "Scenario must exercise re-Delivery, got:\n{run_out}"
    );
    assert!(
        run_out.contains("namespace=left in place")
            || run_out.to_ascii_lowercase().contains("left in place"),
        "default completed run must keep Namespace, got:\n{run_out}"
    );

    let target = Command::new(bin())
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "target",
            "--platform-store-url",
            store_url,
            "--collection",
            "lab_ir_customers",
        ])
        .output()
        .expect("target inspect");
    let target_out = String::from_utf8_lossy(&target.stdout);
    assert!(
        target.status.success(),
        "target inspect failed: {}",
        String::from_utf8_lossy(&target.stderr)
    );
    assert!(
        target_out.contains("Alicia")
            && target_out.contains("Carol")
            && !target_out.contains("Bob"),
        "Target Managed outcomes must remain correct after re-Delivery, got:\n{target_out}"
    );
    assert!(
        target_out.contains("lab-keep-across-redelivery"),
        "non-Managed operatorNote must survive Managed upsert re-Delivery, got:\n{target_out}"
    );
    assert!(
        target_out.contains("documents: 2") || target_out.contains("documents:2"),
        "Target document count must stay at 2 after duplicate-safe re-Delivery, got:\n{target_out}"
    );

    let leftover_status = Command::new(bin())
        .args(["lab", "status", "--lab-dir", &lab])
        .output()
        .expect("lab status after scenario keep");
    let leftover_out = format!(
        "{}{}",
        String::from_utf8_lossy(&leftover_status.stdout),
        String::from_utf8_lossy(&leftover_status.stderr)
    );
    assert!(
        leftover_status.status.success(),
        "lab status after Scenario keep failed:\n{leftover_out}"
    );
    assert!(
        leftover_out.contains("Scenario Namespace leftover: idempotent-redelivery"),
        "lab status must name leftover Scenario Namespace, got:\n{leftover_out}"
    );

    let remove = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "lab",
            "scenario",
            "remove",
            "idempotent-redelivery",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("lab scenario remove idempotent-redelivery");
    assert!(
        remove.status.success(),
        "lab scenario remove idempotent-redelivery failed: {}",
        String::from_utf8_lossy(&remove.stderr)
    );

    let _ = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output();
}

/// Manual Lab Scenario seam for Poison Change quarantine (#22 / ADR-0015 / ADR-0025).
///
/// ```bash
/// export LD_LIBRARY_PATH=/path/to/instantclient
/// cargo test -p migraloop-app --test cli_lab_scenario -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "requires Docker Compose Lab Fixture + Oracle Instant Client; not part of Release Quality Gate"]
async fn lab_scenario_poison_quarantine_run_and_inspect() {
    let lab = lab_dir();
    let store_url = "postgres://migraloop:migraloop@127.0.0.1:5432/migraloop";

    let down_first = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output()
        .expect("lab down cleanup");
    assert!(
        down_first.status.success(),
        "lab down (cleanup) failed: {}",
        String::from_utf8_lossy(&down_first.stderr)
    );

    let up = Command::new(bin())
        .args(["lab", "up", "--lab-dir", &lab])
        .output()
        .expect("lab up");
    let up_out = format!(
        "{}{}",
        String::from_utf8_lossy(&up.stdout),
        String::from_utf8_lossy(&up.stderr)
    );
    assert!(up.status.success(), "lab up failed:\n{up_out}");

    let run = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "lab",
            "scenario",
            "run",
            "poison-quarantine",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("lab scenario run poison-quarantine");
    let run_out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(
        run.status.success(),
        "lab scenario run poison-quarantine failed:\n{run_out}"
    );
    assert!(
        run_out.contains("Lab Scenario: PASS"),
        "expected Scenario PASS, got:\n{run_out}"
    );
    assert!(
        run_out.to_ascii_lowercase().contains("quarantine")
            && run_out.to_ascii_lowercase().contains("alert"),
        "Scenario must quarantine with ALERT, got:\n{run_out}"
    );
    assert!(
        run_out.to_ascii_lowercase().contains("logminer")
            || run_out.contains("Incremental Capture"),
        "Scenario must use real capture path, got:\n{run_out}"
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", store_url])
        .output()
        .expect("status after poison-quarantine");
    let status_out = String::from_utf8_lossy(&status.stdout);
    assert!(status.status.success());
    assert!(
        status_out.contains("Delivery Health: unhealthy")
            && status_out.to_ascii_lowercase().contains("quarantine")
            && (status_out.contains("identity=1")
                || status_out.to_ascii_lowercase().contains("identity=1")),
        "status must show unhealthy quarantine for identity 1, got:\n{status_out}"
    );

    let target = Command::new(bin())
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "target",
            "--platform-store-url",
            store_url,
            "--collection",
            "lab_pq_customers",
        ])
        .output()
        .expect("target inspect");
    let target_out = String::from_utf8_lossy(&target.stdout);
    assert!(target.status.success());
    assert!(
        target_out.contains("Alice")
            && !target_out.contains("Alicia")
            && target_out.contains("Carol")
            && !target_out.contains("Bob"),
        "Target must keep Alice, Deliver Carol, delete Bob, got:\n{target_out}"
    );

    let remove = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "lab",
            "scenario",
            "remove",
            "poison-quarantine",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("lab scenario remove poison-quarantine");
    assert!(
        remove.status.success(),
        "lab scenario remove poison-quarantine failed: {}",
        String::from_utf8_lossy(&remove.stderr)
    );

    let _ = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output();
}

/// Manual Lab Scenario seam for blocking DDL Schema Change warn+pause (#23 / ADR-0009 / ADR-0025).
///
/// ```bash
/// export LD_LIBRARY_PATH=/path/to/instantclient
/// cargo test -p migraloop-app --test cli_lab_scenario -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "requires Docker Compose Lab Fixture + Oracle Instant Client; not part of Release Quality Gate"]
async fn lab_scenario_schema_change_pause_run_and_inspect() {
    let lab = lab_dir();
    let store_url = "postgres://migraloop:migraloop@127.0.0.1:5432/migraloop";

    let down_first = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output()
        .expect("lab down cleanup");
    assert!(
        down_first.status.success(),
        "lab down (cleanup) failed: {}",
        String::from_utf8_lossy(&down_first.stderr)
    );

    let up = Command::new(bin())
        .args(["lab", "up", "--lab-dir", &lab])
        .output()
        .expect("lab up");
    let up_out = format!(
        "{}{}",
        String::from_utf8_lossy(&up.stdout),
        String::from_utf8_lossy(&up.stderr)
    );
    assert!(up.status.success(), "lab up failed:\n{up_out}");

    let run = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "lab",
            "scenario",
            "run",
            "schema-change-pause",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("lab scenario run schema-change-pause");
    let run_out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(
        run.status.success(),
        "lab scenario run schema-change-pause failed:\n{run_out}"
    );
    assert!(
        run_out.contains("Lab Scenario: PASS"),
        "expected Scenario PASS, got:\n{run_out}"
    );
    let run_lower = run_out.to_ascii_lowercase();
    assert!(
        run_lower.contains("warn")
            && run_lower.contains("schema change")
            && run_lower.contains("paused"),
        "Scenario must WARN and pause on blocking DDL, got:\n{run_out}"
    );
    assert!(
        !run_lower.contains("alert: poison")
            && !run_out.contains("Quarantine:")
            && run_lower.contains("not poison quarantine"),
        "Scenario must stay distinct from poison quarantine, got:\n{run_out}"
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", store_url])
        .output()
        .expect("status after schema-change-pause");
    let status_out = String::from_utf8_lossy(&status.stdout);
    assert!(status.status.success());
    let status_lower = status_out.to_ascii_lowercase();
    assert!(
        (status_out.contains("Delivery Health: paused")
            || status_lower.contains("delivery health: paused"))
            && status_lower.contains("schema change")
            && status_lower.contains("blocking"),
        "status must show paused + Schema Change blocking, got:\n{status_out}"
    );

    let remove = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "lab",
            "scenario",
            "remove",
            "schema-change-pause",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("lab scenario remove schema-change-pause");
    assert!(
        remove.status.success(),
        "lab scenario remove schema-change-pause failed: {}",
        String::from_utf8_lossy(&remove.stderr)
    );

    let _ = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output();
}

/// Manual Lab Scenario seam for Source Alignment Check (#24 / ADR-0025).
///
/// ```bash
/// export LD_LIBRARY_PATH=/path/to/instantclient
/// cargo test -p migraloop-app --test cli_lab_scenario -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "requires Docker Compose Lab Fixture + Oracle Instant Client; not part of Release Quality Gate"]
async fn lab_scenario_source_alignment_run_and_inspect() {
    let lab = lab_dir();
    let store_url = "postgres://migraloop:migraloop@127.0.0.1:5432/migraloop";

    let down_first = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output()
        .expect("lab down cleanup");
    assert!(
        down_first.status.success(),
        "lab down (cleanup) failed: {}",
        String::from_utf8_lossy(&down_first.stderr)
    );

    let up = Command::new(bin())
        .args(["lab", "up", "--lab-dir", &lab])
        .output()
        .expect("lab up");
    let up_out = format!(
        "{}{}",
        String::from_utf8_lossy(&up.stdout),
        String::from_utf8_lossy(&up.stderr)
    );
    assert!(up.status.success(), "lab up failed:\n{up_out}");

    let run = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "lab",
            "scenario",
            "run",
            "source-alignment",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("lab scenario run source-alignment");
    let run_out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(
        run.status.success(),
        "lab scenario run source-alignment failed:\n{run_out}"
    );
    assert!(
        run_out.contains("Lab Scenario: PASS"),
        "expected Scenario PASS, got:\n{run_out}"
    );
    let run_lower = run_out.to_ascii_lowercase();
    assert!(
        run_lower.contains("source alignment")
            && (run_lower.contains("repaired") || run_lower.contains("mismatched"))
            && (run_lower.contains("maxrows=1") || run_lower.contains("partial")),
        "Scenario must detect/repair and exercise resource gate, got:\n{run_out}"
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", store_url])
        .output()
        .expect("status after source-alignment");
    let status_out = String::from_utf8_lossy(&status.stdout);
    assert!(status.status.success());
    assert!(
        status_out.contains("Source Alignment:")
            && status_out.to_ascii_lowercase().contains("aligned"),
        "status must show Source Alignment after Scenario, got:\n{status_out}"
    );

    let remove = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "lab",
            "scenario",
            "remove",
            "source-alignment",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("lab scenario remove source-alignment");
    assert!(
        remove.status.success(),
        "lab scenario remove source-alignment failed: {}",
        String::from_utf8_lossy(&remove.stderr)
    );

    let _ = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output();
}

/// Lab Scenario `drift-check` (issue #25 / ADR-0025): Managed-field Target drift
/// detect + default auto-repair; non-Managed preserved; resource-gated max-rows.
///
/// Ignored in Release Quality Gate — CI twin is `cli_drift_check.rs`.
///
/// ```bash
/// export LD_LIBRARY_PATH=/path/to/instantclient
/// cargo test -p migraloop-app --test cli_lab_scenario -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "requires Docker Compose Lab Fixture + Oracle Instant Client; not part of Release Quality Gate"]
async fn lab_scenario_drift_check_run_and_inspect() {
    let lab = lab_dir();
    let store_url = "postgres://migraloop:migraloop@127.0.0.1:5432/migraloop";

    let down_first = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output()
        .expect("lab down cleanup");
    assert!(
        down_first.status.success(),
        "lab down (cleanup) failed: {}",
        String::from_utf8_lossy(&down_first.stderr)
    );

    let up = Command::new(bin())
        .args(["lab", "up", "--lab-dir", &lab])
        .output()
        .expect("lab up");
    let up_out = format!(
        "{}{}",
        String::from_utf8_lossy(&up.stdout),
        String::from_utf8_lossy(&up.stderr)
    );
    assert!(up.status.success(), "lab up failed:\n{up_out}");

    let run = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "lab",
            "scenario",
            "run",
            "drift-check",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("lab scenario run drift-check");
    let run_out = format!(
        "{}{}",
        String::from_utf8_lossy(&run.stdout),
        String::from_utf8_lossy(&run.stderr)
    );
    assert!(
        run.status.success(),
        "lab scenario run drift-check failed:\n{run_out}"
    );
    assert!(
        run_out.contains("Lab Scenario: PASS"),
        "expected Scenario PASS, got:\n{run_out}"
    );
    let run_lower = run_out.to_ascii_lowercase();
    assert!(
        run_lower.contains("drift")
            && (run_lower.contains("repaired") || run_lower.contains("mismatched"))
            && (run_lower.contains("maxrows=1") || run_lower.contains("partial")),
        "Scenario must detect/repair Managed drift and exercise resource gate, got:\n{run_out}"
    );

    let status = Command::new(bin())
        .args(["status", "--platform-store-url", store_url])
        .output()
        .expect("status after drift-check");
    let status_out = String::from_utf8_lossy(&status.stdout);
    assert!(status.status.success());
    assert!(
        status_out.contains("Drift:")
            && (status_out.to_ascii_lowercase().contains("ok")
                || status_out.to_ascii_lowercase().contains("partial")),
        "status must show Drift after Scenario, got:\n{status_out}"
    );

    let remove = Command::new(bin())
        .env("ORACLE_PASSWORD", "lab_oracle")
        .env("MONGO_PASSWORD", "lab_mongo")
        .args([
            "lab",
            "scenario",
            "remove",
            "drift-check",
            "--lab-dir",
            &lab,
        ])
        .output()
        .expect("lab scenario remove drift-check");
    assert!(
        remove.status.success(),
        "lab scenario remove drift-check failed: {}",
        String::from_utf8_lossy(&remove.stderr)
    );

    let _ = Command::new(bin())
        .args(["lab", "down", "--lab-dir", &lab])
        .output();
}
