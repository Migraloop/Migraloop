//! Black-box tests for Handbook guard touchpoints, exemption, and CLI surface checks.
//!
//! Agreed seam (issue #52 / Spec #47): invoke the single guard entrypoint only.

use std::path::{Path, PathBuf};
use std::process::Command;

fn bin() -> String {
    env!("CARGO_BIN_EXE_handbook-guard").to_string()
}

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures")
        .join(name)
}

fn run(args: &[&str]) -> std::process::Output {
    Command::new(bin())
        .args(args)
        .output()
        .expect("run handbook-guard")
}

fn combined(output: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn touchpoint_base_args() -> Vec<String> {
    let handbook = fixture("touchpoint-base");
    let touchpoints = fixture("touchpoints.json");
    let cli_source = fixture("cli-source/lib.rs");
    let cli_surface = fixture("cli-source/surface.txt");
    vec![
        "check".into(),
        "--handbook".into(),
        handbook.display().to_string(),
        "--touchpoints".into(),
        touchpoints.display().to_string(),
        "--cli-source".into(),
        cli_source.display().to_string(),
        "--cli-surface".into(),
        cli_surface.display().to_string(),
    ]
}

#[test]
fn touchpoint_fails_when_gated_path_changes_without_handbook_updates() {
    let mut args = touchpoint_base_args();
    args.extend([
        "--changed-path".into(),
        "src/gated.rs".into(),
    ]);
    let str_args: Vec<&str> = args.iter().map(String::as_str).collect();
    let output = run(&str_args);
    assert!(
        !output.status.success(),
        "expected touchpoint failure:\n{}",
        combined(&output)
    );
    let text = combined(&output);
    assert!(
        text.contains("cli-and-config.md") && text.to_lowercase().contains("touchpoint"),
        "expected touchpoint failure naming cli-and-config.md, got:\n{text}"
    );
}

#[test]
fn touchpoint_passes_when_handbook_pages_updated_in_all_locales() {
    let mut args = touchpoint_base_args();
    args.extend([
        "--changed-path".into(),
        "src/gated.rs".into(),
        "--changed-path".into(),
        "handbook/en/cli-and-config.md".into(),
        "--changed-path".into(),
        "handbook/zh-TW/cli-and-config.md".into(),
        "--changed-path".into(),
        "handbook/zh-CN/cli-and-config.md".into(),
    ]);
    let str_args: Vec<&str> = args.iter().map(String::as_str).collect();
    let output = run(&str_args);
    assert!(
        output.status.success(),
        "expected success when all locale handbook pages update:\n{}",
        combined(&output)
    );
}

#[test]
fn docs_not_needed_exemption_short_circuits_touchpoints_with_rationale() {
    let mut args = touchpoint_base_args();
    args.extend([
        "--changed-path".into(),
        "src/gated.rs".into(),
        "--docs-not-needed".into(),
        "--docs-not-needed-rationale".into(),
        "Refactor only; no Operator-visible behavior change.".into(),
    ]);
    let str_args: Vec<&str> = args.iter().map(String::as_str).collect();
    let output = run(&str_args);
    assert!(
        output.status.success(),
        "expected exemption to skip touchpoint failure:\n{}",
        combined(&output)
    );
}

#[test]
fn docs_not_needed_without_rationale_fails() {
    let mut args = touchpoint_base_args();
    args.extend([
        "--changed-path".into(),
        "src/gated.rs".into(),
        "--docs-not-needed".into(),
    ]);
    let str_args: Vec<&str> = args.iter().map(String::as_str).collect();
    let output = run(&str_args);
    assert!(
        !output.status.success(),
        "expected failure when docs-not-needed lacks rationale:\n{}",
        combined(&output)
    );
    let text = combined(&output);
    assert!(
        text.to_lowercase().contains("rationale"),
        "expected rationale error, got:\n{text}"
    );
}

#[test]
fn cli_surface_mismatch_fails() {
    let handbook = fixture("touchpoint-base");
    let touchpoints = fixture("touchpoints.json");
    let cli_source = fixture("cli-source/lib.rs");
    let stale = fixture("cli-source/surface-stale.txt");
    let args = [
        "check",
        "--handbook",
        handbook.to_str().unwrap(),
        "--touchpoints",
        touchpoints.to_str().unwrap(),
        "--cli-source",
        cli_source.to_str().unwrap(),
        "--cli-surface",
        stale.to_str().unwrap(),
    ];
    let output = run(&args);
    assert!(
        !output.status.success(),
        "expected CLI surface mismatch failure:\n{}",
        combined(&output)
    );
    let text = combined(&output);
    assert!(
        text.contains("apply")
            && (text.to_lowercase().contains("surface") || text.to_lowercase().contains("snapshot")),
        "expected surface mismatch mentioning apply, got:\n{text}"
    );
}

#[test]
fn cli_reference_missing_subcommand_in_locale_fails() {
    let handbook = fixture("cli-ref-missing");
    let touchpoints = fixture("touchpoints.json");
    let cli_source = fixture("cli-source/lib.rs");
    let cli_surface = fixture("cli-source/surface.txt");
    let args = [
        "check",
        "--handbook",
        handbook.to_str().unwrap(),
        "--touchpoints",
        touchpoints.to_str().unwrap(),
        "--cli-source",
        cli_source.to_str().unwrap(),
        "--cli-surface",
        cli_surface.to_str().unwrap(),
    ];
    let output = run(&args);
    assert!(
        !output.status.success(),
        "expected missing CLI reference failure:\n{}",
        combined(&output)
    );
    let text = combined(&output);
    assert!(
        text.contains("apply") && text.contains("zh-CN"),
        "expected missing apply mention in zh-CN, got:\n{text}"
    );
}
