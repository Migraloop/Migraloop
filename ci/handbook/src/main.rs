//! Handbook guard entrypoint (CI tooling).
//!
//! Agreed seam (issues #49 / #52 / Spec #47): external behavior of this binary only.
//! Checks: three-locale path isomorphism; high-signal path touchpoints; CLI surface
//! snapshot drift; CLI reference mentions in every locale.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use serde::Deserialize;

const REQUIRED_LOCALES: [&str; 3] = ["en", "zh-TW", "zh-CN"];
const CLI_REFERENCE_PAGE: &str = "cli-and-config.md";

#[derive(Debug, Parser)]
#[command(
    name = "handbook-guard",
    about = "CI guards for the multilingual Operator/Developer handbook"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Run handbook guards (locale parity, touchpoints, CLI surface)
    Check {
        /// Path to the handbook portal root (contains locale subtrees)
        #[arg(long, default_value = "handbook")]
        handbook: PathBuf,

        /// Touchpoints map (machine config; not human locale prose)
        #[arg(long, default_value = "ci/handbook/touchpoints.json")]
        touchpoints: PathBuf,

        /// Operator CLI definition source of truth (clap Command enum)
        #[arg(long, default_value = "crates/cli/src/lib.rs")]
        cli_source: PathBuf,

        /// Committed CLI subcommand surface snapshot
        #[arg(long, default_value = "ci/handbook/cli-surface.txt")]
        cli_surface: PathBuf,

        /// Changed repo-relative path (repeatable). Also reads HANDBOOK_CHANGED_PATHS.
        #[arg(long = "changed-path", value_name = "PATH")]
        changed_paths: Vec<String>,

        /// File listing changed repo-relative paths (one per line)
        #[arg(long)]
        changed_paths_file: Option<PathBuf>,

        /// Explicit docs-not-needed exemption (also HANDBOOK_DOCS_NOT_NEEDED=1)
        #[arg(long, default_value_t = false)]
        docs_not_needed: bool,

        /// Written rationale required when docs-not-needed is set
        #[arg(long)]
        docs_not_needed_rationale: Option<String>,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Commands::Check {
            handbook,
            touchpoints,
            cli_source,
            cli_surface,
            changed_paths,
            changed_paths_file,
            docs_not_needed,
            docs_not_needed_rationale,
        } => {
            let changed = collect_changed_paths(&changed_paths, changed_paths_file.as_deref());
            let exemption = resolve_exemption(docs_not_needed, docs_not_needed_rationale);
            match run_all_checks(
                &handbook,
                &touchpoints,
                &cli_source,
                &cli_surface,
                &changed,
                exemption,
            ) {
                Ok(messages) => {
                    for message in messages {
                        println!("{message}");
                    }
                    ExitCode::SUCCESS
                }
                Err(errors) => {
                    eprintln!("handbook guard: failed");
                    for error in errors {
                        eprintln!("  - {error}");
                    }
                    ExitCode::FAILURE
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
enum Exemption {
    Inactive,
    Active { rationale: String },
}

fn resolve_exemption(flag: bool, rationale_flag: Option<String>) -> Exemption {
    let env_set = env::var("HANDBOOK_DOCS_NOT_NEEDED")
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false);
    let active = flag || env_set;
    if !active {
        return Exemption::Inactive;
    }
    let rationale = rationale_flag
        .or_else(|| env::var("HANDBOOK_DOCS_NOT_NEEDED_RATIONALE").ok())
        .unwrap_or_default()
        .trim()
        .to_string();
    Exemption::Active { rationale }
}

fn collect_changed_paths(cli_paths: &[String], file: Option<&Path>) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    for path in cli_paths {
        let normalized = normalize_repo_path(path);
        if !normalized.is_empty() {
            paths.insert(normalized);
        }
    }
    if let Some(file) = file {
        if let Ok(contents) = fs::read_to_string(file) {
            for line in contents.lines() {
                let normalized = normalize_repo_path(line);
                if !normalized.is_empty() {
                    paths.insert(normalized);
                }
            }
        }
    }
    if let Ok(env_paths) = env::var("HANDBOOK_CHANGED_PATHS") {
        for line in env_paths.lines() {
            let normalized = normalize_repo_path(line);
            if !normalized.is_empty() {
                paths.insert(normalized);
            }
        }
    }
    paths
}

fn normalize_repo_path(path: &str) -> String {
    path.trim().trim_start_matches("./").replace('\\', "/")
}

fn run_all_checks(
    handbook: &Path,
    touchpoints_path: &Path,
    cli_source: &Path,
    cli_surface: &Path,
    changed_paths: &BTreeSet<String>,
    exemption: Exemption,
) -> Result<Vec<String>, Vec<String>> {
    let mut errors = Vec::new();
    let mut oks = Vec::new();

    match check_locale_parity(handbook) {
        Ok(()) => oks.push("locale parity: ok".to_string()),
        Err(mut errs) => errors.append(&mut errs),
    }

    match check_touchpoints(touchpoints_path, changed_paths, &exemption) {
        Ok(msg) => oks.push(msg),
        Err(mut errs) => errors.append(&mut errs),
    }

    let live_subcommands = match load_live_cli_subcommands(cli_source) {
        Ok(cmds) => Some(cmds),
        Err(mut errs) => {
            errors.append(&mut errs);
            None
        }
    };

    if let Some(ref live) = live_subcommands {
        match check_cli_surface_snapshot(live, cli_source, cli_surface) {
            Ok(()) => oks.push("cli surface: ok".to_string()),
            Err(mut errs) => errors.append(&mut errs),
        }
        match check_cli_reference_mentions(handbook, live) {
            Ok(()) => oks.push("cli reference: ok".to_string()),
            Err(mut errs) => errors.append(&mut errs),
        }
    }

    if errors.is_empty() {
        Ok(oks)
    } else {
        Err(errors)
    }
}

fn check_locale_parity(handbook: &Path) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();

    if !handbook.is_dir() {
        return Err(vec![format!(
            "handbook root is not a directory: {}",
            handbook.display()
        )]);
    }

    let mut pages_by_locale: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for locale in REQUIRED_LOCALES {
        let locale_dir = handbook.join(locale);
        if !locale_dir.is_dir() {
            errors.push(format!("missing required locale directory: {locale}/"));
            continue;
        }
        match collect_relative_markdown_pages(&locale_dir) {
            Ok(pages) => {
                if pages.is_empty() {
                    errors.push(format!("locale {locale}/ has no markdown pages"));
                }
                pages_by_locale.insert(locale.to_string(), pages);
            }
            Err(err) => errors.push(format!("failed to read {locale}/: {err}")),
        }
    }

    if pages_by_locale.len() == REQUIRED_LOCALES.len() {
        let canonical = &pages_by_locale["en"];
        for locale in REQUIRED_LOCALES
            .iter()
            .copied()
            .filter(|locale| *locale != "en")
        {
            let pages = &pages_by_locale[locale];
            for missing in canonical.difference(pages) {
                errors.push(format!(
                    "locale parity: {locale}/ is missing {missing} (present in en/)"
                ));
            }
            for extra in pages.difference(canonical) {
                errors.push(format!(
                    "locale parity: {locale}/ has extra page {extra} (absent from en/)"
                ));
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

#[derive(Debug, Deserialize)]
struct TouchpointsFile {
    touchpoints: Vec<Touchpoint>,
}

#[derive(Debug, Deserialize)]
struct Touchpoint {
    id: String,
    paths: Vec<String>,
    handbook_pages: Vec<String>,
}

fn check_touchpoints(
    touchpoints_path: &Path,
    changed_paths: &BTreeSet<String>,
    exemption: &Exemption,
) -> Result<String, Vec<String>> {
    if let Exemption::Active { rationale } = exemption {
        if rationale.is_empty() {
            return Err(vec![
                "docs-not-needed exemption requires a written rationale \
                 (--docs-not-needed-rationale or HANDBOOK_DOCS_NOT_NEEDED_RATIONALE)"
                    .to_string(),
            ]);
        }
        return Ok(format!(
            "touchpoints: skipped (docs-not-needed: {rationale})"
        ));
    }

    if changed_paths.is_empty() {
        return Ok("touchpoints: ok (no changed paths)".to_string());
    }

    if !touchpoints_path.is_file() {
        return Err(vec![format!(
            "touchpoints map not found: {}",
            touchpoints_path.display()
        )]);
    }

    let contents = fs::read_to_string(touchpoints_path).map_err(|err| {
        vec![format!(
            "failed to read touchpoints map {}: {err}",
            touchpoints_path.display()
        )]
    })?;
    let file: TouchpointsFile = serde_json::from_str(&contents).map_err(|err| {
        vec![format!(
            "invalid touchpoints map {}: {err}",
            touchpoints_path.display()
        )]
    })?;

    let mut errors = Vec::new();
    for touchpoint in &file.touchpoints {
        let gated_hit = changed_paths
            .iter()
            .any(|changed| touchpoint.paths.iter().any(|pattern| path_matches(pattern, changed)));
        if !gated_hit {
            continue;
        }

        for page in &touchpoint.handbook_pages {
            for locale in REQUIRED_LOCALES {
                let required = format!("handbook/{locale}/{page}");
                let updated = changed_paths.iter().any(|changed| {
                    normalize_repo_path(changed) == required
                        || changed.ends_with(&format!("/{locale}/{page}"))
                        || changed == &format!("{locale}/{page}")
                });
                if !updated {
                    errors.push(format!(
                        "touchpoint {}: gated path changed but {} was not updated in this change",
                        touchpoint.id, required
                    ));
                }
            }
        }
    }

    if errors.is_empty() {
        Ok("touchpoints: ok".to_string())
    } else {
        Err(errors)
    }
}

fn path_matches(pattern: &str, path: &str) -> bool {
    let pattern = normalize_repo_path(pattern);
    let path = normalize_repo_path(path);
    if pattern.contains('*') {
        glob_match(&pattern, &path)
    } else {
        path == pattern || path.starts_with(&(pattern.clone() + "/"))
    }
}

fn glob_match(pattern: &str, path: &str) -> bool {
    let pattern_parts: Vec<&str> = pattern.split('/').collect();
    let path_parts: Vec<&str> = path.split('/').filter(|p| !p.is_empty()).collect();
    glob_match_parts(&pattern_parts, &path_parts)
}

fn glob_match_parts(pattern: &[&str], path: &[&str]) -> bool {
    match (pattern.first(), path.first()) {
        (None, None) => true,
        (Some(&"**"), _) => {
            let after = &pattern[1..];
            if after.is_empty() {
                return true;
            }
            for i in 0..=path.len() {
                if glob_match_parts(after, &path[i..]) {
                    return true;
                }
            }
            false
        }
        (Some(p), Some(s)) => {
            if segment_match(p, s) {
                glob_match_parts(&pattern[1..], &path[1..])
            } else {
                false
            }
        }
        (Some(_), None) | (None, Some(_)) => false,
    }
}

fn segment_match(pattern: &str, segment: &str) -> bool {
    if pattern == "*" || pattern == segment {
        return true;
    }
    if !pattern.contains('*') {
        return false;
    }
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == segment;
    }
    let mut rest = segment;
    if !parts[0].is_empty() {
        if !rest.starts_with(parts[0]) {
            return false;
        }
        rest = &rest[parts[0].len()..];
    }
    for (i, part) in parts.iter().enumerate().skip(1) {
        if part.is_empty() {
            if i == parts.len() - 1 {
                return true;
            }
            continue;
        }
        if i == parts.len() - 1 {
            return rest.ends_with(part);
        }
        if let Some(idx) = rest.find(part) {
            rest = &rest[idx + part.len()..];
        } else {
            return false;
        }
    }
    true
}

fn load_live_cli_subcommands(cli_source: &Path) -> Result<BTreeSet<String>, Vec<String>> {
    let source = fs::read_to_string(cli_source).map_err(|err| {
        vec![format!(
            "failed to read CLI source {}: {err}",
            cli_source.display()
        )]
    })?;
    extract_cli_subcommands(&source).map_err(|err| vec![err])
}

fn check_cli_surface_snapshot(
    live: &BTreeSet<String>,
    cli_source: &Path,
    cli_surface: &Path,
) -> Result<(), Vec<String>> {
    let snapshot_raw = fs::read_to_string(cli_surface).map_err(|err| {
        vec![format!(
            "failed to read CLI surface snapshot {}: {err}",
            cli_surface.display()
        )]
    })?;
    let snapshot = parse_surface_snapshot(&snapshot_raw);

    let mut errors = Vec::new();
    for cmd in live.difference(&snapshot) {
        errors.push(format!(
            "cli surface: live Operator CLI has subcommand `{cmd}` absent from snapshot {}",
            cli_surface.display()
        ));
    }
    for cmd in snapshot.difference(live) {
        errors.push(format!(
            "cli surface: snapshot lists subcommand `{cmd}` not present in live CLI source {}",
            cli_source.display()
        ));
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn parse_surface_snapshot(contents: &str) -> BTreeSet<String> {
    contents
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(ToOwned::to_owned)
        .collect()
}

/// Extract clap subcommand names from the Operator CLI `Command` enum source.
///
/// Prefer parsing the definition source of truth over building the product binary
/// solely for handbook CI (Spec #47).
fn extract_cli_subcommands(source: &str) -> Result<BTreeSet<String>, String> {
    let marker = "enum Command";
    let start = source
        .find(marker)
        .ok_or_else(|| "CLI source: could not find `enum Command`".to_string())?;
    let after = &source[start + marker.len()..];
    let brace = after
        .find('{')
        .ok_or_else(|| "CLI source: `enum Command` has no opening brace".to_string())?;
    let body = &after[brace + 1..];

    let mut depth = 1usize;
    let mut end = 0usize;
    for (idx, ch) in body.char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    end = idx;
                    break;
                }
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err("CLI source: unmatched braces in `enum Command`".to_string());
    }

    let enum_body = &body[..end];
    let mut commands = BTreeSet::new();
    for line in enum_body.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') || trimmed.starts_with("//") {
            continue;
        }
        if trimmed.starts_with("#[") {
            continue;
        }
        let name_part = trimmed.split_whitespace().next().unwrap_or("");
        let variant = name_part.trim_end_matches([',', '{', '(']);
        if variant.is_empty() || !variant.starts_with(|c: char| c.is_ascii_uppercase()) {
            continue;
        }
        if variant == "Command" {
            continue;
        }
        commands.insert(pascal_to_kebab(variant));
    }

    if commands.is_empty() {
        return Err("CLI source: no subcommands extracted from `enum Command`".to_string());
    }
    Ok(commands)
}

fn pascal_to_kebab(name: &str) -> String {
    let mut out = String::new();
    for (i, ch) in name.chars().enumerate() {
        if ch.is_uppercase() {
            if i > 0 {
                out.push('-');
            }
            out.extend(ch.to_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

fn check_cli_reference_mentions(
    handbook: &Path,
    subcommands: &BTreeSet<String>,
) -> Result<(), Vec<String>> {
    let mut errors = Vec::new();
    for locale in REQUIRED_LOCALES {
        let page = handbook.join(locale).join(CLI_REFERENCE_PAGE);
        let contents = match fs::read_to_string(&page) {
            Ok(text) => text,
            Err(err) => {
                errors.push(format!(
                    "cli reference: failed to read {}: {err}",
                    page.display()
                ));
                continue;
            }
        };
        let lower = contents.to_ascii_lowercase();
        for cmd in subcommands {
            let needle = cmd.to_ascii_lowercase();
            if !contains_word(&lower, &needle) {
                errors.push(format!(
                    "cli reference: {locale}/{CLI_REFERENCE_PAGE} omits subcommand `{cmd}`"
                ));
            }
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

fn contains_word(haystack: &str, word: &str) -> bool {
    let bytes = haystack.as_bytes();
    let word_bytes = word.as_bytes();
    if word_bytes.is_empty() {
        return true;
    }
    let mut start = 0usize;
    while start + word_bytes.len() <= bytes.len() {
        if let Some(rel) = haystack[start..].find(word) {
            let abs = start + rel;
            let before_ok = abs == 0
                || (!bytes[abs - 1].is_ascii_alphanumeric()
                    && bytes[abs - 1] != b'_'
                    && bytes[abs - 1] != b'-');
            let after = abs + word_bytes.len();
            let after_ok = after >= bytes.len()
                || (!bytes[after].is_ascii_alphanumeric()
                    && bytes[after] != b'_'
                    && bytes[after] != b'-');
            if before_ok && after_ok {
                return true;
            }
            start = abs + 1;
        } else {
            break;
        }
    }
    false
}

fn collect_relative_markdown_pages(locale_dir: &Path) -> Result<BTreeSet<String>, String> {
    let mut pages = BTreeSet::new();
    collect_markdown_into(locale_dir, locale_dir, &mut pages)?;
    Ok(pages)
}

fn collect_markdown_into(
    root: &Path,
    current: &Path,
    pages: &mut BTreeSet<String>,
) -> Result<(), String> {
    let entries = fs::read_dir(current)
        .map_err(|err| format!("read_dir {}: {err}", current.display()))?;

    for entry in entries {
        let entry = entry.map_err(|err| format!("read entry under {}: {err}", current.display()))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|err| format!("file_type {}: {err}", path.display()))?;

        if file_type.is_dir() {
            collect_markdown_into(root, &path, pages)?;
            continue;
        }

        if file_type.is_file()
            && path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| ext.eq_ignore_ascii_case("md"))
        {
            let relative = path
                .strip_prefix(root)
                .map_err(|err| format!("strip_prefix {}: {err}", path.display()))?;
            pages.insert(relative.to_string_lossy().replace('\\', "/"));
        }
    }

    Ok(())
}
