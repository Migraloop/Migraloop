//! Handbook guard entrypoint (CI tooling).
//!
//! Agreed seam (issue #49 / Spec #47): external behavior of this binary only.
//! This slice checks three-locale path isomorphism under a handbook root.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Parser, Subcommand};

const REQUIRED_LOCALES: [&str; 3] = ["en", "zh-TW", "zh-CN"];

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
    /// Run handbook guards (locale parity in this slice)
    Check {
        /// Path to the handbook portal root (contains locale subtrees)
        #[arg(long, default_value = "handbook")]
        handbook: PathBuf,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Commands::Check { handbook } => match check_locale_parity(&handbook) {
            Ok(()) => {
                println!("locale parity: ok");
                ExitCode::SUCCESS
            }
            Err(errors) => {
                eprintln!("locale parity: failed");
                for error in errors {
                    eprintln!("  - {error}");
                }
                ExitCode::FAILURE
            }
        },
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
