//! Verifies that `[patch.crates-io]` in `Cargo.toml` and `SIBLING_MEMBERS` in `MODULE.bazel`
//! stay complete and accurate against the pinned revisions of all sibling repositories.
//!
//! Cargo ignores a dependency's own patch table, so every crate published by any
//! sibling must be listed in `Cargo.toml [patch.crates-io]`. `rules_rs` skips glob
//! members like `crates/*`, so every crate's directory must also be declared in
//! `MODULE.bazel SIBLING_MEMBERS` (with `krabka-protocol` annotated separately).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use clap::Parser;
use regex::Regex;
use serde::Deserialize;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum VerifyError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Regex error: {0}")]
    Regex(#[from] regex::Error),

    #[error("TOML error: {0}")]
    Toml(#[from] toml::de::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Command execution failed: {0}")]
    Command(String),
}

#[derive(Parser, Debug)]
#[command(
    name = "verify-sibling-members",
    about = "Verifies sibling crate completeness in Cargo.toml and MODULE.bazel"
)]
struct Cli {
    /// Path to root Cargo.toml
    #[arg(long, default_value = "Cargo.toml")]
    cargo_toml: PathBuf,

    /// Path to root MODULE.bazel
    #[arg(long, default_value = "MODULE.bazel")]
    module_bazel: PathBuf,

    /// Automatically write updated member lists into Cargo.toml and MODULE.bazel
    #[arg(long)]
    write: bool,

    /// Local directory override for a sibling repository (e.g. --sibling-dir krabka-broker=/path/to/repo)
    #[arg(long = "sibling-dir", value_name = "NAME=PATH")]
    sibling_dirs: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatchEntry {
    pub crate_name: String,
    pub git_url: String,
    pub rev: String,
}

#[derive(Deserialize, Debug)]
struct CargoMetadata {
    packages: Vec<MetadataPackage>,
}

#[derive(Deserialize, Debug)]
struct MetadataPackage {
    name: String,
    manifest_path: String,
}

#[derive(Deserialize, Debug)]
struct ManifestFile {
    package: Option<ManifestPackage>,
}

#[derive(Deserialize, Debug)]
struct ManifestPackage {
    name: Option<String>,
}

/// Parses `[patch.crates-io]` from `Cargo.toml`.
/// Returns map of `git_url -> (pinned_rev, list_of_crate_names)`.
pub fn parse_cargo_toml_patches(
    cargo_toml_content: &str,
) -> Result<BTreeMap<String, (String, Vec<String>)>, VerifyError> {
    let patch_regex = Regex::new(r"(?s)\[patch\.crates-io\]\s*(.*?)(?:\n\[|\z)")?;
    let Some(patch_caps) = patch_regex.captures(cargo_toml_content) else {
        return Err(VerifyError::Config(
            "No [patch.crates-io] section found in Cargo.toml".to_string(),
        ));
    };

    let patch_body = &patch_caps[1];
    let entry_regex = Regex::new(
        r#"(?m)^\s*([a-zA-Z0-9_-]+)\s*=\s*\{\s*git\s*=\s*"([^"]+)",\s*rev\s*=\s*"([0-9a-fA-F]+)"\s*\}"#,
    )?;

    let mut repo_map: BTreeMap<String, (String, Vec<String>)> = BTreeMap::new();
    let mut revisions_by_repo: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

    for caps in entry_regex.captures_iter(patch_body) {
        let crate_name = caps[1].to_string();
        let git_url = caps[2].trim_end_matches('/').to_string();
        let rev = caps[3].to_string();

        revisions_by_repo
            .entry(git_url.clone())
            .or_default()
            .insert(rev.clone());

        repo_map
            .entry(git_url)
            .or_insert_with(|| (rev, Vec::new()))
            .1
            .push(crate_name);
    }

    for (url, revs) in &revisions_by_repo {
        if revs.len() > 1 {
            return Err(VerifyError::Config(format!(
                "Repository {url} is pinned to multiple revisions in [patch.crates-io]: {revs:?}"
            )));
        }
    }

    Ok(repo_map)
}

/// Parses `SIBLING_MEMBERS` from `MODULE.bazel`.
/// Returns map of `crate_name -> relative_directory`.
pub fn parse_module_bazel_members(
    module_bazel_content: &str,
) -> Result<BTreeMap<String, String>, VerifyError> {
    let dict_regex = Regex::new(r"(?s)SIBLING_MEMBERS\s*=\s*\{([^}]+)\}")?;
    let Some(dict_caps) = dict_regex.captures(module_bazel_content) else {
        return Err(VerifyError::Config(
            "SIBLING_MEMBERS dict not found in MODULE.bazel".to_string(),
        ));
    };

    let dict_body = &dict_caps[1];
    let entry_regex = Regex::new(r#""([^"]+)"\s*:\s*"([^"]+)""#)?;

    let mut members = BTreeMap::new();
    for caps in entry_regex.captures_iter(dict_body) {
        members.insert(caps[1].to_string(), caps[2].to_string());
    }

    Ok(members)
}

/// Checks that `krabka-protocol` is annotated with `gen_build_script = "off"`.
#[must_use]
pub fn check_krabka_protocol_annotation(module_bazel_content: &str) -> bool {
    let pattern = Regex::new(
        r#"(?s)crate\.annotation\(\s*crate\s*=\s*"krabka-protocol",\s*gen_build_script\s*=\s*"off",\s*strip_prefix\s*=\s*"crates/protocol",\s*workspace_cargo_toml\s*=\s*"Cargo\.toml",?\s*\)"#,
    ).expect("valid regex");
    pattern.is_match(module_bazel_content)
}

fn parse_manifest_crate(manifest_file: &Path, repo_dir: &Path) -> Option<(String, String)> {
    let content = fs::read_to_string(manifest_file).ok()?;
    let manifest: ManifestFile = toml::from_str(&content).ok()?;
    let name = manifest.package?.name?;
    let parent = manifest_file.parent()?;
    let rel = parent.strip_prefix(repo_dir).ok()?;
    let rel_str = rel.to_string_lossy().replace('\\', "/");
    Some((name, rel_str))
}

fn extract_packages_from_dir(repo_dir: &Path) -> Result<BTreeMap<String, String>, VerifyError> {
    if let Ok(out) = Command::new("cargo")
        .args(["metadata", "--no-deps", "--format-version", "1"])
        .current_dir(repo_dir)
        .output()
        && out.status.success()
    {
        let metadata: CargoMetadata = serde_json::from_slice(&out.stdout)?;
        let mut packages = BTreeMap::new();
        for pkg in metadata.packages {
            let manifest_path = PathBuf::from(pkg.manifest_path);
            if let Some(parent) = manifest_path.parent()
                && let Ok(rel) = parent.strip_prefix(repo_dir)
            {
                let rel_str = rel.to_string_lossy().replace('\\', "/");
                packages.insert(pkg.name, rel_str);
            }
        }
        if !packages.is_empty() {
            return Ok(packages);
        }
    }

    let crates_dir = repo_dir.join("crates");
    let mut packages = BTreeMap::new();
    if let Ok(entries) = fs::read_dir(&crates_dir) {
        for entry in entries.flatten() {
            let manifest_file = entry.path().join("Cargo.toml");
            if let Some((name, rel_dir)) = parse_manifest_crate(&manifest_file, repo_dir) {
                packages.insert(name, rel_dir);
            }
        }
    }

    if packages.is_empty() {
        return Err(VerifyError::Config(format!(
            "Could not discover member crates in {}",
            repo_dir.display()
        )));
    }

    Ok(packages)
}

pub fn get_sibling_metadata(
    git_url: &str,
    rev: &str,
    local_override: Option<&Path>,
) -> Result<BTreeMap<String, String>, VerifyError> {
    if let Some(override_dir) = local_override.filter(|p| p.is_dir()) {
        return extract_packages_from_dir(override_dir);
    }

    let temp_dir = tempfile::tempdir()?;
    let tmp_path = temp_dir.path();

    let init_res = Command::new("git")
        .arg("init")
        .current_dir(tmp_path)
        .output()?;
    if !init_res.status.success() {
        return Err(VerifyError::Command(format!(
            "git init failed: {}",
            String::from_utf8_lossy(&init_res.stderr)
        )));
    }

    let remote_res = Command::new("git")
        .args(["remote", "add", "origin", git_url])
        .current_dir(tmp_path)
        .output()?;
    if !remote_res.status.success() {
        return Err(VerifyError::Command(format!(
            "git remote add failed: {}",
            String::from_utf8_lossy(&remote_res.stderr)
        )));
    }

    let fetch_res = Command::new("git")
        .args(["fetch", "--depth", "1", "origin", rev])
        .current_dir(tmp_path)
        .output()?;
    if !fetch_res.status.success() {
        return Err(VerifyError::Command(format!(
            "git fetch failed for {git_url} at {rev}:\n{}",
            String::from_utf8_lossy(&fetch_res.stderr)
        )));
    }

    let checkout_res = Command::new("git")
        .args(["checkout", "--detach", "FETCH_HEAD"])
        .current_dir(tmp_path)
        .output()?;
    if !checkout_res.status.success() {
        return Err(VerifyError::Command(format!(
            "git checkout failed: {}",
            String::from_utf8_lossy(&checkout_res.stderr)
        )));
    }

    extract_packages_from_dir(tmp_path)
}

pub struct VerificationResult {
    pub is_valid: bool,
    pub errors: Vec<String>,
    pub expected_by_repo: BTreeMap<String, BTreeMap<String, String>>,
    pub patches: BTreeMap<String, (String, Vec<String>)>,
}

pub fn verify_workspace(
    cargo_toml_path: &Path,
    module_bazel_path: &Path,
    sibling_dirs: &BTreeMap<String, PathBuf>,
) -> Result<VerificationResult, VerifyError> {
    let cargo_toml_content = fs::read_to_string(cargo_toml_path)?;
    let module_bazel_content = fs::read_to_string(module_bazel_path)?;

    let patches = parse_cargo_toml_patches(&cargo_toml_content)?;
    let bazel_members = parse_module_bazel_members(&module_bazel_content)?;
    let has_protocol_annotation = check_krabka_protocol_annotation(&module_bazel_content);

    let mut errors = Vec::new();
    if !has_protocol_annotation {
        errors.push(
            "MODULE.bazel: Missing required dedicated annotation for krabka-protocol \
             (with gen_build_script = 'off' and strip_prefix = 'crates/protocol')."
                .to_string(),
        );
    }

    let mut expected_by_repo = BTreeMap::new();
    let mut all_expected_crates = BTreeMap::new();

    for (git_url, (rev, declared_crates)) in &patches {
        let repo_name = git_url.split('/').next_back().unwrap_or(git_url);
        let local_override = sibling_dirs
            .get(repo_name)
            .or_else(|| sibling_dirs.get(git_url))
            .map(PathBuf::as_path);

        let expected_packages = match get_sibling_metadata(git_url, rev, local_override) {
            Ok(pkgs) => pkgs,
            Err(err) => {
                let rev_short = if rev.len() >= 7 { &rev[..7] } else { rev };
                errors.push(format!(
                    "Failed to inspect sibling {git_url} ({rev_short}): {err}"
                ));
                continue;
            }
        };

        all_expected_crates.extend(expected_packages.clone());
        expected_by_repo.insert(git_url.clone(), expected_packages.clone());

        let declared_set: BTreeSet<&String> = declared_crates.iter().collect();
        let expected_set: BTreeSet<&String> = expected_packages.keys().collect();

        let missing_in_cargo: Vec<_> = expected_set.difference(&declared_set).copied().collect();
        let extra_in_cargo: Vec<_> = declared_set.difference(&expected_set).copied().collect();

        let rev_short = if rev.len() >= 7 { &rev[..7] } else { rev };

        if !missing_in_cargo.is_empty() {
            let mut msg = format!(
                "Cargo.toml [patch.crates-io]: Missing {} crate(s) from {repo_name} ({rev_short}):\n",
                missing_in_cargo.len()
            );
            for c in missing_in_cargo {
                let _ = writeln!(msg, "  + {c} = {{ git = \"{git_url}\", rev = \"{rev}\" }}");
            }
            errors.push(msg.trim_end().to_string());
        }

        if !extra_in_cargo.is_empty() {
            let mut msg = format!(
                "Cargo.toml [patch.crates-io]: Extraneous {} crate(s) declared for {repo_name} ({rev_short}):\n",
                extra_in_cargo.len()
            );
            for c in extra_in_cargo {
                let _ = writeln!(msg, "  - {c}");
            }
            errors.push(msg.trim_end().to_string());
        }
    }

    let expected_bazel_set: BTreeSet<String> = all_expected_crates
        .keys()
        .filter(|k| *k != "krabka-protocol")
        .cloned()
        .collect();
    let declared_bazel_set: BTreeSet<String> = bazel_members.keys().cloned().collect();

    let missing_in_bazel: Vec<_> = expected_bazel_set.difference(&declared_bazel_set).collect();
    let extra_in_bazel: Vec<_> = declared_bazel_set.difference(&expected_bazel_set).collect();

    if !missing_in_bazel.is_empty() {
        let mut msg = format!(
            "MODULE.bazel SIBLING_MEMBERS: Missing {} crate(s):\n",
            missing_in_bazel.len()
        );
        for c in missing_in_bazel {
            let _ = writeln!(msg, "  + \"{c}\": \"{}\"", all_expected_crates[c]);
        }
        errors.push(msg.trim_end().to_string());
    }

    if !extra_in_bazel.is_empty() {
        let mut msg = format!(
            "MODULE.bazel SIBLING_MEMBERS: Extraneous {} crate(s) declared:\n",
            extra_in_bazel.len()
        );
        for c in extra_in_bazel {
            let _ = writeln!(msg, "  - \"{c}\"");
        }
        errors.push(msg.trim_end().to_string());
    }

    for crate_name in expected_bazel_set.intersection(&declared_bazel_set) {
        let expected_dir = &all_expected_crates[crate_name];
        let declared_dir = &bazel_members[crate_name];
        if expected_dir != declared_dir {
            errors.push(format!(
                "MODULE.bazel SIBLING_MEMBERS: Path mismatch for '{crate_name}': expected '{expected_dir}', got '{declared_dir}'"
            ));
        }
    }

    Ok(VerificationResult {
        is_valid: errors.is_empty(),
        errors,
        expected_by_repo,
        patches,
    })
}

pub fn write_sync(
    cargo_toml_path: &Path,
    module_bazel_path: &Path,
    expected_by_repo: &BTreeMap<String, BTreeMap<String, String>>,
    patches: &BTreeMap<String, (String, Vec<String>)>,
) -> Result<(), VerifyError> {
    // 1. Rebuild [patch.crates-io] in Cargo.toml
    let cargo_content = fs::read_to_string(cargo_toml_path)?;

    let mut patch_lines = vec!["[patch.crates-io]".to_string()];
    let repo_order = [
        "https://github.com/krabka-io/krabka-broker",
        "https://github.com/krabka-io/krabka-schema-registry",
        "https://github.com/krabka-io/krabka-client-rs",
        "https://github.com/krabka-io/krabka-protocol",
    ];

    let mut ordered_urls: Vec<&str> = Vec::new();
    for url in repo_order {
        if expected_by_repo.contains_key(url) {
            ordered_urls.push(url);
        }
    }
    for url in expected_by_repo.keys() {
        if !ordered_urls.contains(&url.as_str()) {
            ordered_urls.push(url.as_str());
        }
    }

    for git_url in ordered_urls {
        let Some(packages) = expected_by_repo.get(git_url) else {
            continue;
        };
        let Some((rev, _)) = patches.get(git_url) else {
            continue;
        };

        if git_url == "https://github.com/krabka-io/krabka-schema-registry" {
            patch_lines.push(
                "# `krabka-broker` depends on `krabka-schema-serde`, so the git dependency\n\
                 # above needs it patched here too. Resolved from krabka-schema-registry, the\n\
                 # same source the broker itself patches, so the broker gets the copy it was\n\
                 # built against."
                    .to_string(),
            );
        }

        for crate_name in packages.keys() {
            patch_lines.push(format!(
                "{crate_name} = {{ git = \"{git_url}\", rev = \"{rev}\" }}"
            ));
        }
    }

    let new_patch_block = patch_lines.join("\n") + "\n";
    let patch_replace_regex = Regex::new(r"(?s)\[patch\.crates-io\]\s*.*?(?:\n\[|\z)")?;
    let updated_cargo = patch_replace_regex.replace(&cargo_content, format!("{new_patch_block}\n"));
    fs::write(cargo_toml_path, updated_cargo.as_bytes())?;
    println!("Updated {}", cargo_toml_path.display());

    // 2. Rebuild SIBLING_MEMBERS in MODULE.bazel
    let mut all_crates = BTreeMap::new();
    for packages in expected_by_repo.values() {
        all_crates.extend(packages.clone());
    }
    all_crates.remove("krabka-protocol");

    let mut bazel_lines = vec!["SIBLING_MEMBERS = {".to_string()];
    for (crate_name, dir) in &all_crates {
        bazel_lines.push(format!("    \"{crate_name}\": \"{dir}\","));
    }
    bazel_lines.push("}".to_string());
    let new_dict_str = bazel_lines.join("\n");

    let bazel_content = fs::read_to_string(module_bazel_path)?;
    let dict_replace_regex = Regex::new(r"(?s)SIBLING_MEMBERS\s*=\s*\{[^}]+\}")?;
    let updated_bazel = dict_replace_regex.replace(&bazel_content, new_dict_str);
    fs::write(module_bazel_path, updated_bazel.as_bytes())?;
    println!("Updated {}", module_bazel_path.display());

    Ok(())
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let mut sibling_dirs = BTreeMap::new();
    for entry in cli.sibling_dirs {
        if let Some((name, path)) = entry.split_once('=') {
            sibling_dirs.insert(name.to_string(), PathBuf::from(path));
        }
    }

    println!("Verifying sibling members against Cargo.toml and MODULE.bazel...");

    let res = match verify_workspace(&cli.cargo_toml, &cli.module_bazel, &sibling_dirs) {
        Ok(res) => res,
        Err(err) => {
            eprintln!("\nError verifying sibling members: {err}");
            return ExitCode::FAILURE;
        }
    };

    if !res.is_valid {
        if cli.write {
            if let Err(err) = write_sync(
                &cli.cargo_toml,
                &cli.module_bazel,
                &res.expected_by_repo,
                &res.patches,
            ) {
                eprintln!("\nError updating files: {err}");
                return ExitCode::FAILURE;
            }
            println!("\nSuccessfully updated Cargo.toml and MODULE.bazel.");
            return ExitCode::SUCCESS;
        }

        eprintln!("\nSibling member drift detected!\n");
        for err in &res.errors {
            eprintln!("ERROR: {err}\n");
        }
        eprintln!(
            "Run `cargo run -p verify-sibling-members -- --write` to update Cargo.toml and MODULE.bazel."
        );
        return ExitCode::FAILURE;
    }

    let total_crates: usize = res.expected_by_repo.values().map(BTreeMap::len).sum();
    println!(
        "OK: Verified {total_crates} member crates across {} sibling repositories in Cargo.toml and MODULE.bazel.",
        res.expected_by_repo.len()
    );

    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert2::assert;

    #[test]
    fn parses_cargo_toml_patch_table() {
        let cargo_toml = r#"
[workspace]
members = ["crates/*"]

[patch.crates-io]
krabka-broker = { git = "https://github.com/krabka-io/krabka-broker", rev = "32dd555ec1b7bf9d82b4b7bcdb0c9332f7029617" }
krabka-audit = { git = "https://github.com/krabka-io/krabka-broker", rev = "32dd555ec1b7bf9d82b4b7bcdb0c9332f7029617" }
krabka-schema-serde = { git = "https://github.com/krabka-io/krabka-schema-registry", rev = "ee9c3933fb1d71735846e7e5e33ea6ab3a694dc5" }
"#;

        let patches = parse_cargo_toml_patches(cargo_toml).expect("patches parsed");
        assert!(patches.len() == 2);
        assert!(
            patches["https://github.com/krabka-io/krabka-broker"].0
                == "32dd555ec1b7bf9d82b4b7bcdb0c9332f7029617"
        );
        assert!(
            patches["https://github.com/krabka-io/krabka-broker"]
                .1
                .len()
                == 2
        );
        assert!(
            patches["https://github.com/krabka-io/krabka-schema-registry"].0
                == "ee9c3933fb1d71735846e7e5e33ea6ab3a694dc5"
        );
    }

    #[test]
    fn errors_on_multiple_revisions_for_same_repo() {
        let cargo_toml = r#"
[patch.crates-io]
krabka-broker = { git = "https://github.com/krabka-io/krabka-broker", rev = "1111111111111111111111111111111111111111" }
krabka-audit = { git = "https://github.com/krabka-io/krabka-broker", rev = "2222222222222222222222222222222222222222" }
"#;

        let res = parse_cargo_toml_patches(cargo_toml);
        assert!(res.is_err());
    }

    #[test]
    fn parses_module_bazel_members_dict() {
        let module_bazel = r#"
SIBLING_MEMBERS = {
    "krabka-audit": "crates/audit",
    "krabka-broker": "crates/broker",
}

crate.annotation(
    crate = "krabka-protocol",
    gen_build_script = "off",
    strip_prefix = "crates/protocol",
    workspace_cargo_toml = "Cargo.toml",
)
"#;

        let members = parse_module_bazel_members(module_bazel).expect("members parsed");
        assert!(members.len() == 2);
        assert!(members["krabka-audit"] == "crates/audit");
        assert!(members["krabka-broker"] == "crates/broker");
        assert!(check_krabka_protocol_annotation(module_bazel));
    }

    #[test]
    fn write_sync_generates_expected_files() {
        let temp_dir = tempfile::tempdir().expect("temp dir created");
        let cargo_path = temp_dir.path().join("Cargo.toml");
        let bazel_path = temp_dir.path().join("MODULE.bazel");

        let initial_cargo = r#"
[workspace]
members = ["crates/*"]

[patch.crates-io]
old-crate = { git = "https://github.com/krabka-io/krabka-broker", rev = "32dd555ec1b7bf9d82b4b7bcdb0c9332f7029617" }
"#;

        let initial_bazel = r#"
SIBLING_MEMBERS = {
    "old-crate": "crates/old",
}

crate.annotation(
    crate = "krabka-protocol",
    gen_build_script = "off",
    strip_prefix = "crates/protocol",
    workspace_cargo_toml = "Cargo.toml",
)
"#;

        fs::write(&cargo_path, initial_cargo).expect("wrote initial cargo");
        fs::write(&bazel_path, initial_bazel).expect("wrote initial bazel");

        let mut expected_by_repo = BTreeMap::new();
        let mut broker_packages = BTreeMap::new();
        broker_packages.insert("krabka-audit".to_string(), "crates/audit".to_string());
        broker_packages.insert("krabka-broker".to_string(), "crates/broker".to_string());
        expected_by_repo.insert(
            "https://github.com/krabka-io/krabka-broker".to_string(),
            broker_packages,
        );

        let mut patches = BTreeMap::new();
        patches.insert(
            "https://github.com/krabka-io/krabka-broker".to_string(),
            (
                "32dd555ec1b7bf9d82b4b7bcdb0c9332f7029617".to_string(),
                vec![],
            ),
        );

        write_sync(&cargo_path, &bazel_path, &expected_by_repo, &patches).expect("write sync ok");

        let updated_cargo = fs::read_to_string(&cargo_path).expect("read cargo");
        let updated_bazel = fs::read_to_string(&bazel_path).expect("read bazel");

        assert!(
            updated_cargo
                .contains("krabka-audit = { git = \"https://github.com/krabka-io/krabka-broker\"")
        );
        assert!(
            updated_cargo
                .contains("krabka-broker = { git = \"https://github.com/krabka-io/krabka-broker\"")
        );
        assert!(!updated_cargo.contains("old-crate"));

        assert!(updated_bazel.contains("\"krabka-audit\": \"crates/audit\""));
        assert!(updated_bazel.contains("\"krabka-broker\": \"crates/broker\""));
        assert!(!updated_bazel.contains("\"old-crate\""));
        assert!(check_krabka_protocol_annotation(&updated_bazel));
    }
}
