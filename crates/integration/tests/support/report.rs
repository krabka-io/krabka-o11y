//! The versioned `soak-report.json`, and where it is written.

use std::{
    path::PathBuf,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::{Value, json};

use super::{config::Config, minio, phases::PHASES, rss::host_memory_kib, runner::wal_lag};

/// The report format. `tools/soak-gate.py` refuses any other.
///
/// Version 1 was the logs-only soak in `krabka-observability`, which this
/// harness replaced. Version 2 is one entry per signal and phase.
pub const SCHEMA_VERSION: u64 = 2;

/// Assembles the report around `entries`.
pub fn build(config: &Config, entries: &[Value], total_seconds: f64) -> Value {
    let generated = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_secs());
    let commit = env("KRABKA_SOAK_COMMIT").or_else(git_head);
    let run_id = env("KRABKA_SOAK_RUN_ID").unwrap_or_else(|| {
        let short = commit.as_deref().map_or("local", |c| &c[..c.len().min(12)]);
        format!("{short}-{generated}-{}", std::process::id())
    });
    json!({
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "generated_at_unix": generated,
        "total_seconds": total_seconds,
        "commit": commit,
        "image_digest": env("KRABKA_SOAK_IMAGE_DIGEST"),
        "minio_image": { "ref": minio::image_ref(), "id": minio::image_id() },
        "host": {
            "cpus": std::thread::available_parallelism().map_or(0, std::num::NonZero::get),
            "memory_kib": host_memory_kib(),
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "runner": env("KRABKA_SOAK_RUNNER"),
        },
        "rustc": rustc_version(),
        "command": env("KRABKA_SOAK_COMMAND")
            .unwrap_or_else(|| std::env::args().collect::<Vec<_>>().join(" ")),
        "dataset": config.dataset(),
        "shape": config.report(),
        "objectives": config.objectives.report(),
        "phases": PHASES,
        "wal_lag": wal_lag(),
        "entries": entries,
    })
}

/// `KRABKA_SOAK_RUSTC` when the build sets it, else `rustc --version` from
/// the path, else a statement that it was not measured.
fn rustc_version() -> Value {
    if let Some(version) = env("KRABKA_SOAK_RUSTC") {
        return json!(version);
    }
    Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map_or_else(
            || json!({ "status": "not_measured", "reason": "no rustc on PATH and KRABKA_SOAK_RUSTC unset" }),
            |version| json!(version.trim()),
        )
}

/// `git rev-parse HEAD`, for a local run that did not set
/// `KRABKA_SOAK_COMMIT`. A run outside a checkout reports no commit, and the
/// gate refuses that report.
fn git_head() -> Option<String> {
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|head| head.trim().to_string())
        .filter(|head| !head.is_empty())
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

/// Prints the report and writes it to every place a caller asked for:
/// Bazel's undeclared outputs, and `KRABKA_SOAK_REPORT`.
pub fn write(report: &Value) -> Vec<PathBuf> {
    let text = serde_json::to_string_pretty(report).expect("the report serializes");
    println!("{text}");
    let mut targets = Vec::new();
    if let Some(dir) = env("TEST_UNDECLARED_OUTPUTS_DIR") {
        targets.push(PathBuf::from(dir).join("soak-report.json"));
    }
    if let Some(path) = env("KRABKA_SOAK_REPORT") {
        targets.push(PathBuf::from(path));
    }
    for target in &targets {
        std::fs::write(target, &text)
            .unwrap_or_else(|error| panic!("{} is writable: {error}", target.display()));
    }
    targets
}
