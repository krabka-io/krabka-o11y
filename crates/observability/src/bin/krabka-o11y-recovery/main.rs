//! Backs up, audits, and restores a whole deployment as one consistent cut.
//!
//! A cut binds one part for each store of the deployment to the broker state
//! at the cut: the next offset of every partition of the six topics, and every
//! committed consumer-group offset on them. `docs/disaster_recovery.md` is the
//! procedure. Every mutating command needs `--apply`.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::{Parser, Subcommand};
use krabka_blockstore::{
    DeploymentBackupPlan, DeploymentPart, RecoveryError, audit_deployment_backup,
    backup_deployment, restore_deployment_backup,
};
use krabka_observability::{
    recovery_cut::{KafkaBrokerState, deployment_drained_groups},
    server_security::install_crypto_provider,
    topic_contract::ALL_TOPICS,
    wal_client_security::WalClientSecurityArgs,
};
use object_store::{ObjectStore, local::LocalFileSystem, parse_url_opts, prefix::PrefixStore};
use serde::Serialize;
use url::Url;

use self::failure::failure;

mod failure;

#[derive(Debug, Parser)]
#[command(name = "krabka-o11y-recovery")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Copy every part of a quiesced deployment and seal the cut.
    Backup {
        #[command(flatten)]
        broker: BrokerArgs,
        /// An empty, dedicated object-store prefix for the backup set.
        #[arg(long)]
        backup_url: String,
        #[arg(long)]
        cut_id: String,
        /// The name of the broker-side capture, for example the
        /// `krabka-backup` capture id.
        #[arg(long)]
        broker_capture: String,
        /// One part as `NAME=URL`. A `file://` URL names a local directory,
        /// such as the logs `data-root`.
        #[arg(long = "part", value_parser = parse_part, required = true)]
        parts: Vec<(String, String)>,
        #[arg(long)]
        apply: bool,
        #[arg(long)]
        report: Option<PathBuf>,
    },
    /// Verify a backup set without changing it.
    Audit {
        #[arg(long)]
        backup_url: String,
        #[arg(long)]
        report: Option<PathBuf>,
    },
    /// Restore every part into empty targets after the broker is restored.
    Restore {
        #[command(flatten)]
        broker: BrokerArgs,
        #[arg(long)]
        backup_url: String,
        /// One target as `NAME=URL`, for every part of the cut.
        #[arg(long = "part", value_parser = parse_part, required = true)]
        parts: Vec<(String, String)>,
        #[arg(long)]
        apply: bool,
        #[arg(long)]
        report: Option<PathBuf>,
    },
}

#[derive(Debug, clap::Args)]
struct BrokerArgs {
    #[arg(long, env = "KRABKA_BOOTSTRAP_SERVER")]
    bootstrap: String,
    #[command(flatten)]
    wal_client_security: WalClientSecurityArgs,
}

impl BrokerArgs {
    fn state(&self) -> Result<KafkaBrokerState, String> {
        let security = self
            .wal_client_security
            .load()
            .map_err(|error| error.to_string())?;
        Ok(KafkaBrokerState::new(
            self.bootstrap.clone(),
            security,
            ALL_TOPICS.iter().map(|topic| topic.name),
        ))
    }
}

#[tokio::main]
async fn main() {
    install_crypto_provider();
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("krabka-o11y-recovery: {error}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), String> {
    match cli.command {
        Command::Backup {
            broker,
            backup_url,
            cut_id,
            broker_capture,
            parts,
            apply,
            report,
        } => {
            require_apply(apply, "backup")?;
            let plan = DeploymentBackupPlan {
                cut_id,
                broker_capture,
                drained_groups: deployment_drained_groups(),
                parts: open_parts(&parts, Some(&backup_url))?,
            };
            let result = backup_deployment(&broker.state()?, &open_store(&backup_url)?, &plan)
                .await
                .map_err(|error| failure(error, report.as_deref()))?;
            emit(&result, report)
        }
        Command::Audit { backup_url, report } => {
            let audit = audit_deployment_backup(&open_store(&backup_url)?)
                .await
                .map_err(|error| error.to_string())?;
            emit(&audit, report)?;
            if audit
                .parts
                .values()
                .all(krabka_blockstore::AuditReport::is_clean)
            {
                Ok(())
            } else {
                Err("audit found storage damage".into())
            }
        }
        Command::Restore {
            broker,
            backup_url,
            parts,
            apply,
            report,
        } => {
            require_apply(apply, "restore")?;
            let result = restore_deployment_backup(
                &open_store(&backup_url)?,
                &broker.state()?,
                &open_parts(&parts, Some(&backup_url))?,
            )
            .await
            .map_err(|error| failure(error, report.as_deref()))?;
            emit(&result, report)
        }
    }
}

fn parse_part(raw: &str) -> Result<(String, String), String> {
    let (name, url) = raw.split_once('=').ok_or("expected NAME=URL")?;
    if name.is_empty() || url.is_empty() {
        return Err("expected NAME=URL".into());
    }
    Ok((name.into(), url.into()))
}

fn open_parts(
    parts: &[(String, String)],
    backup_url: Option<&str>,
) -> Result<Vec<DeploymentPart>, String> {
    parts
        .iter()
        .map(|(name, url)| {
            if Some(url.as_str()) == backup_url {
                return Err(format!("part `{name}` and the backup set use one URL"));
            }
            Ok(DeploymentPart {
                name: name.clone(),
                store: open_store(url)?,
            })
        })
        .collect()
}

/// An object store rooted at `raw`. A `file://` URL is a local directory, and
/// every other URL must name a dedicated prefix.
fn open_store(raw: &str) -> Result<Arc<dyn ObjectStore>, String> {
    let url = Url::parse(raw).map_err(|error| format!("invalid store URL `{raw}`: {error}"))?;
    if url.scheme() == "file" {
        let path = url
            .to_file_path()
            .map_err(|()| format!("`{raw}` is not a local path"))?;
        std::fs::create_dir_all(&path).map_err(|error| error.to_string())?;
        return Ok(Arc::new(
            LocalFileSystem::new_with_prefix(path).map_err(|error| error.to_string())?,
        ));
    }
    let (store, prefix) = parse_url_opts(&url, std::env::vars())
        .map_err(|error| format!("object-store configuration failed: {error}"))?;
    if prefix.as_ref().is_empty() {
        return Ok(Arc::from(store));
    }
    Ok(Arc::new(PrefixStore::new(store, prefix)))
}

fn require_apply(apply: bool, operation: &str) -> Result<(), String> {
    apply
        .then_some(())
        .ok_or_else(|| format!("{operation} is mutating; pass --apply explicitly"))
}

fn emit<T: Serialize>(value: &T, path: Option<PathBuf>) -> Result<(), String> {
    if let Some(path) = path {
        write_report(value, &path)
    } else {
        print!("{}", String::from_utf8_lossy(&report_bytes(value)?));
        Ok(())
    }
}

fn write_report<T: Serialize>(value: &T, path: &Path) -> Result<(), String> {
    std::fs::write(path, report_bytes(value)?).map_err(|error| error.to_string())
}

fn report_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    let mut bytes = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    bytes.push(b'\n');
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn parts_are_named_urls() {
        let cases = [
            (
                "metrics=s3://krabka-metrics",
                Ok(("metrics", "s3://krabka-metrics")),
            ),
            ("metrics", Err(())),
            ("=s3://krabka-metrics", Err(())),
            ("metrics=", Err(())),
        ];
        for (raw, expected) in cases {
            let parsed = parse_part(raw);
            check!(
                parsed
                    .as_ref()
                    .map(|(name, url)| (name.as_str(), url.as_str()))
                    .map_err(|_| ())
                    == expected,
                "{raw}"
            );
        }
    }

    #[test]
    fn mutations_require_an_explicit_apply_flag() {
        assert!(require_apply(false, "restore").is_err());
        assert!(require_apply(true, "restore").is_ok());
    }

    #[test]
    fn a_part_may_not_share_the_backup_url() {
        let parts = [("metrics".to_string(), "memory:///a".to_string())];
        assert!(open_parts(&parts, Some("memory:///a")).is_err());
        assert!(open_parts(&parts, Some("memory:///b")).is_ok());
    }

    #[test]
    fn a_file_url_opens_a_local_directory() {
        let directory = tempfile::tempdir().expect("tempdir");
        let url = Url::from_directory_path(directory.path().join("data-root"))
            .expect("file URL")
            .to_string();
        assert!(open_store(&url).is_ok());
        assert!(directory.path().join("data-root").is_dir());
        assert!(open_store("not a url").is_err());
    }
}
