//! Backs up, audits, and restores a whole deployment as one consistent cut.
//!
//! A cut binds one part for each store of the deployment to the broker state
//! at the cut: the next offset of every partition of the deployment's WAL and
//! state topics, and every committed consumer-group offset on them.
//! `docs/disaster_recovery.md` is the procedure. Every mutating command needs
//! `--apply`.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
};

use clap::{Parser, Subcommand};
use krabka_blockstore::{
    DeploymentBackupPlan, DeploymentPart, RecoveryError, audit_deployment_backup,
    backup_deployment, restore_deployment_backup,
};
use krabka_observability::{
    recovery_cut::{DeploymentKafkaNames, KafkaBrokerState},
    server_security::install_crypto_provider,
    wal_client_security::WalClientSecurityArgs,
};
use object_store::{ObjectStore, local::LocalFileSystem, parse_url_opts, prefix::PrefixStore};
use serde::Serialize;
use url::Url;

use self::{
    failure::failure, refuse_missing_parts::refuse_missing_parts,
    refuse_overlapping_stores::refuse_overlapping_stores,
};

mod failure;
mod refuse_missing_parts;
mod refuse_overlapping_stores;

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
        /// A deployment part that this deployment does not have, such as
        /// `profiles` when no profiles role runs. The cut records it.
        #[arg(long = "omit-part")]
        omitted_parts: Vec<String>,
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
    names: DeploymentKafkaNames,
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
            self.names.topics(),
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
            omitted_parts,
            apply,
            report,
        } => {
            require_apply(apply, "backup")?;
            refuse_missing_parts(
                &parts
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>(),
                &omitted_parts,
            )?;
            let plan = DeploymentBackupPlan {
                cut_id,
                broker_capture,
                drained_groups: broker.names.drained_groups(),
                parts: open_parts(&parts, &backup_url)?,
                omitted_parts,
            };
            let result = backup_deployment(&broker.state()?, &open_store(&backup_url)?, &plan)
                .await
                .map_err(|error| failure(&error, report.as_deref()))?;
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
                &open_parts(&parts, &backup_url)?,
            )
            .await
            .map_err(|error| failure(&error, report.as_deref()))?;
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

/// Opens every part store, after it refuses parts that overlap each other or
/// the backup set. No store is opened when one overlaps.
fn open_parts(parts: &[(String, String)], backup_url: &str) -> Result<Vec<DeploymentPart>, String> {
    let stores = std::iter::once(("the backup set".to_string(), backup_url))
        .chain(
            parts
                .iter()
                .map(|(name, url)| (format!("part `{name}`"), url.as_str())),
        )
        .collect::<Vec<_>>();
    refuse_overlapping_stores(&stores)?;
    parts
        .iter()
        .map(|(name, url)| {
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
    fn a_part_may_not_overlap_the_backup_set_or_another_part() {
        let directory = tempfile::tempdir().expect("tempdir");
        let url = |name: &str| {
            Url::from_directory_path(directory.path().join(name))
                .expect("file URL")
                .to_string()
        };
        let part = |name: &str, dir: &str| (name.to_string(), url(dir));
        let cases = [
            (vec![part("metrics", "metrics")], "backup", true),
            (vec![part("metrics", "backup")], "backup", false),
            (vec![part("metrics", "backup/metrics")], "backup", false),
            (
                vec![part("metrics", "target"), part("logs", "target")],
                "backup",
                false,
            ),
            (
                vec![part("metrics", "target"), part("logs", "target/logs")],
                "backup",
                false,
            ),
        ];
        for (parts, backup, accepted) in cases {
            check!(
                open_parts(&parts, &url(backup)).is_ok() == accepted,
                "{parts:?}"
            );
            // A refused command opens no store, so it creates no directory.
            if !accepted {
                for (_, dir) in &parts {
                    let path = Url::parse(dir).expect("URL").to_file_path().expect("path");
                    check!(!path.exists(), "{dir}");
                }
            }
        }
    }

    #[test]
    fn the_broker_arguments_name_the_deployment_topics_and_groups() {
        let defaults = Cli::try_parse_from([
            "krabka-o11y-recovery",
            "restore",
            "--bootstrap",
            "broker:9092",
            "--backup-url",
            "s3://backups/cut-1",
            "--part",
            "metrics=s3://restore-metrics",
        ])
        .expect("defaults");
        let Command::Restore { broker, .. } = defaults.command else {
            panic!("restore");
        };
        check!(broker.names == DeploymentKafkaNames::default());

        let custom = Cli::try_parse_from([
            "krabka-o11y-recovery",
            "backup",
            "--bootstrap",
            "broker:9092",
            "--backup-url",
            "s3://backups/cut-1",
            "--cut-id",
            "cut-1",
            "--broker-capture",
            "snapshot-1",
            "--part",
            "metrics=s3://krabka-metrics",
            "--omit-part",
            "profiles",
            "--metrics-wal-topic",
            "metrics-a",
            "--metrics-ha-tracker-topic",
            "ha-a",
            "--metrics-ruler-state-topic",
            "ruler-a",
            "--logs-wal-topic",
            "logs-a",
            "--profiles-wal-topic",
            "profiles-a",
            "--metrics-block-builder-group-id",
            "metrics-builder-a",
            "--logs-wal-group-id",
            "logs-builder-a",
            "--profiles-block-builder-group-id",
            "profiles-builder-a",
        ])
        .expect("custom names");
        let Command::Backup {
            broker,
            omitted_parts,
            ..
        } = custom.command
        else {
            panic!("backup");
        };
        check!(omitted_parts == ["profiles"]);
        check!(
            broker.names
                == DeploymentKafkaNames {
                    metrics_wal_topic: "metrics-a".into(),
                    metrics_ha_tracker_topic: "ha-a".into(),
                    metrics_ruler_state_topic: "ruler-a".into(),
                    logs_wal_topic: "logs-a".into(),
                    profiles_wal_topic: "profiles-a".into(),
                    metrics_block_builder_group_id: "metrics-builder-a".into(),
                    logs_wal_group_id: "logs-builder-a".into(),
                    profiles_block_builder_group_id: "profiles-builder-a".into(),
                }
        );
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
