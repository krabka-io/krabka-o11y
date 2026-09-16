use std::{path::PathBuf, sync::Arc};

use clap::{Parser, Subcommand};
use krabka_blockstore::{
    TenantId, WalOffset, audit_backup, audit_recovery_target, create_backup, load_backup_manifest,
    restore_backup,
};
use object_store::{ObjectStore, parse_url_opts, prefix::PrefixStore};
use serde::Serialize;
use url::Url;

#[derive(Debug, Parser)]
#[command(name = "krabka-storage-admin")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Compare a target prefix with a completed backup manifest.
    Audit {
        #[arg(long)]
        backup_url: String,
        #[arg(long)]
        target_url: String,
        #[arg(long)]
        report: Option<PathBuf>,
    },
    /// Copy a quiesced tenant prefix into a checksummed backup set.
    Backup {
        #[arg(long)]
        source_url: String,
        #[arg(long)]
        backup_url: String,
        #[arg(long)]
        tenant: TenantId,
        #[arg(long)]
        cut_id: String,
        #[arg(long = "wal-offset", value_parser = parse_wal_offset, required = true)]
        wal_offsets: Vec<WalOffset>,
        #[arg(long)]
        apply: bool,
        #[arg(long)]
        report: Option<PathBuf>,
    },
    /// Restore or resume a completed backup into a dedicated empty prefix.
    Restore {
        #[arg(long)]
        backup_url: String,
        #[arg(long)]
        target_url: String,
        #[arg(long)]
        apply: bool,
        #[arg(long)]
        report: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(Cli::parse()).await {
        eprintln!("krabka-storage-admin: {error}");
        std::process::exit(1);
    }
}

async fn run(cli: Cli) -> Result<(), String> {
    match cli.command {
        Command::Audit {
            backup_url,
            target_url,
            report,
        } => {
            let backup = scoped_store(&backup_url)?;
            let target = scoped_store(&target_url)?;
            let manifest = load_backup_manifest(backup.as_ref())
                .await
                .map_err(|error| error.to_string())?;
            let audit = if backup_url == target_url {
                audit_backup(target.as_ref(), &manifest).await
            } else {
                audit_recovery_target(target.as_ref(), &manifest).await
            }
            .map_err(|error| error.to_string())?;
            emit(&audit, report)?;
            if !audit.is_clean() {
                return Err("audit found storage damage".into());
            }
        }
        Command::Backup {
            source_url,
            backup_url,
            tenant,
            cut_id,
            wal_offsets,
            apply,
            report,
        } => {
            require_apply(apply, "backup")?;
            require_distinct(&source_url, &backup_url)?;
            let result = create_backup(
                scoped_store(&source_url)?,
                scoped_store(&backup_url)?,
                tenant,
                cut_id,
                wal_offsets,
            )
            .await
            .map_err(|error| error.to_string())?;
            emit(&result, report)?;
        }
        Command::Restore {
            backup_url,
            target_url,
            apply,
            report,
        } => {
            require_apply(apply, "restore")?;
            require_distinct(&backup_url, &target_url)?;
            let result = restore_backup(scoped_store(&backup_url)?, scoped_store(&target_url)?)
                .await
                .map_err(|error| error.to_string())?;
            emit(&result, report)?;
        }
    }
    Ok(())
}

fn scoped_store(raw: &str) -> Result<Arc<dyn ObjectStore>, String> {
    let url = Url::parse(raw).map_err(|error| format!("invalid object-store URL: {error}"))?;
    let (store, prefix) = parse_url_opts(&url, std::env::vars())
        .map_err(|error| format!("object-store configuration failed: {error}"))?;
    if prefix.as_ref().is_empty() {
        return Err("object-store URL must include a dedicated prefix".into());
    }
    Ok(Arc::new(PrefixStore::new(store, prefix)))
}

fn parse_wal_offset(raw: &str) -> Result<WalOffset, String> {
    let mut fields = raw.split(':');
    let topic = fields.next().unwrap_or_default();
    let partition = fields
        .next()
        .ok_or("expected TOPIC:PARTITION:NEXT_OFFSET")?
        .parse()
        .map_err(|_| "WAL partition must be an integer")?;
    let next_offset = fields
        .next()
        .ok_or("expected TOPIC:PARTITION:NEXT_OFFSET")?
        .parse()
        .map_err(|_| "WAL offset must be an integer")?;
    if topic.is_empty() || fields.next().is_some() {
        return Err("expected TOPIC:PARTITION:NEXT_OFFSET".into());
    }
    Ok(WalOffset {
        topic: topic.into(),
        partition,
        next_offset,
    })
}

fn require_apply(apply: bool, operation: &str) -> Result<(), String> {
    apply
        .then_some(())
        .ok_or_else(|| format!("{operation} is mutating; pass --apply explicitly"))
}

fn require_distinct(left: &str, right: &str) -> Result<(), String> {
    if left == right {
        return Err("source and target object-store URLs must differ".into());
    }
    Ok(())
}

fn emit<T: Serialize>(value: &T, path: Option<PathBuf>) -> Result<(), String> {
    let mut bytes = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    bytes.push(b'\n');
    if let Some(path) = path {
        std::fs::write(path, bytes).map_err(|error| error.to_string())
    } else {
        print!("{}", String::from_utf8(bytes).expect("JSON is UTF-8"));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn wal_offsets_are_explicit_partition_cuts() {
        check!(
            parse_wal_offset("krabka.metrics.wal:2:42")
                == Ok(WalOffset {
                    topic: "krabka.metrics.wal".into(),
                    partition: 2,
                    next_offset: 42,
                })
        );
        assert!(parse_wal_offset("topic:2").is_err());
        assert!(parse_wal_offset("topic:2:42:extra").is_err());
    }

    #[test]
    fn mutations_require_an_explicit_apply_flag() {
        assert!(require_apply(false, "restore").is_err());
        assert!(require_apply(true, "restore").is_ok());
    }
}
