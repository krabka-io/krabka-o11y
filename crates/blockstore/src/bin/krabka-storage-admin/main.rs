use std::{collections::BTreeSet, fs::OpenOptions, path::PathBuf, sync::Arc, time::SystemTime};

use clap::{Parser, Subcommand};
use krabka_blockstore::{
    RepairOptions, StorageAuditOptions, StorageFindingKind, StorageSignal, TenantId, WalOffset,
    audit_backup, audit_recovery_target, audit_store, create_backup, load_backup_manifest,
    repair_store, restore_backup,
};
use krabka_units::Time;
use object_store::{ObjectStore, parse_url_opts, path::Path as ObjectPath, prefix::PrefixStore};
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
    /// Audit the blocks, indexes and state of a live store. Reads only.
    AuditStore {
        /// The root the services write to. A URL with no path audits the
        /// whole bucket.
        #[arg(long)]
        store_url: String,
        #[arg(long)]
        signal: Option<StorageSignal>,
        #[arg(long)]
        tenant: Option<String>,
        /// Unindexed objects younger than this are pending, not orphans.
        #[arg(long, value_parser = krabka_units::parse::non_negative_time, default_value = "1h")]
        grace: Time,
        /// Decode every row of every block, not only the footers, and the
        /// symbol table of every live profiles block.
        #[arg(long)]
        verify_data: bool,
        #[arg(long, default_value = "index/traces.json")]
        trace_index_key: String,
        #[arg(long, default_value = "index/profiles.json")]
        profile_index_key: String,
        #[arg(long)]
        report: Option<PathBuf>,
    },
    /// Delete the orphans an audit of one tenant finds. Plans unless
    /// `--apply` is set.
    Repair {
        #[arg(long)]
        store_url: String,
        #[arg(long)]
        tenant: String,
        #[arg(long)]
        signal: StorageSignal,
        /// A finding kind to act on. Repeat for more than one.
        #[arg(long = "finding-kind", required = true)]
        finding_kinds: Vec<StorageFindingKind>,
        #[arg(long, value_parser = krabka_units::parse::non_negative_time, default_value = "1h")]
        grace: Time,
        #[arg(long, default_value = "index/traces.json")]
        trace_index_key: String,
        #[arg(long, default_value = "index/profiles.json")]
        profile_index_key: String,
        #[arg(long)]
        apply: bool,
        #[arg(long)]
        report: Option<PathBuf>,
        /// The JSON Lines file each action is appended to.
        #[arg(long)]
        audit_log: PathBuf,
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
        Command::AuditStore {
            store_url,
            signal,
            tenant,
            grace,
            verify_data,
            trace_index_key,
            profile_index_key,
            report,
        } => {
            let options = StorageAuditOptions {
                signal,
                tenant,
                grace,
                verify_data,
                now: SystemTime::now(),
                trace_index_key,
                profile_index_key,
            };
            let audit = audit_store(&root_store(&store_url)?, &options)
                .await
                .map_err(|error| error.to_string())?;
            emit(&audit, report)?;
            if audit.has_damage() {
                return Err("audit found storage damage".into());
            }
        }
        Command::Repair {
            store_url,
            tenant,
            signal,
            finding_kinds,
            grace,
            trace_index_key,
            profile_index_key,
            apply,
            report,
            audit_log,
        } => {
            let options = RepairOptions {
                tenant,
                signal,
                kinds: finding_kinds.into_iter().collect::<BTreeSet<_>>(),
                apply,
                grace,
                now: SystemTime::now(),
                trace_index_key,
                profile_index_key,
            };
            options.validate().map_err(|error| error.to_string())?;
            let mut log = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&audit_log)
                .map_err(|error| format!("cannot open {}: {error}", audit_log.display()))?;
            let result = repair_store(&root_store(&store_url)?, &options, &mut log)
                .await
                .map_err(|error| error.to_string())?;
            emit(&result, report)?;
            if result.has_failures() {
                return Err("repair could not act on every object; run it again".into());
            }
        }
    }
    Ok(())
}

/// Parses an object-store URL into its store and the path prefix it names.
fn parse_store_url(raw: &str) -> Result<(Box<dyn ObjectStore>, ObjectPath), String> {
    let url = Url::parse(raw).map_err(|error| format!("invalid object-store URL: {error}"))?;
    parse_url_opts(&url, std::env::vars())
        .map_err(|error| format!("object-store configuration failed: {error}"))
}

fn root_store(raw: &str) -> Result<Arc<dyn ObjectStore>, String> {
    let (store, prefix) = parse_store_url(raw)?;
    if prefix.as_ref().is_empty() {
        return Ok(Arc::from(store));
    }
    Ok(Arc::new(PrefixStore::new(store, prefix)))
}

fn scoped_store(raw: &str) -> Result<Arc<dyn ObjectStore>, String> {
    let (store, prefix) = parse_store_url(raw)?;
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
    use krabka_blockstore::DEFAULT_BLOCK_SWEEP_GRACE;

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

    #[test]
    fn stores_use_distinct_dedicated_prefixes() {
        assert!(scoped_store("not a URL").is_err());
        assert!(scoped_store("memory:///").is_err());
        assert!(scoped_store("memory:///tenant-a").is_ok());
        assert!(require_distinct("memory:///a", "memory:///a").is_err());
        assert!(require_distinct("memory:///a", "memory:///b").is_ok());
    }

    #[test]
    fn repair_needs_tenant_signal_and_finding_kinds() {
        let parsed = |args: &[&str]| {
            Cli::try_parse_from(
                [
                    "krabka-storage-admin",
                    "repair",
                    "--store-url",
                    "memory:///",
                ]
                .iter()
                .chain(args),
            )
        };
        let full = [
            "--tenant",
            "t",
            "--signal",
            "traces",
            "--finding-kind",
            "orphan",
            "--audit-log",
            "log.jsonl",
        ];
        for missing in ["--tenant", "--signal", "--finding-kind", "--audit-log"] {
            let at = full.iter().position(|arg| *arg == missing).unwrap();
            let args = [&full[..at], &full[at + 2..]].concat();
            check!(parsed(&args).is_err(), "{missing}");
        }
        let Command::Repair {
            tenant,
            signal,
            finding_kinds,
            apply,
            grace,
            ..
        } = parsed(&full).unwrap().command
        else {
            panic!("expected repair");
        };
        check!(
            (tenant, signal, finding_kinds, apply, grace)
                == (
                    "t".to_string(),
                    StorageSignal::Traces,
                    vec![StorageFindingKind::Orphan],
                    false,
                    DEFAULT_BLOCK_SWEEP_GRACE,
                )
        );
    }

    #[test]
    fn audit_store_defaults_to_every_signal_and_tenant() {
        let cli = Cli::try_parse_from([
            "krabka-storage-admin",
            "audit-store",
            "--store-url",
            "memory:///",
            "--grace",
            "10m",
        ])
        .unwrap();
        let Command::AuditStore {
            signal,
            tenant,
            grace,
            verify_data,
            trace_index_key,
            profile_index_key,
            ..
        } = cli.command
        else {
            panic!("expected audit-store");
        };
        check!(
            (
                signal,
                tenant,
                grace,
                verify_data,
                trace_index_key,
                profile_index_key
            ) == (
                None,
                None,
                krabka_units::minutes(10),
                false,
                "index/traces.json".to_string(),
                "index/profiles.json".to_string(),
            )
        );
    }

    #[test]
    fn audited_stores_may_be_a_bucket_root() {
        assert!(root_store("memory:///").is_ok());
        assert!(root_store("memory:///prefix").is_ok());
        assert!(root_store("not a URL").is_err());
    }

    #[tokio::test]
    async fn audit_store_and_plan_only_repair_run_against_an_empty_store() {
        let directory = tempfile::tempdir().unwrap();
        let report = directory.path().join("report.json");
        let audit_log = directory.path().join("repair.jsonl");
        let audit = Cli::try_parse_from([
            "krabka-storage-admin",
            "audit-store",
            "--store-url",
            "memory:///",
            "--report",
            report.to_str().unwrap(),
        ])
        .unwrap();
        run(audit).await.unwrap();
        let written =
            krabka_blockstore::StorageAuditReport::from_json(&std::fs::read(&report).unwrap())
                .unwrap();
        check!(written.findings.is_empty());

        let repair = Cli::try_parse_from([
            "krabka-storage-admin",
            "repair",
            "--store-url",
            "memory:///",
            "--tenant",
            "t",
            "--signal",
            "profiles",
            "--finding-kind",
            "orphan_sidecar",
            "--audit-log",
            audit_log.to_str().unwrap(),
            "--report",
            report.to_str().unwrap(),
        ])
        .unwrap();
        run(repair).await.unwrap();
        check!(std::fs::read(&audit_log).unwrap().is_empty());
    }

    #[tokio::test]
    async fn repair_refuses_a_kind_that_needs_a_person() {
        let directory = tempfile::tempdir().unwrap();
        let audit_log = directory.path().join("repair.jsonl");
        let repair = Cli::try_parse_from([
            "krabka-storage-admin",
            "repair",
            "--store-url",
            "memory:///",
            "--tenant",
            "t",
            "--signal",
            "metrics",
            "--finding-kind",
            "corrupt_block",
            "--apply",
            "--audit-log",
            audit_log.to_str().unwrap(),
        ])
        .unwrap();
        assert!(run(repair).await.is_err());
        check!(!audit_log.exists());
    }

    #[test]
    fn reports_can_be_written_to_a_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("report.json");

        emit(&serde_json::json!({"clean": true}), Some(path.clone())).unwrap();

        check!(std::fs::read_to_string(path).unwrap() == "{\n  \"clean\": true\n}\n");
    }
}
