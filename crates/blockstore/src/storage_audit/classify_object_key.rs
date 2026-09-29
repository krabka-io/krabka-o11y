use super::{
    BlockShape, ClassifiedObject, ObjectRole, StorageAuditOptions, StorageSignal,
    index_shards_prefix_for_key, index_snapshot_prefix_for_key, shard_payload_prefix_for_key,
    unescape_object_path_segment,
};

const METRIC_KINDS: [&str; 5] = [
    "float",
    "native-histograms",
    "exemplars",
    "metadata",
    "clock-readings",
];

/// Matches `key` against the key grammar of every signal.
///
/// Returns `None` for a key that no grammar matches. The audit counts such a
/// key and does not report it, because a key it does not understand is not
/// evidence of damage.
pub fn classify_object_key(key: &str, options: &StorageAuditOptions) -> Option<ClassifiedObject> {
    for (signal, index_key) in [
        (StorageSignal::Traces, options.trace_index_key.as_str()),
        (StorageSignal::Profiles, options.profile_index_key.as_str()),
    ] {
        if let Some(object) = snapshot_index_object(key, index_key, signal) {
            return Some(object);
        }
    }
    let segments: Vec<&str> = key.split('/').collect();
    match segments.as_slice() {
        ["metrics", tenant, rest @ ..] => metrics_object(tenant, rest),
        ["mimir-block-uploads", tenant, _, ..] => {
            tenant_object(StorageSignal::Metrics, tenant, ObjectRole::Staging)
        }
        ["mimir-tenant-deletions", file] => {
            let tenant = file.strip_suffix(".json")?;
            tenant_object(StorageSignal::Metrics, tenant, ObjectRole::TenantDeletion)
        }
        ["metric-erasure-requests", tenant, file] => {
            let id = unescape_object_path_segment(file.strip_suffix(".json")?)?;
            tenant_object(
                StorageSignal::Metrics,
                tenant,
                ObjectRole::ErasureRequest { id },
            )
        }
        ["traces", tenant, rest @ ..] => traces_object(tenant, rest),
        ["blocks", tenant, rest @ ..] => profiles_object(key, tenant, rest),
        ["debug-info", tenant, _, ..] => Some(ClassifiedObject::new(
            StorageSignal::Profiles,
            Some(unescape_object_path_segment(tenant).unwrap_or_else(|| (*tenant).to_string())),
            ObjectRole::Staging,
        )),
        ["index", "logs", "manifest.json"] => Some(ClassifiedObject::new(
            StorageSignal::Logs,
            None,
            ObjectRole::LogGlobalManifest,
        )),
        ["index", "logs", "compaction-frontier.json"] => Some(ClassifiedObject::new(
            StorageSignal::Logs,
            None,
            ObjectRole::LogFrontier,
        )),
        [tenant, rest @ ..] => logs_object(tenant.strip_prefix("tenant=")?, rest),
        [] => None,
    }
}

fn tenant_object(
    signal: StorageSignal,
    tenant: &str,
    role: ObjectRole,
) -> Option<ClassifiedObject> {
    Some(ClassifiedObject::new(
        signal,
        Some(unescape_object_path_segment(tenant)?),
        role,
    ))
}

/// The object of the snapshot index at `index_key` that `key` names, if it
/// names one. A tenant's shard payload has that tenant; every other index
/// object is shared.
fn snapshot_index_object(
    key: &str,
    index_key: &str,
    signal: StorageSignal,
) -> Option<ClassifiedObject> {
    let shared = || ClassifiedObject::new(signal, None, ObjectRole::IndexSnapshot);
    if key == index_key.trim_matches('/') {
        return Some(shared());
    }
    let payloads = format!("{}/", shard_payload_prefix_for_key(index_key));
    if let Some(rest) = key.strip_prefix(&payloads) {
        let tenant = rest
            .split('/')
            .next()
            .and_then(|segment| segment.strip_prefix("tenant="))
            .and_then(unescape_object_path_segment);
        return Some(ClassifiedObject::new(
            signal,
            tenant,
            ObjectRole::IndexSnapshot,
        ));
    }
    [
        index_snapshot_prefix_for_key(index_key),
        index_shards_prefix_for_key(index_key),
    ]
    .iter()
    .any(|prefix| key.starts_with(&format!("{prefix}/")))
    .then(shared)
}

fn metrics_object(tenant: &str, rest: &[&str]) -> Option<ClassifiedObject> {
    let (lane, partition, file) = match rest {
        ["uploaded", file] => ("uploaded", None, *file),
        [kind, file] if METRIC_KINDS.contains(kind) => (*kind, None, *file),
        [kind, "compacted", file] if METRIC_KINDS.contains(kind) => (*kind, None, *file),
        [kind, partition, file] if METRIC_KINDS.contains(kind) => {
            let partition = partition.strip_prefix("partition=")?.parse().ok()?;
            (*kind, Some(partition), *file)
        }
        _ => return None,
    };
    let compacted = rest.contains(&"compacted");
    let role = if let Some(stem) = file.strip_suffix(".parquet") {
        ObjectRole::Block(BlockShape {
            partition,
            lane: lane.to_string(),
            offsets: (!compacted && lane != "uploaded")
                .then(|| offset_range(stem))
                .flatten(),
        })
    } else {
        let stem = file.strip_suffix(".index")?;
        let mut block = vec!["metrics", tenant];
        block.extend_from_slice(&rest[..rest.len() - 1]);
        ObjectRole::MetricsIndex {
            block: format!("{}/{stem}.parquet", block.join("/")),
        }
    };
    tenant_object(StorageSignal::Metrics, tenant, role)
}

fn traces_object(tenant: &str, rest: &[&str]) -> Option<ClassifiedObject> {
    let shape = match rest {
        ["compacted", file] => {
            file.strip_suffix(".parquet")?;
            BlockShape {
                partition: None,
                lane: String::new(),
                offsets: None,
            }
        }
        [partition, file] => {
            let stem = file.strip_suffix(".parquet")?;
            let (offsets, _window_start) = stem.rsplit_once('-')?;
            BlockShape {
                partition: Some(partition.parse().ok()?),
                lane: String::new(),
                offsets: Some(offset_range(offsets)?),
            }
        }
        _ => return None,
    };
    tenant_object(StorageSignal::Traces, tenant, ObjectRole::Block(shape))
}

fn profiles_object(key: &str, tenant: &str, rest: &[&str]) -> Option<ClassifiedObject> {
    if let Some(block) = key.strip_suffix(".symdb") {
        block.strip_suffix(".parquet")?;
        return tenant_object(
            StorageSignal::Profiles,
            tenant,
            ObjectRole::Symbols {
                block: block.to_string(),
            },
        );
    }
    let shape = match rest {
        ["compacted", file] => {
            file.strip_suffix(".parquet")?;
            BlockShape {
                partition: None,
                lane: String::new(),
                offsets: None,
            }
        }
        [partition, file] => {
            let stem = file.strip_suffix(".parquet")?;
            let mut fields = stem.splitn(3, '-');
            let first = fields.next()?;
            let last = fields.next()?;
            fields.next()?;
            BlockShape {
                partition: Some(partition.parse().ok()?),
                lane: String::new(),
                offsets: Some(offset_range(&format!("{first}-{last}"))?),
            }
        }
        _ => return None,
    };
    tenant_object(StorageSignal::Profiles, tenant, ObjectRole::Block(shape))
}

fn logs_object(tenant: &str, rest: &[&str]) -> Option<ClassifiedObject> {
    let role = match rest {
        ["index", "logs", "manifest.json"] => ObjectRole::LogTenantManifest,
        ["index", "logs", "shards", "manifest.json"] => ObjectRole::LogShardCatalog,
        ["index", "logs", "shards", time, "manifest.json"] => {
            let (start_ns, end_ns) = offset_range(time.strip_prefix("time=")?)?;
            ObjectRole::LogShardManifest { start_ns, end_ns }
        }
        ["index", "logs", "shards", time] => {
            time.strip_prefix("time=")?.parse::<i64>().ok()?;
            ObjectRole::LogShardListOffset
        }
        [partition, offsets, time] => {
            let partition = partition.strip_prefix("partition=")?.parse().ok()?;
            let offsets = offset_range(offsets.strip_prefix("offsets=")?)?;
            time.strip_prefix("time=")?.strip_suffix(".parquet")?;
            ObjectRole::Block(BlockShape {
                partition: Some(partition),
                lane: String::new(),
                offsets: Some(offsets),
            })
        }
        _ => return None,
    };
    tenant_object(StorageSignal::Logs, tenant, role)
}

/// Parses `A-B` into an inclusive range. Either bound may be negative, so the
/// split is at the first `-` that leaves two integers.
fn offset_range(raw: &str) -> Option<(i64, i64)> {
    raw.match_indices('-')
        .filter(|(at, _)| *at > 0)
        .find_map(|(at, _)| {
            let first = raw[..at].parse().ok()?;
            let last = raw[at + 1..].parse().ok()?;
            Some((first, last))
        })
}
