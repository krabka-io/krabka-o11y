use super::{
    BlockKey, Component, ObjectPath, Path, PathBuf, TimeRange,
    log_tenant_index_shard_list_offset_start_ns, log_tenant_index_shards_object_prefix,
};

/// Local path of the block that `key` names, under `root`.
///
/// The result is always under `root`. The path grows one component at a time
/// and keeps only the normal components, so a root, a prefix, `.` and `..` are
/// dropped and never followed. [`BlockKey::object_key`] escapes the tenant
/// through [`crate::escape_object_path_segment`], so none of those reach here
/// in the first place. The filter is a second guard, and it does not depend on
/// the escape.
#[must_use]
pub fn block_path(root: impl AsRef<Path>, key: &BlockKey) -> PathBuf {
    let mut path = root.as_ref().to_path_buf();
    for segment in key.object_key().split('/') {
        for component in Path::new(segment).components() {
            if let Component::Normal(name) = component {
                path.push(name);
            }
        }
    }
    path
}

#[must_use]
pub fn log_tenant_index_shard_list_offset_object_path(
    prefix: &ObjectPath,
    tenant: &str,
    query_range: TimeRange,
) -> ObjectPath {
    log_tenant_index_shards_object_prefix(prefix, tenant).join(format!(
        "time={}",
        log_tenant_index_shard_list_offset_start_ns(query_range)
    ))
}

/// The logical key whose sibling `manifest/snapshots/` prefix holds this shard's generations.
#[must_use]
pub fn log_tenant_index_shard_manifest_object_path(
    prefix: &ObjectPath,
    tenant: &str,
    shard_range: TimeRange,
) -> ObjectPath {
    log_tenant_index_shards_object_prefix(prefix, tenant)
        .join(format!(
            "time={}-{}",
            shard_range.start_ns, shard_range.end_ns
        ))
        .join("manifest.json")
}

pub(crate) fn parse_log_tenant_index_shard_range_from_object_path(
    shard_prefix: &ObjectPath,
    location: &ObjectPath,
) -> Option<TimeRange> {
    let rest = location
        .as_ref()
        .strip_prefix(shard_prefix.as_ref())?
        .trim_start_matches('/');
    let mut parts = rest.split('/');
    let range_part = parts.next()?.strip_prefix("time=")?;
    if parts.next()? != "manifest"
        || parts.next()? != "snapshots"
        || parts.next()?.strip_suffix(".json").is_none()
        || parts.next().is_some()
    {
        return None;
    }

    for index in range_part.match_indices('-').map(|marker| marker.0) {
        if index == 0 {
            continue;
        }
        let start = &range_part[..index];
        let end = &range_part[index + 1..];
        if let Ok(start_ns) = start.parse::<i64>()
            && let Ok(end_ns) = end.parse::<i64>()
            && let Ok(range) = TimeRange::new(start_ns, end_ns)
        {
            return Some(range);
        }
    }
    None
}
