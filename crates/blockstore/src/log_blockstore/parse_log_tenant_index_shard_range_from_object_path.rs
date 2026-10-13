use super::*;

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
