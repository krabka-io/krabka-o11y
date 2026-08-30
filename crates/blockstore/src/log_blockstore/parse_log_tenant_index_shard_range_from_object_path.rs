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
    if parts.next()? != "manifest.json" || parts.next().is_some() {
        return None;
    }

    for (index, _) in range_part.match_indices('-') {
        if index == 0 {
            continue;
        }
        let (start, end) = range_part.split_at(index);
        let end = &end[1..];
        if let (Ok(start_ns), Ok(end_ns)) = (start.parse::<i64>(), end.parse::<i64>())
            && let Ok(range) = TimeRange::new(start_ns, end_ns)
        {
            return Some(range);
        }
    }
    None
}
