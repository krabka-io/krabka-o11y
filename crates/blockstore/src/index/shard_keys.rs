use super::escape_object_path_segment;

/// Object prefix holding every shard of the index stored under `key`.
///
/// A sibling of the key rather than a directory under it, for the reason
/// [`crate::index_snapshot::index_snapshot_prefix_for_key`] gives: a
/// filesystem-backed S3 service may already have the key itself mapped to a
/// directory.
#[must_use]
pub fn index_shards_prefix_for_key(key: &str) -> String {
    let key = key.trim_matches('/');
    if let Some(stem) = key.strip_suffix(".json") {
        format!("{stem}/shards")
    } else {
        format!("{key}.shards")
    }
}

/// Object prefix holding one tenant's index shards.
///
/// The tenant is one path segment, as it is on the logs path, so a
/// tenant-scoped load lists its own prefix and never sees another tenant's
/// objects. [`escape_object_path_segment`] is what holds the tenant inside
/// that one segment: a name carrying `/`, `.` or `..` would otherwise reach
/// another tenant's prefix.
#[must_use]
pub fn index_shard_tenant_prefix(key: &str, tenant: &str) -> String {
    format!(
        "{}/tenant={}",
        index_shards_prefix_for_key(key),
        escape_object_path_segment(tenant)
    )
}

/// Object key of the tenant's unbound-series object.
///
/// A writer names its head series as it ingests them, so a series can exist
/// before any block carries it. Such a series belongs to no shard, because the
/// shards are cut on the time spans of blocks. To drop it would lose every
/// series a writer has not yet flushed. To copy it into every shard would
/// multiply it by the retention. It goes here instead, and every load reads
/// it.
///
/// The name sorts after `time=`, so a listing that starts at a `time=` offset
/// still reaches it.
#[must_use]
pub fn index_unbound_series_object_key(key: &str, tenant: &str) -> String {
    format!("{}/unbound.kbi", index_shard_tenant_prefix(key, tenant))
}

/// The path spelling of one shard bound.
///
/// The bound is written in offset binary, with the sign bit flipped, then
/// padded with zeros to twenty digits. Twenty is the widest decimal a `u64`
/// takes. Both halves matter. The flip puts a negative timestamp below a
/// positive one instead of above it. The padding makes the lexicographic order
/// of the keys equal to the numeric order of the bounds. A listing over the
/// prefix therefore returns the shards in time order, which is the property
/// [`crate::index_snapshot`] gets from the same padding on its generation
/// numbers.
pub(crate) fn shard_bound_key(bound: i64) -> String {
    let ordered = u64::from_le_bytes(bound.to_le_bytes()) ^ (1 << 63);
    format!("{ordered:020}")
}

/// Reads back a bound written by [`super::shard_bound_key`].
pub(crate) fn parse_shard_bound_key(key: &str) -> Option<i64> {
    if key.len() != 20 || !key.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let ordered = key.parse::<u64>().ok()?;
    Some(i64::from_le_bytes((ordered ^ (1 << 63)).to_le_bytes()))
}
