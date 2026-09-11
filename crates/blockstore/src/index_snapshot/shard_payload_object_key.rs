use super::{
    IndexShardRange, escape_object_path_segment, shard_bound_key, shard_payload_prefix_for_key,
};

/// Object key of one shard payload.
///
/// The tenant and the span are path segments so that the sweep can tell a
/// payload from anything else under the prefix without reading it, and so that
/// a listing is grouped the way a tenant-scoped load wants it. The tenant
/// carries an escape, which is what holds it inside its own segment. The
/// content hash is the filename, which is what keeps a payload immutable: a
/// changed shard is a new object, never an overwrite of one an older retained
/// manifest names.
#[must_use]
pub(crate) fn shard_payload_object_key(
    key: &str,
    tenant: &str,
    range: IndexShardRange,
    content: &str,
) -> String {
    format!(
        "{}/tenant={}/time={}-{}/{content}.kbs",
        shard_payload_prefix_for_key(key),
        escape_object_path_segment(tenant),
        shard_bound_key(range.start),
        shard_bound_key(range.end),
    )
}
