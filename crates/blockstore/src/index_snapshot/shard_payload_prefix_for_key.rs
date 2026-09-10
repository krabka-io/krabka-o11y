/// Object prefix holding every shard payload of the index stored under `key`.
///
/// A sibling of the key, and a sibling of the snapshot prefix, for the reason
/// [`super::index_snapshot_prefix_for_key`] gives: a filesystem-backed S3
/// service may already have the key itself mapped to a directory.
#[must_use]
pub(crate) fn shard_payload_prefix_for_key(key: &str) -> String {
    let key = key.trim_matches('/');
    key.strip_suffix(".json").map_or_else(
        || format!("{key}.payloads"),
        |stem| format!("{stem}/payloads"),
    )
}
