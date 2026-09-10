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
