use super::{BLOCK_OBJECT_PREFIX, escape_object_path_segment};

/// Object key of one block that the block-builder writes.
///
/// The tenant is one path segment. It goes through
/// [`escape_object_path_segment`], so a tenant name that holds a character
/// such as `*` or `!` stays in its own segment and survives the object store.
/// The other segments come from integers and cannot hold a separator.
#[must_use]
pub fn object_key(
    tenant: &str,
    partition: i32,
    min_offset: i64,
    max_offset: i64,
    min_ts: i64,
    max_ts: i64,
) -> String {
    let tenant = escape_object_path_segment(tenant);
    format!(
        "{BLOCK_OBJECT_PREFIX}/{tenant}/{partition:05}/{min_offset:020}-{max_offset:020}-{min_ts}-{max_ts}.parquet"
    )
}
