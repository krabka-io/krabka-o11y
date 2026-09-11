use super::{MaxOffset, MinOffset, WindowStartNs, escape_object_path_segment};

/// Deterministic object key for one block-builder flush window.
///
/// The tenant is one path segment, escaped with
/// [`krabka_blockstore::escape_object_path_segment`]. A tenant id can hold
/// characters that `object_store` rewrites, so the raw name is not always the
/// segment the store lists.
#[must_use]
pub fn object_key(
    tenant: &str,
    partition: i32,
    min_offset: MinOffset,
    max_offset: MaxOffset,
    window_start_ns: WindowStartNs,
) -> String {
    let (min_offset, max_offset, window_start_ns) = (min_offset.0, max_offset.0, window_start_ns.0);
    let tenant = escape_object_path_segment(tenant);
    format!(
        "traces/{tenant}/{partition:05}/{min_offset:020}-{max_offset:020}-{window_start_ns}.parquet"
    )
}
