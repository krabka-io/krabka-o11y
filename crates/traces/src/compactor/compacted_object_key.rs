use super::{MinOffset, MaxOffset, WindowStartNs};

/// Deterministic object key for a compacted span block.
#[must_use]
pub fn compacted_object_key(
    tenant: &str,
    partition: i32,
    min_offset: MinOffset,
    max_offset: MaxOffset,
    window_start_ns: WindowStartNs,
) -> String {
    let (min_offset, max_offset, window_start_ns) = (min_offset.0, max_offset.0, window_start_ns.0);
    format!(
        "traces/{tenant}/{partition:05}/compacted-{min_offset:020}-{max_offset:020}-{window_start_ns}.parquet"
    )
}
