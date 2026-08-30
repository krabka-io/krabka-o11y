pub(crate) fn is_unbounded_metadata_range(start_ms: i64, end_ms: i64) -> bool {
    start_ms == 0 && end_ms == i64::MAX
}
