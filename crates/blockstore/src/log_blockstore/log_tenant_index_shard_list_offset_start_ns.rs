use super::TimeRange;

#[must_use]
pub fn log_tenant_index_shard_list_offset_start_ns(query_range: TimeRange) -> i64 {
    let query_width_ns = query_range
        .end_ns
        .saturating_sub(query_range.start_ns)
        .max(1);
    query_range.start_ns.saturating_sub(query_width_ns)
}
