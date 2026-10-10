use super::SharedTraceIndex;

/// One nanosecond past the newest `max_ts` across `tenant`'s indexed blocks,
/// or 0 when the tenant has none.
///
/// This is what [`crate::querier::live::LiveSource::block_builder_frontier_ns`]
/// reports for a live source backed by the shared trace index.
#[must_use]
pub fn block_builder_frontier_ns(trace_index: &SharedTraceIndex, tenant: &str) -> i64 {
    trace_index
        .load()
        .trace_blocks(tenant)
        .iter()
        .map(|block| block.max_ts.saturating_add(1))
        .max()
        .unwrap_or_default()
}
