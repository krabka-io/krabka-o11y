use super::{QuerierState, StreamPlan, WalLogRecord};

/// The hot-tail records a stream plan selects: the plan's tenant, inside its
/// window, not yet compacted into a block, and matching its label matchers.
///
/// A block answers for what the block builder already wrote. The rest is still
/// in the WAL, and Loki answers for it from the ingester, so an analytics read
/// that skipped this reported nothing about the last minutes of logs.
/// The pipeline stages are not applied here. A caller that needs them runs
/// them.
pub(crate) fn plan_hot_tail_records(state: &QuerierState, plan: &StreamPlan) -> Vec<WalLogRecord> {
    let Some(hot_tail) = &state.hot_tail else {
        return Vec::new();
    };
    let frontier = hot_tail.frontier.snapshot();
    let mut records = hot_tail
        .source
        .records_in_range(plan.time_range.start_ns, plan.time_range.end_ns);
    records.retain(|record| {
        record.tenant == plan.tenant
            && !frontier.is_compacted(record)
            && plan.time_range.start_ns <= record.timestamp_ns
            && record.timestamp_ns <= plan.time_range.end_ns
            && plan
                .query
                .matchers
                .iter()
                .all(|matcher| matcher.matches(&record.labels))
    });
    records
}
