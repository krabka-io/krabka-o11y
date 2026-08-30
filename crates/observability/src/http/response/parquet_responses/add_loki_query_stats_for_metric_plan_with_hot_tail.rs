use super::{
    ActiveLogDeleteFilter, CompactionFrontier, MetricQuery, StreamPlan, TimeRange, Value,
    WalLogRecord, count_loki_metric_result_hot_tail_samples, count_loki_metric_result_scan_lines,
    loki_query_stats, planned_block_bytes, populate_loki_query_scan_stats,
};

pub(crate) fn add_loki_query_stats_for_metric_plan_with_hot_tail(
    mut value: Value,
    plan: &StreamPlan,
    query: &MetricQuery,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
    evaluation: (TimeRange, i64),
    delete_filters: &[ActiveLogDeleteFilter],
) -> Value {
    let bytes = planned_block_bytes(plan);
    let chunks = u64::try_from(plan.blocks.len()).unwrap_or(u64::MAX);
    let samples = count_loki_metric_result_scan_lines(&value, query);
    let ingester_samples = count_loki_metric_result_hot_tail_samples(
        &value,
        plan,
        query,
        hot_tail,
        frontier,
        evaluation,
        delete_filters,
    );
    let store_samples = samples.saturating_sub(ingester_samples);
    let mut stats = loki_query_stats();
    populate_loki_query_scan_stats(&mut stats, bytes, store_samples, ingester_samples, chunks);
    value["data"]["stats"] = stats;
    value
}
