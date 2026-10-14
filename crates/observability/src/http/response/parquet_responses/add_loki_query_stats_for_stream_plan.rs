use super::{
    StreamPlan, Value, count_loki_stream_result_lines, loki_query_stats, planned_block_bytes,
    populate_loki_query_scan_stats,
};

pub(crate) fn add_loki_query_stats_for_stream_plan(mut value: Value, plan: &StreamPlan) -> Value {
    let bytes = planned_block_bytes(plan);
    let chunks = u64::try_from(plan.blocks.len()).unwrap_or(u64::MAX);
    let lines = count_loki_stream_result_lines(&value);
    let mut stats = loki_query_stats();
    let (store_lines, ingester_lines) = if chunks == 0 { (0, lines) } else { (lines, 0) };
    populate_loki_query_scan_stats(&mut stats, bytes, store_lines, ingester_lines, chunks);
    value["data"]["stats"] = stats;
    value
}
