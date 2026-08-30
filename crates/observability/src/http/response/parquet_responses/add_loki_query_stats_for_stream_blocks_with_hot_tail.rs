use super::*;

pub(crate) fn add_loki_query_stats_for_stream_blocks_with_hot_tail(
    mut value: Value,
    blocks: &[BlockDescriptor],
    plan: &StreamPlan,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
) -> Value {
    let bytes = planned_block_bytes_for_blocks(blocks);
    let chunks = u64::try_from(blocks.len()).unwrap_or(u64::MAX);
    let lines = count_loki_stream_result_lines(&value);
    let ingester_lines = count_loki_stream_result_hot_tail_lines(&value, plan, hot_tail, frontier);
    let store_lines = lines.saturating_sub(ingester_lines);
    let mut stats = loki_query_stats();
    populate_loki_query_scan_stats(&mut stats, bytes, store_lines, ingester_lines, chunks);
    value["data"]["stats"] = stats;
    value
}
