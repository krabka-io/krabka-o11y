use std::borrow::Borrow;

use super::{
    CompactionFrontier, StreamPlan, Value, WalLogRecord,
    add_loki_query_stats_for_stream_blocks_with_hot_tail,
};

pub(crate) fn add_loki_query_stats_for_stream_plan_with_hot_tail<R: Borrow<WalLogRecord> + Sync>(
    value: Value,
    plan: &StreamPlan,
    hot_tail: &[R],
    frontier: &CompactionFrontier,
) -> Value {
    add_loki_query_stats_for_stream_blocks_with_hot_tail(
        value,
        &plan.blocks,
        plan,
        hot_tail,
        frontier,
    )
}
