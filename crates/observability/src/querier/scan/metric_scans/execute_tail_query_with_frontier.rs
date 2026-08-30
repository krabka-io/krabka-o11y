use super::{
    CompactionFrontier, StreamPlan, Value, WalLogRecord,
    execute_tail_query_with_frontier_and_deletes,
};

#[must_use]
pub fn execute_tail_query_with_frontier(
    plan: &StreamPlan,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
) -> Value {
    execute_tail_query_with_frontier_and_deletes(plan, hot_tail, frontier, &[])
}
