use super::*;

#[must_use]
pub fn execute_tail_query(
    plan: &StreamPlan,
    hot_tail: &[WalLogRecord],
    compacted_through_ns: i64,
) -> Value {
    execute_tail_query_with_frontier(
        plan,
        hot_tail,
        &CompactionFrontier::new(compacted_through_ns),
    )
}
