use super::*;

/// Merges range-matrix subquery results back into one Prometheus matrix.
///
/// This function is the query-frontend counterpart to
/// [`super::plan_range_query`]. It joins the time-split subqueries for the same
/// series. Sharded subqueries contribute distinct series.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn merge_range_query_results(results: Vec<QueryResult>) -> Result<QueryResult, PromqlError> {
    merge_range_query_results_with_reducer(results, QueryShardReducer::Sum)
}
