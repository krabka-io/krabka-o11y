use super::*;

pub(crate) async fn execute_http_metric_range_query(
    state: &QuerierState,
    plan: &StreamPlan,
    query: &MetricQuery,
    time_range: TimeRange,
    step_ns: i64,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, HttpQueryError> {
    if step_ns <= 0 {
        return Err(HttpQueryError::InvalidStep);
    }
    if let Some(cold_store) = &state.cold_store {
        let (records, frontier) = hot_tail_snapshot(state, plan.time_range);
        return execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes(
            Arc::clone(&cold_store.store),
            &cold_store.prefix,
            plan,
            query,
            &state.label_index,
            (time_range, step_ns),
            QueryHotTail {
                records: &records,
                frontier: &frontier,
                delete_filters,
            },
        )
        .await
        .map_err(HttpQueryError::from);
    }
    if let Some(hot_tail) = &state.hot_tail {
        let records = hot_tail
            .source
            .records_in_range(plan.time_range.start_ns, plan.time_range.end_ns);
        let frontier = hot_tail.frontier.snapshot();
        return execute_metric_query_range_with_hot_tail_frontier_and_deletes(
            &state.root,
            plan,
            query,
            &state.label_index,
            (time_range, step_ns),
            QueryHotTail {
                records: &records,
                frontier: &frontier,
                delete_filters,
            },
        )
        .await
        .map_err(HttpQueryError::from);
    }
    execute_metric_query_range_with_deletes(
        &state.root,
        plan,
        query,
        &state.label_index,
        time_range,
        step_ns,
        delete_filters,
    )
    .await
    .map_err(HttpQueryError::from)
}
