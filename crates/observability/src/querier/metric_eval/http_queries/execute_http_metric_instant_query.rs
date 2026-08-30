use super::{
    ActiveLogDeleteFilter, Arc, HttpQueryError, MetricQuery, QuerierState, QueryHotTail,
    StreamPlan, Value, execute_metric_query_from_object_store_with_hot_tail_frontier_and_deletes,
    execute_metric_query_with_deletes, execute_metric_query_with_hot_tail_frontier_and_deletes,
    hot_tail_snapshot, loki_vector_response_from_matrix,
};

pub(crate) async fn execute_http_metric_instant_query(
    state: &QuerierState,
    plan: &StreamPlan,
    query: &MetricQuery,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<Value, HttpQueryError> {
    let response = if let Some(cold_store) = &state.cold_store {
        let (records, frontier) = hot_tail_snapshot(state, plan.time_range);
        execute_metric_query_from_object_store_with_hot_tail_frontier_and_deletes(
            Arc::clone(&cold_store.store),
            &cold_store.prefix,
            plan,
            query,
            &state.label_index,
            QueryHotTail {
                records: &records,
                frontier: &frontier,
                delete_filters,
            },
        )
        .await
        .map_err(HttpQueryError::from)?
    } else if let Some(hot_tail) = &state.hot_tail {
        let records = hot_tail
            .source
            .records_in_range(plan.time_range.start_ns, plan.time_range.end_ns);
        let frontier = hot_tail.frontier.snapshot();
        execute_metric_query_with_hot_tail_frontier_and_deletes(
            &state.root,
            plan,
            query,
            &state.label_index,
            &records,
            &frontier,
            delete_filters,
        )
        .await
        .map_err(HttpQueryError::from)?
    } else {
        execute_metric_query_with_deletes(
            &state.root,
            plan,
            query,
            &state.label_index,
            delete_filters,
        )
        .await
        .map_err(HttpQueryError::from)?
    };

    Ok(loki_vector_response_from_matrix(response))
}
