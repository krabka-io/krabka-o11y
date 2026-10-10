use krabka_logql::VectorAggregationOp;

use super::{
    HttpQueryError, MetricQuery, QuerierState, QueryKind, TimeRange, Value,
    active_log_delete_filters, add_loki_query_stats_for_metric_plan,
    add_loki_query_stats_for_metric_plan_with_hot_tail, default_metric_range_step,
    execute_http_metric_instant_query, execute_http_metric_range_query, hot_tail_snapshot,
    metric_query_uses_approx_topk, metric_query_uses_count_values, metric_scan_range,
    plan_stream_query, validate_query_bytes_limit, validate_query_series_limit,
};
use crate::{
    http::params_format::aggregation_formatting::apply_approx_metric_selection,
    loki_vector_response_from_matrix,
};

/// One metric query, as the HTTP query API evaluates it.
pub(crate) struct HttpMetricQuery {
    pub(crate) time_range: TimeRange,
    pub(crate) step: Option<i64>,
    pub(crate) kind: QueryKind,
    pub(crate) query: MetricQuery,
    /// The log range every read covers, or `None` to derive it from `query`.
    /// Variants use the common log range for reads and their own ranges for
    /// evaluation.
    pub(crate) common_scan_range: Option<TimeRange>,
}

pub(crate) async fn execute_http_metric_query(
    state: &QuerierState,
    tenant: &str,
    metric_query: HttpMetricQuery,
) -> Result<Value, HttpQueryError> {
    let HttpMetricQuery {
        time_range,
        step,
        kind,
        mut query,
        common_scan_range,
    } = metric_query;
    let approximate_limit = take_approximate_limit(state, kind, &mut query)?;
    if metric_query_uses_count_values(&query) {
        return Err(HttpQueryError::CountValuesQuery);
    }
    let scan_range = common_scan_range.map_or_else(|| metric_scan_range(&query, time_range), Ok)?;
    let state = state.with_request_tenant_index(tenant, scan_range).await?;
    let plan = plan_stream_query(
        tenant,
        scan_range,
        query.stream.clone(),
        &state.label_index,
        &state.block_index,
    )?;
    if common_scan_range.is_none() && approximate_limit.is_none() {
        validate_query_series_limit(&state, &plan)?;
    }
    validate_query_bytes_limit(&state, &plan)?;
    let delete_filters = active_log_delete_filters(&state, tenant, scan_range)?;
    if matches!(kind, QueryKind::Range) {
        let step_ns = step.unwrap_or_else(|| default_metric_range_step(time_range));
        let response = execute_http_metric_range_query(
            &state,
            &plan,
            &query,
            time_range,
            step_ns,
            &delete_filters,
        )
        .await?;
        if state.hot_tail.is_some() {
            let (records, frontier) = hot_tail_snapshot(&state, plan.time_range);
            return Ok(add_loki_query_stats_for_metric_plan_with_hot_tail(
                response,
                &plan,
                &query,
                &records,
                &frontier,
                (time_range, step_ns),
                &delete_filters,
            ));
        }
        return Ok(add_loki_query_stats_for_metric_plan(
            response, &plan, &query,
        ));
    }
    let eval_range = TimeRange::new(time_range.end_ns, time_range.end_ns)
        .expect("single timestamp metric eval range is valid");
    let mut response = if common_scan_range.is_none() && query.offset_ns.0 == 0 {
        execute_http_metric_instant_query(&state, &plan, &query, &delete_filters).await?
    } else {
        loki_vector_response_from_matrix(
            execute_http_metric_range_query(&state, &plan, &query, eval_range, 1, &delete_filters)
                .await?,
        )
    };
    if let Some(limit) = approximate_limit {
        apply_approx_metric_selection(&mut response, limit, state.max_count_min_sketch_heap_size);
    }
    if state.hot_tail.is_some() {
        let (records, frontier) = hot_tail_snapshot(&state, plan.time_range);
        return Ok(add_loki_query_stats_for_metric_plan_with_hot_tail(
            response,
            &plan,
            &query,
            &records,
            &frontier,
            (eval_range, 1),
            &delete_filters,
        ));
    }
    Ok(add_loki_query_stats_for_metric_plan(
        response, &plan, &query,
    ))
}

fn take_approximate_limit(
    state: &QuerierState,
    kind: QueryKind,
    query: &mut MetricQuery,
) -> Result<Option<usize>, HttpQueryError> {
    let limit = if metric_query_uses_approx_topk(query) {
        if !state
            .limits
            .shard_aggregations
            .iter()
            .any(|name| name == "approx_topk")
        {
            return Err(HttpQueryError::ApproxTopKDisabled);
        }
        if matches!(kind, QueryKind::Range) {
            return Err(HttpQueryError::ApproxTopKRangeQuery);
        }
        match query
            .vector_aggregation
            .take()
            .map(|aggregation| aggregation.op)
        {
            Some(VectorAggregationOp::ApproxTopK(limit)) => {
                Some(usize::try_from(limit).unwrap_or(usize::MAX))
            }
            _ => None,
        }
    } else {
        None
    };
    Ok(limit)
}
