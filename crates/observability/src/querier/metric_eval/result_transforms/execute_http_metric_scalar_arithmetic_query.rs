use super::{
    HttpQueryError, MetricScalarArithmetic, QuerierState, QueryKind, TimeRange, Value,
    active_log_delete_filters, add_loki_query_stats_for_metric_plan,
    add_loki_query_stats_for_metric_plan_with_hot_tail,
    apply_metric_scalar_arithmetic_to_loki_result, default_metric_range_step,
    execute_http_metric_instant_query, execute_http_metric_range_query, hot_tail_snapshot,
    metric_scan_range, plan_stream_query, validate_query_bytes_limit, validate_query_series_limit,
};

pub(crate) async fn execute_http_metric_scalar_arithmetic_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    arithmetic: MetricScalarArithmetic,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let query = arithmetic.query.clone();
    let scan_range = metric_scan_range(&query, time_range)?;
    let state = state.with_request_tenant_index(tenant, scan_range).await?;
    let plan = plan_stream_query(
        tenant,
        scan_range,
        query.stream.clone(),
        &state.label_index,
        &state.block_index,
    )?;
    validate_query_series_limit(&state, &plan)?;
    validate_query_bytes_limit(&state, &plan)?;
    let delete_filters = active_log_delete_filters(&state, tenant, scan_range)?;
    if matches!(kind, QueryKind::Range) {
        let step_ns = step.unwrap_or_else(|| default_metric_range_step(time_range));
        let mut response = execute_http_metric_range_query(
            &state,
            &plan,
            &query,
            time_range,
            step_ns,
            &delete_filters,
        )
        .await?;
        apply_metric_scalar_arithmetic_to_loki_result(&mut response, &arithmetic, query_text)?;
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

    let mut response =
        execute_http_metric_instant_query(&state, &plan, &query, &delete_filters).await?;
    apply_metric_scalar_arithmetic_to_loki_result(&mut response, &arithmetic, query_text)?;
    if state.hot_tail.is_some() {
        let (records, frontier) = hot_tail_snapshot(&state, plan.time_range);
        let eval_range = TimeRange::new(time_range.end_ns, time_range.end_ns)
            .expect("single timestamp metric eval range is valid");
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
