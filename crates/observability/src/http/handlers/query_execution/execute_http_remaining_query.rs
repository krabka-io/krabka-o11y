use super::*;

pub(crate) async fn execute_http_remaining_query(
    state: &QuerierState,
    tenant: &str,
    params: &QueryParams,
    kind: QueryKind,
    time_range: TimeRange,
    stream_options: (LokiDirection, Option<usize>, Option<i64>),
) -> Result<Value, HttpQueryError> {
    let (direction, limit, interval) = stream_options;
    if let Some(inner_query) = strip_outer_parenthesized_expression(&params.query) {
        return execute_http_metric_expression_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            inner_query,
            &params.query,
        )
        .await;
    }
    if let Some(arithmetic) = parse_metric_vector_arithmetic_expression(&params.query) {
        return execute_http_metric_vector_arithmetic_expression(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            arithmetic,
            &params.query,
        )
        .await;
    }
    if let Some(comparison) = parse_metric_vector_comparison_expression(&params.query) {
        return execute_http_metric_vector_comparison_expression(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            comparison,
            &params.query,
        )
        .await;
    }
    if let Some(set) = parse_metric_vector_set_expression(&params.query) {
        return execute_http_metric_vector_set_expression(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            set,
            &params.query,
        )
        .await;
    }
    if let Ok(arithmetic) = parse_metric_binary_arithmetic_query(&params.query) {
        return execute_http_metric_binary_arithmetic_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            arithmetic,
        )
        .await;
    }
    if let Ok(comparison) = parse_metric_binary_comparison_query(&params.query) {
        return execute_http_metric_binary_comparison_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            comparison,
        )
        .await;
    }
    if let Ok(set) = parse_metric_binary_set_query(&params.query) {
        return execute_http_metric_binary_set_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            set,
        )
        .await;
    }
    if let Ok(arithmetic) = parse_metric_scalar_arithmetic_query(&params.query) {
        return execute_http_metric_scalar_arithmetic_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            arithmetic,
            &params.query,
        )
        .await;
    }
    if let Ok(comparison) = parse_metric_scalar_comparison_query(&params.query) {
        return execute_http_metric_scalar_comparison_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            comparison,
            &params.query,
        )
        .await;
    }
    let value = if let Ok(query) = parse_metric_query(&params.query) {
        execute_http_metric_query(state, tenant, time_range, params.step, kind, query).await?
    } else {
        execute_http_stream_query(
            state,
            &params.query,
            tenant,
            time_range,
            (
                direction,
                limit,
                interval,
                if matches!(kind, QueryKind::Range) {
                    Some(time_range.end_ns)
                } else {
                    None
                },
            ),
        )
        .await
        .map_err(|error| match error {
            HttpQueryError::Parse(source) => HttpQueryError::LokiParse {
                query: params.query.clone(),
                source,
            },
            error => error,
        })?
    };

    Ok(add_loki_query_stats(value))
}
