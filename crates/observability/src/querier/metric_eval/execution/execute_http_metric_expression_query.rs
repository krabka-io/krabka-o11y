use super::*;

pub(crate) async fn execute_http_metric_expression_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    query: &str,
    full_query: &str,
) -> Result<Value, HttpQueryError> {
    if let Some(sort) = parse_sort_vector_expression(query) {
        return Box::pin(execute_http_sort_vector_expression(
            state, tenant, time_range, step, kind, sort, full_query,
        ))
        .await;
    }
    if split_logql_function_arguments(query, "label_join").is_none()
        && let Some(result) = scalar_vector_expression_result(query)
    {
        let value = match kind {
            QueryKind::Instant => loki_instant_scalar_or_vector_response(time_range.end_ns, result),
            QueryKind::Range => loki_range_vector_response(
                time_range,
                resolved_range_step(step, time_range)?,
                result,
            ),
        };
        return Ok(add_loki_query_stats(value));
    }
    if let Ok(label_replace) = parse_metric_label_replace_query(query) {
        let mut value = execute_http_metric_query(
            state,
            tenant,
            time_range,
            step,
            kind,
            label_replace.query.clone(),
        )
        .await?;
        apply_label_replace_to_loki_result(
            &mut value,
            &label_replace.destination_label,
            &label_replace.replacement,
            &label_replace.source_label,
            &label_replace.pattern,
            full_query,
        )?;
        return Ok(value);
    }
    if let Some(label_replace) = parse_label_replace_expression(query) {
        let mut value = Box::pin(execute_http_metric_expression_query(
            state,
            tenant,
            time_range,
            step,
            kind,
            &label_replace.query,
            full_query,
        ))
        .await?;
        apply_label_replace_to_loki_result(
            &mut value,
            &label_replace.destination_label,
            &label_replace.replacement,
            &label_replace.source_label,
            &label_replace.pattern,
            full_query,
        )?;
        return Ok(value);
    }
    if let Some(inner_query) = strip_outer_parenthesized_expression(query) {
        return Box::pin(execute_http_metric_expression_query(
            state,
            tenant,
            time_range,
            step,
            kind,
            inner_query,
            full_query,
        ))
        .await;
    }
    if let Some(arithmetic) = parse_metric_vector_arithmetic_expression(query) {
        return execute_http_metric_vector_arithmetic_expression(
            state, tenant, time_range, step, kind, arithmetic, full_query,
        )
        .await;
    }
    if let Some(comparison) = parse_metric_vector_comparison_expression(query) {
        return execute_http_metric_vector_comparison_expression(
            state, tenant, time_range, step, kind, comparison, full_query,
        )
        .await;
    }
    if let Some(set) = parse_metric_vector_set_expression(query) {
        return execute_http_metric_vector_set_expression(
            state, tenant, time_range, step, kind, set, full_query,
        )
        .await;
    }
    if let Ok(arithmetic) = parse_metric_binary_arithmetic_query(query) {
        return execute_http_metric_binary_arithmetic_query(
            state, tenant, time_range, step, kind, arithmetic,
        )
        .await;
    }
    if let Ok(comparison) = parse_metric_binary_comparison_query(query) {
        return execute_http_metric_binary_comparison_query(
            state, tenant, time_range, step, kind, comparison,
        )
        .await;
    }
    if let Ok(set) = parse_metric_binary_set_query(query) {
        return execute_http_metric_binary_set_query(state, tenant, time_range, step, kind, set)
            .await;
    }
    if let Ok(arithmetic) = parse_metric_scalar_arithmetic_query(query) {
        return execute_http_metric_scalar_arithmetic_query(
            state, tenant, time_range, step, kind, arithmetic, full_query,
        )
        .await;
    }
    if let Ok(comparison) = parse_metric_scalar_comparison_query(query) {
        return execute_http_metric_scalar_comparison_query(
            state, tenant, time_range, step, kind, comparison, full_query,
        )
        .await;
    }
    let query = parse_metric_query(query).map_err(|source| HttpQueryError::LokiParse {
        query: full_query.to_string(),
        source,
    })?;
    execute_http_metric_query(state, tenant, time_range, step, kind, query).await
}
