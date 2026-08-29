use super::*;

pub(crate) async fn execute_http_query_for_tenant(
    state: &QuerierState,
    tenant: &str,
    params: &QueryParams,
    kind: QueryKind,
) -> Result<Value, HttpQueryError> {
    let time_range = time_range(params, kind)?;
    validate_loki_range_query_range_limit(kind, time_range)?;
    validate_query_range_limit(state, time_range)?;
    validate_query_length_limit(state, &params.query)?;
    validate_loki_query_range_resolution(params, kind, time_range)?;
    let limit = params.limit;
    let direction = loki_direction(params.direction.as_deref())?;
    let interval = params.interval;
    reject_signed_vector_function_literal(&params.query)?;
    if let Some(result) = scalar_vector_expression_result(&params.query) {
        let value = match kind {
            QueryKind::Instant => loki_instant_scalar_or_vector_response(time_range.end_ns, result),
            QueryKind::Range => loki_range_vector_response(
                time_range,
                resolved_range_step(params.step, time_range)?,
                result,
            ),
        };
        return Ok(add_loki_query_stats(value));
    }
    if let Some(sort) = parse_sort_vector_expression(&params.query) {
        return execute_http_sort_vector_expression(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            sort,
            &params.query,
        )
        .await;
    }
    if let Ok(label_replace) = parse_metric_label_replace_query(&params.query) {
        let mut value = execute_http_metric_query(
            state,
            tenant,
            time_range,
            params.step,
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
            &params.query,
        )?;
        return Ok(value);
    }
    if let Some(binary) = parse_label_replace_metric_binary_expression(&params.query) {
        return execute_http_label_replace_metric_binary_expression(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            binary,
            &params.query,
        )
        .await;
    }
    if let Some(label_replace) = parse_label_replace_expression(&params.query) {
        let mut value = execute_http_metric_expression_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            &label_replace.query,
            &params.query,
        )
        .await?;
        apply_label_replace_to_loki_result(
            &mut value,
            &label_replace.destination_label,
            &label_replace.replacement,
            &label_replace.source_label,
            &label_replace.pattern,
            &params.query,
        )?;
        return Ok(value);
    }
    if let Ok(label_join) = parse_metric_label_join_query(&params.query) {
        let mut value = execute_http_metric_query(
            state,
            tenant,
            time_range,
            params.step,
            kind,
            label_join.query.clone(),
        )
        .await?;
        apply_label_join_to_loki_result(&mut value, &label_join);
        return Ok(value);
    }
    execute_http_remaining_query(
        state,
        tenant,
        params,
        kind,
        time_range,
        (direction, limit, interval),
    )
    .await
}

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
