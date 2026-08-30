use super::{
    HttpQueryError, QuerierState, QueryKind, QueryParams, Value, add_loki_query_stats,
    apply_label_join_to_loki_result, apply_label_replace_to_loki_result,
    execute_http_label_replace_metric_binary_expression, execute_http_metric_expression_query,
    execute_http_metric_query, execute_http_remaining_query, execute_http_sort_vector_expression,
    loki_direction, loki_instant_scalar_or_vector_response, loki_range_vector_response,
    parse_label_replace_expression, parse_label_replace_metric_binary_expression,
    parse_metric_label_join_query, parse_metric_label_replace_query, parse_sort_vector_expression,
    reject_signed_vector_function_literal, resolved_range_step, scalar_vector_expression_result,
    time_range, validate_loki_query_range_resolution, validate_loki_range_query_range_limit,
    validate_query_length_limit, validate_query_range_limit,
};

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
