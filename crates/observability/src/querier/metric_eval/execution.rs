use crate::{
    HttpQueryError, LabelReplaceMetricBinaryExpression, MetricVectorArithmeticExpression,
    MetricVectorComparisonExpression, MetricVectorSetExpression, QuerierState, QueryKind,
    SortVectorExpression, TimeRange, Value, add_loki_query_stats,
    apply_label_replace_to_loki_result, apply_metric_binary_arithmetic_to_loki_result,
    apply_metric_binary_comparison_to_loki_result, apply_metric_binary_set_to_loki_result,
    execute_http_metric_binary_arithmetic_query, execute_http_metric_binary_comparison_query,
    execute_http_metric_binary_set_query, execute_http_metric_query,
    execute_http_metric_scalar_arithmetic_query, execute_http_metric_scalar_comparison_query,
    execute_http_scalar_vector_expression_result, json, loki_instant_scalar_or_vector_response,
    loki_range_vector_response, merge_loki_query_stats, parse_label_replace_expression,
    parse_metric_binary_arithmetic_query, parse_metric_binary_comparison_query,
    parse_metric_binary_set_query, parse_metric_label_replace_query, parse_metric_query,
    parse_metric_scalar_arithmetic_query, parse_metric_scalar_comparison_query,
    parse_metric_vector_arithmetic_expression, parse_metric_vector_comparison_expression,
    parse_metric_vector_set_expression, parse_sort_vector_expression, resolved_range_step,
    retain_metric_binary_on_labels, scalar_vector_expression_result, scalar_vector_query_is_vector,
    sort_loki_vector_result, split_logql_function_arguments, strip_outer_parenthesized_expression,
    unix_ns_string_to_loki_seconds,
};

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

pub(crate) async fn execute_http_label_replace_metric_binary_expression(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    binary: LabelReplaceMetricBinaryExpression,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    match binary {
        LabelReplaceMetricBinaryExpression::Arithmetic {
            left,
            op,
            matching,
            right,
        } => {
            let mut left = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &left, query_text,
            )
            .await?;
            let right = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &right, query_text,
            )
            .await?;
            apply_metric_binary_arithmetic_to_loki_result(&mut left, &right, op, matching.as_ref());
            retain_metric_binary_on_labels(&mut left, matching.as_ref());
            Ok(left)
        }
        LabelReplaceMetricBinaryExpression::Comparison {
            left,
            op,
            bool_modifier,
            matching,
            right,
        } => {
            let mut left = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &left, query_text,
            )
            .await?;
            let right = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &right, query_text,
            )
            .await?;
            apply_metric_binary_comparison_to_loki_result(
                &mut left,
                &right,
                op,
                bool_modifier,
                matching.as_ref(),
            );
            retain_metric_binary_on_labels(&mut left, matching.as_ref());
            Ok(left)
        }
        LabelReplaceMetricBinaryExpression::Set {
            left,
            op,
            matching,
            right,
        } => {
            let mut left = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &left, query_text,
            )
            .await?;
            let right = execute_http_metric_binary_operand(
                state, tenant, time_range, step, kind, &right, query_text,
            )
            .await?;
            apply_metric_binary_set_to_loki_result(&mut left, &right, op, matching.as_ref());
            Ok(left)
        }
    }
}

pub(crate) async fn execute_http_metric_binary_operand(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    operand: &str,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    if let Some(label_replace) = parse_label_replace_expression(operand) {
        let mut value = execute_http_metric_expression_query(
            state,
            tenant,
            time_range,
            step,
            kind,
            &label_replace.query,
            query_text,
        )
        .await?;
        apply_label_replace_to_loki_result(
            &mut value,
            &label_replace.destination_label,
            &label_replace.replacement,
            &label_replace.source_label,
            &label_replace.pattern,
            query_text,
        )?;
        return Ok(value);
    }
    if scalar_vector_query_is_vector(operand) {
        return execute_http_scalar_vector_expression_result(
            operand, time_range, step, kind, query_text,
        );
    }

    let query = parse_metric_query(operand).map_err(|source| HttpQueryError::LokiParse {
        query: query_text.to_string(),
        source,
    })?;
    execute_http_metric_query(state, tenant, time_range, step, kind, query).await
}

pub(crate) async fn execute_http_sort_vector_expression(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    sort: SortVectorExpression,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let mut value = Box::pin(execute_http_metric_expression_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        &sort.query,
        query_text,
    ))
    .await?;
    sort_loki_vector_result(&mut value, sort.descending);
    Ok(value)
}

pub(crate) async fn execute_http_metric_vector_arithmetic_expression(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    arithmetic: MetricVectorArithmeticExpression,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let metric_value = Box::pin(execute_http_metric_expression_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        &arithmetic.metric_query,
        query_text,
    ))
    .await?;
    let vector_value = execute_http_scalar_vector_expression_result(
        &arithmetic.vector_query,
        time_range,
        step,
        kind,
        query_text,
    )?;

    if arithmetic.vector_on_left {
        let mut value = vector_value;
        apply_metric_binary_arithmetic_to_loki_result(
            &mut value,
            &metric_value,
            arithmetic.op,
            arithmetic.matching.as_ref(),
        );
        retain_metric_binary_on_labels(&mut value, arithmetic.matching.as_ref());
        merge_loki_query_stats(&mut value["data"]["stats"], &metric_value["data"]["stats"]);
        Ok(value)
    } else {
        let mut value = metric_value;
        apply_metric_binary_arithmetic_to_loki_result(
            &mut value,
            &vector_value,
            arithmetic.op,
            arithmetic.matching.as_ref(),
        );
        retain_metric_binary_on_labels(&mut value, arithmetic.matching.as_ref());
        Ok(value)
    }
}

pub(crate) async fn execute_http_metric_vector_comparison_expression(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    comparison: MetricVectorComparisonExpression,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let metric_value = Box::pin(execute_http_metric_expression_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        &comparison.metric_query,
        query_text,
    ))
    .await?;
    let vector_value = execute_http_scalar_vector_expression_result(
        &comparison.vector_query,
        time_range,
        step,
        kind,
        query_text,
    )?;

    if comparison.vector_on_left {
        let mut value = vector_value;
        apply_metric_binary_comparison_to_loki_result(
            &mut value,
            &metric_value,
            comparison.op,
            comparison.bool_modifier,
            comparison.matching.as_ref(),
        );
        retain_metric_binary_on_labels(&mut value, comparison.matching.as_ref());
        merge_loki_query_stats(&mut value["data"]["stats"], &metric_value["data"]["stats"]);
        Ok(value)
    } else {
        let mut value = metric_value;
        apply_metric_binary_comparison_to_loki_result(
            &mut value,
            &vector_value,
            comparison.op,
            comparison.bool_modifier,
            comparison.matching.as_ref(),
        );
        retain_metric_binary_on_labels(&mut value, comparison.matching.as_ref());
        Ok(value)
    }
}

pub(crate) async fn execute_http_metric_vector_set_expression(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    set: MetricVectorSetExpression,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let metric_value = Box::pin(execute_http_metric_expression_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        &set.metric_query,
        query_text,
    ))
    .await?;
    let vector_value = execute_http_scalar_vector_expression_result(
        &set.vector_query,
        time_range,
        step,
        kind,
        query_text,
    )?;

    if set.vector_on_left {
        let mut value = vector_value;
        if matches!(kind, QueryKind::Instant) {
            normalize_loki_vector_sample_timestamps_to_seconds(&mut value);
        }
        apply_metric_binary_set_to_loki_result(
            &mut value,
            &metric_value,
            set.op,
            set.matching.as_ref(),
        );
        merge_loki_query_stats(&mut value["data"]["stats"], &metric_value["data"]["stats"]);
        Ok(value)
    } else {
        let mut value = metric_value;
        apply_metric_binary_set_to_loki_result(
            &mut value,
            &vector_value,
            set.op,
            set.matching.as_ref(),
        );
        Ok(value)
    }
}

pub(crate) fn normalize_loki_vector_sample_timestamps_to_seconds(value: &mut Value) {
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    for series in results {
        let Some(sample) = series.get_mut("value").and_then(Value::as_array_mut) else {
            continue;
        };
        let Some(timestamp) = sample.get_mut(0) else {
            continue;
        };
        *timestamp = match timestamp {
            Value::Number(number) => {
                let seconds = unix_ns_string_to_loki_seconds(&number.to_string());
                json!(seconds)
            }
            Value::String(text) => json!(unix_ns_string_to_loki_seconds(text)),
            _ => continue,
        };
    }
}
