fn execute_http_scalar_vector_expression_result(
    query: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let vector_result =
        scalar_vector_expression_result(query).ok_or_else(|| HttpQueryError::LokiParse {
            query: query_text.to_string(),
            source: ParseError::Syntax {
                message: "expected vector expression".to_string(),
                position: 0,
            },
        })?;
    let value = match kind {
        QueryKind::Instant => {
            loki_instant_scalar_or_vector_response(time_range.end_ns, vector_result)
        }
        QueryKind::Range => loki_range_vector_response(
            time_range,
            resolved_range_step(step, time_range)?,
            vector_result,
        ),
    };
    Ok(add_loki_query_stats(value))
}

fn retain_metric_binary_on_labels(value: &mut Value, matching: Option<&MetricVectorMatching>) {
    let Some(MetricVectorMatching::On {
        labels,
        group: None,
    }) = matching
    else {
        return;
    };
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    for series in results {
        let Some(metric) = series.get_mut("metric").and_then(Value::as_object_mut) else {
            continue;
        };
        metric.retain(|label, _| labels.contains(label));
    }
}

fn sort_loki_vector_result(value: &mut Value, descending: bool) {
    if value.pointer("/data/resultType").and_then(Value::as_str) != Some("vector") {
        return;
    }
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    results.sort_by(|left, right| {
        let ordering = match (
            loki_vector_sample_value(left),
            loki_vector_sample_value(right),
        ) {
            (Some(left), Some(right)) => left.cmp_value(right),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        };
        if descending {
            ordering.reverse()
        } else {
            ordering
        }
    });
}

fn loki_vector_sample_value(sample: &Value) -> Option<MetricValue> {
    sample
        .pointer("/value/1")
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
}

fn metric_query_uses_approx_topk(query: &MetricQuery) -> bool {
    query
        .vector_aggregation
        .as_ref()
        .is_some_and(|aggregation| matches!(aggregation.op, VectorAggregationOp::ApproxTopK(_)))
}

fn metric_query_uses_count_values(query: &MetricQuery) -> bool {
    query
        .vector_aggregation
        .as_ref()
        .is_some_and(|aggregation| matches!(aggregation.op, VectorAggregationOp::CountValues(_)))
}

async fn execute_http_metric_binary_arithmetic_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    arithmetic: MetricBinaryArithmetic,
) -> Result<Value, HttpQueryError> {
    let mut left = execute_http_metric_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        arithmetic.left.clone(),
    )
    .await?;
    let right =
        execute_http_metric_query(state, tenant, time_range, step, kind, arithmetic.right).await?;
    apply_metric_binary_arithmetic_to_loki_result(
        &mut left,
        &right,
        arithmetic.op,
        arithmetic.matching.as_ref(),
    );
    Ok(left)
}

async fn execute_http_metric_binary_comparison_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    comparison: MetricBinaryComparison,
) -> Result<Value, HttpQueryError> {
    let mut left = execute_http_metric_query(
        state,
        tenant,
        time_range,
        step,
        kind,
        comparison.left.clone(),
    )
    .await?;
    let right =
        execute_http_metric_query(state, tenant, time_range, step, kind, comparison.right).await?;
    apply_metric_binary_comparison_to_loki_result(
        &mut left,
        &right,
        comparison.op,
        comparison.bool_modifier,
        comparison.matching.as_ref(),
    );
    Ok(left)
}

async fn execute_http_metric_binary_set_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    set: MetricBinarySet,
) -> Result<Value, HttpQueryError> {
    let mut left =
        execute_http_metric_query(state, tenant, time_range, step, kind, set.left.clone()).await?;
    let right = execute_http_metric_query(state, tenant, time_range, step, kind, set.right).await?;
    apply_metric_binary_set_to_loki_result(&mut left, &right, set.op, set.matching.as_ref());
    Ok(left)
}

async fn execute_http_metric_scalar_comparison_query(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
    step: Option<i64>,
    kind: QueryKind,
    comparison: MetricScalarComparison,
    query_text: &str,
) -> Result<Value, HttpQueryError> {
    let query = comparison.query.clone();
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
        apply_metric_scalar_comparison_to_loki_result(&mut response, &comparison, query_text)?;
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
    apply_metric_scalar_comparison_to_loki_result(&mut response, &comparison, query_text)?;
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

async fn execute_http_metric_scalar_arithmetic_query(
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

fn apply_metric_binary_arithmetic_to_loki_result(
    left: &mut Value,
    right: &Value,
    op: MetricScalarArithmeticOp,
    matching: Option<&MetricVectorMatching>,
) {
    let Some(left_results) = left
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    let Some(right_results) = right.pointer("/data/result").and_then(Value::as_array) else {
        left_results.clear();
        return;
    };

    if let Some(MetricVectorGroupModifier::Right(group_labels)) =
        metric_vector_group_modifier(matching)
    {
        apply_metric_binary_arithmetic_group_right_to_results(
            left_results,
            right_results,
            op,
            matching,
            group_labels,
        );
        return;
    }

    let mut index = 0;
    while index < left_results.len() {
        let Some(left_labels) = metric_series_labels(&left_results[index]) else {
            left_results.remove(index);
            continue;
        };
        let left_key = metric_vector_matching_key(&left_labels, matching);
        let Some(right_series) = right_results.iter().find(|series| {
            metric_series_labels(series).is_some_and(|right_labels| {
                metric_vector_matching_key(&right_labels, matching) == left_key
            })
        }) else {
            left_results.remove(index);
            continue;
        };

        if apply_metric_binary_arithmetic_to_series(&mut left_results[index], right_series, op) {
            if let Some(MetricVectorGroupModifier::Left(group_labels)) =
                metric_vector_group_modifier(matching)
            {
                include_metric_group_labels(&mut left_results[index], right_series, group_labels);
            }
            index += 1;
        } else {
            left_results.remove(index);
        }
    }
}

fn apply_metric_binary_arithmetic_group_right_to_results(
    left_results: &mut Vec<Value>,
    right_results: &[Value],
    op: MetricScalarArithmeticOp,
    matching: Option<&MetricVectorMatching>,
    group_labels: &[String],
) {
    let original_left = std::mem::take(left_results);
    for right_series in right_results {
        let Some(right_labels) = metric_series_labels(right_series) else {
            continue;
        };
        let right_key = metric_vector_matching_key(&right_labels, matching);
        let Some(left_series) = original_left.iter().find(|series| {
            metric_series_labels(series)
                .is_some_and(|labels| metric_vector_matching_key(&labels, matching) == right_key)
        }) else {
            continue;
        };
        let mut output_series = right_series.clone();
        if apply_metric_binary_arithmetic_to_series_with_left_operand(
            &mut output_series,
            left_series,
            op,
        ) {
            include_metric_group_labels(&mut output_series, left_series, group_labels);
            left_results.push(output_series);
        }
    }
}

fn apply_metric_binary_arithmetic_to_series(
    left_series: &mut Value,
    right_series: &Value,
    op: MetricScalarArithmeticOp,
) -> bool {
    if let Some(left_values) = left_series.get_mut("values").and_then(Value::as_array_mut) {
        let Some(right_values) = right_series.get("values").and_then(Value::as_array) else {
            return false;
        };
        let mut index = 0;
        while index < left_values.len() {
            let Some(right_sample) =
                matching_metric_binary_sample(&left_values[index], right_values)
            else {
                left_values.remove(index);
                continue;
            };
            if apply_metric_binary_arithmetic_to_sample(&mut left_values[index], right_sample, op) {
                index += 1;
            } else {
                left_values.remove(index);
            }
        }
        return !left_values.is_empty();
    }

    let Some(left_sample) = left_series.get_mut("value") else {
        return false;
    };
    let Some(right_sample) = right_series.get("value") else {
        return false;
    };
    apply_metric_binary_arithmetic_to_sample(left_sample, right_sample, op)
}

