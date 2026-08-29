use super::*;

pub(crate) fn apply_metric_binary_set_to_loki_result(
    left: &mut Value,
    right: &Value,
    op: MetricBinarySetOp,
    matching: Option<&MetricVectorMatching>,
) {
    let Some(left_results) = left
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    let Some(right_results) = right.pointer("/data/result").and_then(Value::as_array) else {
        if matches!(op, MetricBinarySetOp::And) {
            left_results.clear();
        }
        return;
    };

    if matches!(op, MetricBinarySetOp::Or) {
        let left_label_sets = left_results
            .iter()
            .filter_map(metric_series_labels)
            .map(|labels| metric_vector_matching_key(&labels, matching))
            .collect::<BTreeSet<_>>();
        for right_series in right_results {
            let Some(right_labels) = metric_series_labels(right_series) else {
                continue;
            };
            let right_key = metric_vector_matching_key(&right_labels, matching);
            if !left_label_sets.contains(&right_key) {
                left_results.push(right_series.clone());
            }
        }
        sort_loki_metric_results_by_labels(left_results);
        return;
    }

    let mut index = 0;
    while index < left_results.len() {
        let Some(left_labels) = metric_series_labels(&left_results[index]) else {
            left_results.remove(index);
            continue;
        };
        let left_key = metric_vector_matching_key(&left_labels, matching);
        let right_series = right_results.iter().find(|series| {
            metric_series_labels(series)
                .is_some_and(|labels| metric_vector_matching_key(&labels, matching) == left_key)
        });
        let keep = match (op, right_series) {
            (MetricBinarySetOp::And | MetricBinarySetOp::Unless, Some(right_series)) => {
                apply_metric_binary_set_to_series(&mut left_results[index], right_series, op)
            }
            (MetricBinarySetOp::And, None) => false,
            (MetricBinarySetOp::Unless, None) | (MetricBinarySetOp::Or, _) => true,
        };
        if keep {
            index += 1;
        } else {
            left_results.remove(index);
        }
    }
}

pub(crate) fn metric_series_labels(series: &Value) -> Option<Labels> {
    series.get("metric").and_then(json_object_to_labels)
}

pub(crate) fn sort_loki_metric_results_by_labels(results: &mut [Value]) {
    results.sort_by_key(metric_series_labels);
}

pub(crate) fn metric_vector_matching_key(
    labels: &Labels,
    matching: Option<&MetricVectorMatching>,
) -> Labels {
    match matching {
        None => labels.clone(),
        Some(MetricVectorMatching::On { labels: names, .. }) => names
            .iter()
            .filter_map(|name| labels.get(name).map(|value| (name.clone(), value.clone())))
            .collect(),
        Some(MetricVectorMatching::Ignoring { labels: names, .. }) => labels
            .iter()
            .filter(|(name, _)| !names.contains(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
    }
}

pub(crate) fn metric_vector_group_modifier(
    matching: Option<&MetricVectorMatching>,
) -> Option<&MetricVectorGroupModifier> {
    match matching {
        Some(
            MetricVectorMatching::On { group, .. } | MetricVectorMatching::Ignoring { group, .. },
        ) => group.as_ref(),
        None => None,
    }
}

pub(crate) fn include_metric_group_labels(
    output_series: &mut Value,
    source_series: &Value,
    labels: &[String],
) {
    if labels.is_empty() {
        return;
    }
    let Some(source_metric) = source_series.get("metric").and_then(Value::as_object) else {
        return;
    };
    let Some(output_metric) = output_series
        .get_mut("metric")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    for label in labels {
        output_metric.remove(label);
        if let Some(value) = source_metric.get(label).and_then(Value::as_str) {
            output_metric.insert(label.clone(), json!(value));
        }
    }
}

pub(crate) fn apply_metric_binary_set_to_series(
    left_series: &mut Value,
    right_series: &Value,
    op: MetricBinarySetOp,
) -> bool {
    if let Some(left_values) = left_series.get_mut("values").and_then(Value::as_array_mut) {
        let right_values = right_series.get("values").and_then(Value::as_array);
        let mut index = 0;
        while index < left_values.len() {
            let matched = right_values
                .and_then(|right_values| {
                    matching_metric_binary_sample(&left_values[index], right_values)
                })
                .is_some();
            if metric_binary_set_keeps_sample(op, matched) {
                index += 1;
            } else {
                left_values.remove(index);
            }
        }
        return !left_values.is_empty();
    }

    let Some(left_sample) = left_series.get("value") else {
        return false;
    };
    let matched = right_series
        .get("value")
        .is_some_and(|right_sample| metric_samples_share_timestamp(left_sample, right_sample));
    metric_binary_set_keeps_sample(op, matched)
}

pub(crate) fn metric_binary_set_keeps_sample(op: MetricBinarySetOp, matched: bool) -> bool {
    match op {
        MetricBinarySetOp::And => matched,
        MetricBinarySetOp::Or => true,
        MetricBinarySetOp::Unless => !matched,
    }
}

pub(crate) fn metric_samples_share_timestamp(left_sample: &Value, right_sample: &Value) -> bool {
    metric_binary_sample_timestamps_match(left_sample, right_sample)
}

pub(crate) fn apply_metric_scalar_arithmetic_to_loki_result(
    value: &mut Value,
    arithmetic: &MetricScalarArithmetic,
    query: &str,
) -> Result<(), HttpQueryError> {
    let scalar =
        parse_metric_sample_value(&arithmetic.scalar).ok_or_else(|| HttpQueryError::LokiParse {
            query: query.to_string(),
            source: ParseError::Syntax {
                message: "expected scalar literal".to_string(),
                position: 0,
            },
        })?;
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return Ok(());
    };

    let mut index = 0;
    while index < results.len() {
        if apply_metric_scalar_arithmetic_to_series(
            &mut results[index],
            arithmetic.op,
            scalar,
            arithmetic.scalar_on_left,
        ) {
            index += 1;
        } else {
            results.remove(index);
        }
    }
    Ok(())
}

pub(crate) fn apply_metric_scalar_arithmetic_to_series(
    series: &mut Value,
    op: MetricScalarArithmeticOp,
    scalar: MetricValue,
    scalar_on_left: bool,
) -> bool {
    if let Some(values) = series.get_mut("values").and_then(Value::as_array_mut) {
        let mut index = 0;
        while index < values.len() {
            if apply_metric_scalar_arithmetic_to_sample(
                &mut values[index],
                op,
                scalar,
                scalar_on_left,
            ) {
                index += 1;
            } else {
                values.remove(index);
            }
        }
        return !values.is_empty();
    }

    let Some(sample) = series.get_mut("value") else {
        return false;
    };
    apply_metric_scalar_arithmetic_to_sample(sample, op, scalar, scalar_on_left)
}

pub(crate) fn apply_metric_scalar_arithmetic_to_sample(
    sample: &mut Value,
    op: MetricScalarArithmeticOp,
    scalar: MetricValue,
    scalar_on_left: bool,
) -> bool {
    let Some(values) = sample.as_array_mut() else {
        return false;
    };
    let Some(sample_value) = values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let Some(result) = metric_scalar_arithmetic_value(sample_value, op, scalar, scalar_on_left)
    else {
        return false;
    };
    if let Some(value) = values.get_mut(1) {
        *value = json!(format_metric_value(result));
    }
    true
}

pub(crate) fn metric_scalar_arithmetic_value(
    sample: MetricValue,
    op: MetricScalarArithmeticOp,
    scalar: MetricValue,
    scalar_on_left: bool,
) -> Option<MetricValue> {
    let (left, right) = if scalar_on_left {
        (scalar, sample)
    } else {
        (sample, scalar)
    };
    match op {
        MetricScalarArithmeticOp::Add => Some(left.add(right)),
        MetricScalarArithmeticOp::Subtract => Some(left.subtract(right)),
        MetricScalarArithmeticOp::Multiply => Some(left.multiply(right)),
        MetricScalarArithmeticOp::Divide => left.divide(right),
        MetricScalarArithmeticOp::Modulo => left.modulo(right),
        MetricScalarArithmeticOp::Power => left.power(right),
    }
}

pub(crate) fn apply_metric_scalar_comparison_to_loki_result(
    value: &mut Value,
    comparison: &MetricScalarComparison,
    query: &str,
) -> Result<(), HttpQueryError> {
    let scalar =
        parse_metric_sample_value(&comparison.scalar).ok_or_else(|| HttpQueryError::LokiParse {
            query: query.to_string(),
            source: ParseError::Syntax {
                message: "expected scalar literal".to_string(),
                position: 0,
            },
        })?;
    let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return Ok(());
    };

    let mut index = 0;
    while index < results.len() {
        if apply_metric_scalar_comparison_to_series(&mut results[index], comparison, scalar) {
            index += 1;
        } else {
            results.remove(index);
        }
    }
    Ok(())
}

pub(crate) fn apply_metric_scalar_comparison_to_series(
    series: &mut Value,
    comparison: &MetricScalarComparison,
    scalar: MetricValue,
) -> bool {
    if let Some(values) = series.get_mut("values").and_then(Value::as_array_mut) {
        let mut index = 0;
        while index < values.len() {
            if apply_metric_scalar_comparison_to_sample(&mut values[index], comparison, scalar) {
                index += 1;
            } else {
                values.remove(index);
            }
        }
        return !values.is_empty();
    }

    let Some(sample) = series.get_mut("value") else {
        return false;
    };
    apply_metric_scalar_comparison_to_sample(sample, comparison, scalar)
}

pub(crate) fn apply_metric_scalar_comparison_to_sample(
    sample: &mut Value,
    comparison: &MetricScalarComparison,
    scalar: MetricValue,
) -> bool {
    let Some(values) = sample.as_array_mut() else {
        return false;
    };
    let Some(sample_value) = values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let matches = metric_scalar_comparison_matches(
        sample_value,
        comparison.op,
        scalar,
        comparison.scalar_on_left,
    );
    if comparison.bool_modifier {
        if let Some(value) = values.get_mut(1) {
            *value = json!(if matches { "1" } else { "0" });
        }
        true
    } else {
        matches
    }
}

pub(crate) fn metric_scalar_comparison_matches(
    sample: MetricValue,
    op: ComparisonOp,
    scalar: MetricValue,
    scalar_on_left: bool,
) -> bool {
    let (left, right) = if scalar_on_left {
        (scalar, sample)
    } else {
        (sample, scalar)
    };
    let ordering = left.cmp_value(right);
    match op {
        ComparisonOp::Equal => ordering == Ordering::Equal,
        ComparisonOp::NotEqual => ordering != Ordering::Equal,
        ComparisonOp::RegexEqual | ComparisonOp::RegexNotEqual => false,
        ComparisonOp::Greater => ordering == Ordering::Greater,
        ComparisonOp::GreaterEqual => matches!(ordering, Ordering::Greater | Ordering::Equal),
        ComparisonOp::Less => ordering == Ordering::Less,
        ComparisonOp::LessEqual => matches!(ordering, Ordering::Less | Ordering::Equal),
    }
}

pub(crate) fn default_metric_range_step(time_range: TimeRange) -> i64 {
    time_range.end_ns.saturating_sub(time_range.start_ns).max(1)
}

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
