#![cfg_attr(test, allow(dead_code, unused_imports))]

use crate::querier::scan::{MetricEvaluationRange, MetricRangeScanRequest};

use crate::{
    ActiveLogDeleteFilter, Arc, BTreeSet, ColdBlockScan, ComparisonOp, HttpQueryError, Labels,
    MetricBinarySetOp, MetricQuery, MetricScalarArithmeticOp, MetricValue,
    MetricVectorGroupModifier, MetricVectorMatching, Ordering, ParseError, QuerierState,
    QueryHotTail, SampleOrder, StreamPlan, TimeRange, Value,
    execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes,
    execute_metric_query_range_with_deletes,
    execute_metric_query_range_with_hot_tail_frontier_and_deletes, format_metric_value,
    hot_tail_snapshot, json, json_object_to_labels, matching_metric_binary_sample,
    metric_binary_sample_timestamps_match, parse_metric_sample_value,
};

fn apply_scalar_to_series(
    series: &mut Value,
    mut apply_sample: impl FnMut(&mut Value) -> bool,
) -> bool {
    if let Some(values) = series.get_mut("values").and_then(Value::as_array_mut) {
        let mut index = 0;
        while index < values.len() {
            if apply_sample(&mut values[index]) {
                index += 1;
            } else {
                values.remove(index);
            }
        }
        return !values.is_empty();
    }

    series.get_mut("value").is_some_and(&mut apply_sample)
}

fn scalar_operands_for_sample(
    sample: &mut Value,
    scalar: MetricValue,
    scalar_side: ScalarSide,
) -> Option<(&mut Value, ScalarOperands)> {
    let SampleValueSlot { slot, sample_value } = sample_value_slot(sample)?;
    Some((
        slot,
        ScalarOperands {
            sample: sample_value,
            scalar,
            scalar_side,
        },
    ))
}
#[cfg(test)]
use crate::{MetricScalarArithmetic, MetricScalarComparison};

mod apply_metric_scalar_arithmetic_to_loki_result;
mod apply_metric_scalar_comparison_to_loki_result;
mod apply_metric_scalar_comparison_to_sample;
mod apply_metric_scalar_comparison_to_series;
mod apply_metric_selection;
mod metric_comparison;
mod scalar_operands;

#[cfg(test)]
pub(crate) use apply_metric_scalar_arithmetic_to_loki_result::apply_metric_scalar_arithmetic_to_loki_result;
pub(crate) use apply_metric_scalar_arithmetic_to_loki_result::apply_scalar_arithmetic_to_loki_result;
#[cfg(test)]
pub(crate) use apply_metric_scalar_comparison_to_loki_result::apply_metric_scalar_comparison_to_loki_result;
pub(crate) use apply_metric_scalar_comparison_to_loki_result::apply_scalar_comparison_to_loki_result;
#[cfg(test)]
pub(crate) use apply_metric_scalar_comparison_to_sample::apply_metric_scalar_comparison_to_sample;
pub(crate) use apply_metric_scalar_comparison_to_sample::apply_scalar_comparison_to_sample;
#[cfg(test)]
pub(crate) use apply_metric_scalar_comparison_to_series::apply_metric_scalar_comparison_to_series;
pub(crate) use apply_metric_scalar_comparison_to_series::apply_scalar_comparison_to_series;
pub(crate) use apply_metric_selection::apply_metric_selection;
pub(crate) use metric_comparison::{
    ComparisonResult, MetricComparison, ScalarArithmetic, ScalarComparison,
};
pub(crate) use scalar_operands::{ScalarOperands, ScalarSide};

pub(crate) fn default_metric_range_step(time_range: TimeRange) -> i64 {
    time_range.end_ns.saturating_sub(time_range.start_ns).max(1)
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

pub(crate) fn metric_series_labels(series: &Value) -> Option<Labels> {
    series.get("metric").and_then(json_object_to_labels)
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

pub(crate) fn sort_loki_metric_results_by_labels(results: &mut [Value]) {
    results.sort_by_key(metric_series_labels);
}

pub(crate) fn apply_metric_scalar_arithmetic_to_sample(
    sample: &mut Value,
    arithmetic: ScalarArithmetic,
    scalar: MetricValue,
) -> bool {
    let Some((slot, operands)) = scalar_operands_for_sample(sample, scalar, arithmetic.scalar_side)
    else {
        return false;
    };
    let Some(result) = metric_scalar_arithmetic_value(operands, arithmetic.op) else {
        return false;
    };
    *slot = json!(format_metric_value(result));
    true
}

pub(crate) fn apply_metric_scalar_arithmetic_to_series(
    series: &mut Value,
    arithmetic: ScalarArithmetic,
    scalar: MetricValue,
) -> bool {
    apply_scalar_to_series(series, |sample| {
        apply_metric_scalar_arithmetic_to_sample(sample, arithmetic, scalar)
    })
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

pub(crate) fn metric_scalar_arithmetic_value(
    operands: ScalarOperands,
    op: MetricScalarArithmeticOp,
) -> Option<MetricValue> {
    let pair = operands.left_and_right();
    let left = pair.left;
    let right = pair.right;
    match op {
        MetricScalarArithmeticOp::Add => Some(left.add(right)),
        MetricScalarArithmeticOp::Subtract => Some(left.subtract(right)),
        MetricScalarArithmeticOp::Multiply => Some(left.multiply(right)),
        MetricScalarArithmeticOp::Divide => left.divide(right),
        MetricScalarArithmeticOp::Modulo => left.modulo(right),
        MetricScalarArithmeticOp::Power => left.power(right),
    }
}

pub(crate) fn metric_scalar_comparison_matches(operands: ScalarOperands, op: ComparisonOp) -> bool {
    let pair = operands.left_and_right();
    let left = pair.left;
    let right = pair.right;
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

pub(crate) fn metric_vector_matching_key(
    labels: &Labels,
    matching: Option<&MetricVectorMatching>,
) -> Labels {
    match matching {
        None => labels.clone(),
        Some(MetricVectorMatching::On { labels: names, .. }) => {
            let mut key = Labels::new();
            for name in names {
                if let Some(value) = labels.get(name) {
                    key.insert(name.clone(), value.clone());
                }
            }
            key
        }
        Some(MetricVectorMatching::Ignoring { labels: names, .. }) => {
            let mut key = Labels::new();
            for entry in labels {
                if !names.contains(entry.0) {
                    key.insert(entry.0.clone(), entry.1.clone());
                }
            }
            key
        }
    }
}

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

    crate::querier::metric_eval::binary_arithmetic::retain_matching_metric_result_series(
        left_results,
        right_results,
        matching,
        |left_series, right_series| match (op, right_series) {
            (MetricBinarySetOp::And | MetricBinarySetOp::Unless, Some(right_series)) => {
                apply_metric_binary_set_to_series(left_series, right_series, op)
            }
            (MetricBinarySetOp::Unless, None) | (MetricBinarySetOp::Or, Some(_)) => true,
            (MetricBinarySetOp::And | MetricBinarySetOp::Or, None) => false,
        },
    );
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
        let snapshot = hot_tail_snapshot(state, plan.time_range);
        return execute_metric_query_range_from_object_store_with_hot_tail_frontier_and_deletes(
            ColdBlockScan {
                store: Arc::clone(&cold_store.store),
                prefix: &cold_store.prefix,
                block_fetch_concurrency: state.cold_block_fetch_concurrency,
            },
            MetricRangeScanRequest::new(
                plan,
                query,
                &state.label_index,
                MetricEvaluationRange::new(time_range, step_ns),
                QueryHotTail {
                    records: &snapshot.records,
                    frontier: &snapshot.frontier,
                    delete_filters,
                },
            ),
        )
        .await
        .map_err(HttpQueryError::from);
    }
    if let Some(hot_tail) = &state.hot_tail {
        let records = hot_tail
            .source
            .records_shared_in_range(plan.time_range.start_ns, plan.time_range.end_ns);
        let frontier = hot_tail.frontier.snapshot();
        return execute_metric_query_range_with_hot_tail_frontier_and_deletes(
            &state.root,
            MetricRangeScanRequest::new(
                plan,
                query,
                &state.label_index,
                MetricEvaluationRange::new(time_range, step_ns),
                QueryHotTail {
                    records: &records,
                    frontier: &frontier,
                    delete_filters,
                },
            ),
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

/// The value slot of a `[timestamp, "value"]` sample, and the value it holds.
pub(crate) struct SampleValueSlot<'a> {
    pub(crate) slot: &'a mut Value,
    pub(crate) sample_value: MetricValue,
}

/// Finds the value slot of `sample`, when the sample is an array whose second
/// element is a decimal string.
pub(crate) fn sample_value_slot(sample: &mut Value) -> Option<SampleValueSlot<'_>> {
    let slot = sample.as_array_mut()?.get_mut(1)?;
    let sample_value = slot.as_str().and_then(parse_metric_sample_value)?;
    Some(SampleValueSlot { slot, sample_value })
}

/// The scalar literal operand of a binary operator in `query`.
#[derive(Clone, Copy)]
pub(crate) struct ScalarLiteral<'a> {
    pub(crate) text: &'a str,
    pub(crate) query: &'a str,
}

impl ScalarLiteral<'_> {
    /// The literal's value. An unparsable literal is a parse error of the
    /// query.
    pub(crate) fn parse(self) -> Result<MetricValue, HttpQueryError> {
        parse_metric_sample_value(self.text).ok_or_else(|| HttpQueryError::LokiParse {
            query: self.query.to_string(),
            source: ParseError::Syntax {
                message: "expected scalar literal".to_string(),
                position: 0,
            },
        })
    }
}

/// Keeps each series of a Loki result that `apply_series` accepts with
/// `scalar` as its operand.
pub(crate) fn apply_scalar_to_loki_result(
    loki_result: &mut Value,
    scalar: MetricValue,
    apply_series: impl Fn(&mut Value, MetricValue) -> bool,
) {
    let Some(results) = loki_result
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };

    results.retain_mut(|series| apply_series(series, scalar));
}
