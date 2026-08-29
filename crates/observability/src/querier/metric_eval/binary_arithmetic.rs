#[allow(clippy::wildcard_imports)]
use super::*;

pub(crate) fn apply_metric_binary_arithmetic_to_series_with_left_operand(
    output_series: &mut Value,
    left_series: &Value,
    op: MetricScalarArithmeticOp,
) -> bool {
    if let Some(output_values) = output_series
        .get_mut("values")
        .and_then(Value::as_array_mut)
    {
        let Some(left_values) = left_series.get("values").and_then(Value::as_array) else {
            return false;
        };
        let mut index = 0;
        while index < output_values.len() {
            let right_sample = output_values[index].clone();
            let Some(left_sample) = matching_metric_binary_sample(&right_sample, left_values)
            else {
                output_values.remove(index);
                continue;
            };
            if apply_metric_binary_arithmetic_to_sample_operands(
                &mut output_values[index],
                left_sample,
                &right_sample,
                op,
            ) {
                index += 1;
            } else {
                output_values.remove(index);
            }
        }
        return !output_values.is_empty();
    }

    let Some(output_sample) = output_series.get_mut("value") else {
        return false;
    };
    let right_sample = output_sample.clone();
    let Some(left_sample) = left_series.get("value") else {
        return false;
    };
    apply_metric_binary_arithmetic_to_sample_operands(output_sample, left_sample, &right_sample, op)
}

pub(crate) fn matching_metric_binary_sample<'a>(
    left_sample: &Value,
    right_values: &'a [Value],
) -> Option<&'a Value> {
    right_values
        .iter()
        .find(|right_sample| metric_binary_sample_timestamps_match(left_sample, right_sample))
}

pub(crate) fn metric_binary_sample_timestamps_match(
    left_sample: &Value,
    right_sample: &Value,
) -> bool {
    match (
        metric_binary_sample_timestamp_ns_candidates(left_sample),
        metric_binary_sample_timestamp_ns_candidates(right_sample),
    ) {
        (Some(left), Some(right)) => left
            .iter()
            .any(|left_timestamp| right.contains(left_timestamp)),
        (None, None) => {
            left_sample.as_array().and_then(|sample| sample.first())
                == right_sample.as_array().and_then(|sample| sample.first())
        }
        _ => false,
    }
}

pub(crate) fn metric_binary_sample_timestamp_ns_candidates(sample: &Value) -> Option<Vec<i64>> {
    let timestamp = sample.as_array()?.first()?;
    if let Some(timestamp) = timestamp.as_i64() {
        return Some(metric_binary_integer_timestamp_ns_candidates(timestamp));
    }
    if let Some(timestamp) = timestamp.as_u64() {
        return i64::try_from(timestamp)
            .ok()
            .map(metric_binary_integer_timestamp_ns_candidates);
    }
    if let Some(timestamp) = timestamp.as_f64() {
        let timestamp = timestamp * 1_000_000_000.0;
        return i64::from_f64(timestamp.round()).map(|timestamp| vec![timestamp]);
    }
    if let Some(timestamp) = timestamp.as_str() {
        let mut candidates = Vec::new();
        if let Some(timestamp) = parse_decimal_seconds_timestamp(timestamp) {
            candidates.push(timestamp);
        }
        if let Ok(timestamp) = timestamp.parse::<i64>() {
            candidates.extend(metric_binary_integer_timestamp_ns_candidates(timestamp));
        }
        candidates.sort_unstable();
        candidates.dedup();
        if !candidates.is_empty() {
            return Some(candidates);
        }
    }
    None
}

pub(crate) fn metric_binary_integer_timestamp_ns_candidates(timestamp: i64) -> Vec<i64> {
    let mut candidates = vec![timestamp];
    if let Some(seconds_timestamp) = timestamp.checked_mul(1_000_000_000) {
        candidates.push(seconds_timestamp);
    }
    candidates.sort_unstable();
    candidates.dedup();
    candidates
}

pub(crate) fn apply_metric_binary_arithmetic_to_sample(
    left_sample: &mut Value,
    right_sample: &Value,
    op: MetricScalarArithmeticOp,
) -> bool {
    let original_left = left_sample.clone();
    apply_metric_binary_arithmetic_to_sample_operands(left_sample, &original_left, right_sample, op)
}

pub(crate) fn apply_metric_binary_arithmetic_to_sample_operands(
    output_sample: &mut Value,
    left_sample: &Value,
    right_sample: &Value,
    op: MetricScalarArithmeticOp,
) -> bool {
    let Some(output_values) = output_sample.as_array_mut() else {
        return false;
    };
    let Some(left_values) = left_sample.as_array() else {
        return false;
    };
    let Some(right_values) = right_sample.as_array() else {
        return false;
    };
    if !metric_binary_sample_timestamps_match(left_sample, right_sample) {
        return false;
    }
    let Some(left_value) = left_values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let Some(right_value) = right_values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let Some(result) = metric_scalar_arithmetic_value(left_value, op, right_value, false) else {
        return false;
    };
    if let Some(value) = output_values.get_mut(1) {
        *value = json!(format_metric_value(result));
    }
    true
}

pub(crate) fn apply_metric_binary_comparison_to_loki_result(
    left: &mut Value,
    right: &Value,
    op: ComparisonOp,
    bool_modifier: bool,
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
        apply_metric_binary_comparison_group_right_to_results(
            left_results,
            right_results,
            op,
            bool_modifier,
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

        if apply_metric_binary_comparison_to_series(
            &mut left_results[index],
            right_series,
            op,
            bool_modifier,
        ) {
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

pub(crate) fn apply_metric_binary_comparison_group_right_to_results(
    left_results: &mut Vec<Value>,
    right_results: &[Value],
    op: ComparisonOp,
    bool_modifier: bool,
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
        if apply_metric_binary_comparison_to_series_with_left_operand(
            &mut output_series,
            left_series,
            op,
            bool_modifier,
        ) {
            include_metric_group_labels(&mut output_series, left_series, group_labels);
            left_results.push(output_series);
        }
    }
}

pub(crate) fn apply_metric_binary_comparison_to_series(
    left_series: &mut Value,
    right_series: &Value,
    op: ComparisonOp,
    bool_modifier: bool,
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
            if apply_metric_binary_comparison_to_sample(
                &mut left_values[index],
                right_sample,
                op,
                bool_modifier,
            ) {
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
    apply_metric_binary_comparison_to_sample(left_sample, right_sample, op, bool_modifier)
}

pub(crate) fn apply_metric_binary_comparison_to_series_with_left_operand(
    output_series: &mut Value,
    left_series: &Value,
    op: ComparisonOp,
    bool_modifier: bool,
) -> bool {
    if let Some(output_values) = output_series
        .get_mut("values")
        .and_then(Value::as_array_mut)
    {
        let Some(left_values) = left_series.get("values").and_then(Value::as_array) else {
            return false;
        };
        let mut index = 0;
        while index < output_values.len() {
            let right_sample = output_values[index].clone();
            let Some(left_sample) = matching_metric_binary_sample(&right_sample, left_values)
            else {
                output_values.remove(index);
                continue;
            };
            if apply_metric_binary_comparison_to_sample_operands(
                &mut output_values[index],
                left_sample,
                &right_sample,
                op,
                bool_modifier,
            ) {
                index += 1;
            } else {
                output_values.remove(index);
            }
        }
        return !output_values.is_empty();
    }

    let Some(output_sample) = output_series.get_mut("value") else {
        return false;
    };
    let right_sample = output_sample.clone();
    let Some(left_sample) = left_series.get("value") else {
        return false;
    };
    apply_metric_binary_comparison_to_sample_operands(
        output_sample,
        left_sample,
        &right_sample,
        op,
        bool_modifier,
    )
}

pub(crate) fn apply_metric_binary_comparison_to_sample(
    left_sample: &mut Value,
    right_sample: &Value,
    op: ComparisonOp,
    bool_modifier: bool,
) -> bool {
    let original_left = left_sample.clone();
    apply_metric_binary_comparison_to_sample_operands(
        left_sample,
        &original_left,
        right_sample,
        op,
        bool_modifier,
    )
}

pub(crate) fn apply_metric_binary_comparison_to_sample_operands(
    output_sample: &mut Value,
    left_sample: &Value,
    right_sample: &Value,
    op: ComparisonOp,
    bool_modifier: bool,
) -> bool {
    let Some(output_values) = output_sample.as_array_mut() else {
        return false;
    };
    let Some(left_values) = left_sample.as_array() else {
        return false;
    };
    let Some(right_values) = right_sample.as_array() else {
        return false;
    };
    if !metric_binary_sample_timestamps_match(left_sample, right_sample) {
        return false;
    }
    let Some(left_value) = left_values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let Some(right_value) = right_values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
    else {
        return false;
    };
    let matches = metric_scalar_comparison_matches(left_value, op, right_value, false);
    if bool_modifier {
        if let Some(value) = output_values.get_mut(1) {
            *value = json!(if matches { "1" } else { "0" });
        }
        true
    } else {
        if matches
            && let (Some(output), Some(left)) = (output_values.get_mut(1), left_values.get(1))
        {
            *output = left.clone();
        }
        matches
    }
}
