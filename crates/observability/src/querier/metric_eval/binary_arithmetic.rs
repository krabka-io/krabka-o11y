use crate::querier::metric_eval::MetricOperandValues;
use num_traits::FromPrimitive as _;

use crate::{
    ComparisonResult, MetricComparison, MetricScalarArithmeticOp, MetricVectorGroupModifier,
    MetricVectorMatching, ScalarOperands, ScalarSide, Value, format_metric_value,
    include_metric_group_labels, json, metric_scalar_arithmetic_value,
    metric_scalar_comparison_matches, metric_series_labels, metric_vector_group_modifier,
    metric_vector_matching_key, parse_decimal_seconds_timestamp, parse_metric_sample_value,
};

/// Keeps left-hand result series accepted by `keep`, paired with a matching
/// right-hand series when one exists.
pub(crate) fn retain_matching_metric_result_series(
    left_results: &mut Vec<Value>,
    right_results: &[Value],
    matching: Option<&MetricVectorMatching>,
    mut keep: impl FnMut(&mut Value, Option<&Value>) -> bool,
) {
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

        if keep(&mut left_results[index], right_series) {
            index += 1;
        } else {
            left_results.remove(index);
        }
    }
}

fn output_sample_scalar_operands<'a>(
    output_sample: &'a mut Value,
    operands: SampleOperands<'_>,
) -> Option<(&'a mut Vec<Value>, ScalarOperands)> {
    let output_values = output_sample.as_array_mut()?;
    let values = metric_binary_operand_values(operands)?;
    Some((
        output_values,
        ScalarOperands {
            sample: values.left,
            scalar: values.right,
            scalar_side: ScalarSide::Right,
        },
    ))
}

pub(crate) fn matching_metric_binary_sample<'a>(
    left_sample: &Value,
    right_values: &'a [Value],
) -> Option<&'a Value> {
    right_values
        .iter()
        .find(|right_sample| metric_binary_sample_timestamps_match(left_sample, right_sample))
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
    apply_metric_binary_arithmetic_to_sample_operands(
        left_sample,
        SampleOperands {
            left: &original_left,
            right: right_sample,
        },
        op,
    )
}

pub(crate) fn apply_metric_binary_arithmetic_to_sample_operands(
    output_sample: &mut Value,
    operands: SampleOperands<'_>,
    op: MetricScalarArithmeticOp,
) -> bool {
    let Some((output_values, scalar_operands)) =
        output_sample_scalar_operands(output_sample, operands)
    else {
        return false;
    };
    let Some(result) = metric_scalar_arithmetic_value(scalar_operands, op) else {
        return false;
    };
    if let Some(value) = output_values.get_mut(1) {
        *value = json!(format_metric_value(result));
    }
    true
}

pub(crate) fn apply_metric_binary_arithmetic_to_series_with_left_operand(
    output_series: &mut Value,
    left_series: &Value,
    op: MetricScalarArithmeticOp,
) -> bool {
    apply_metric_binary_to_series_with_left_operand(
        output_series,
        left_series,
        |output_sample, left_sample, right_sample| {
            apply_metric_binary_arithmetic_to_sample_operands(
                output_sample,
                SampleOperands {
                    left: left_sample,
                    right: right_sample,
                },
                op,
            )
        },
    )
}

pub(crate) fn apply_metric_binary_comparison_to_sample(
    left_sample: &mut Value,
    right_sample: &Value,
    comparison: MetricComparison,
) -> bool {
    let original_left = left_sample.clone();
    apply_metric_binary_comparison_to_sample_operands(
        left_sample,
        SampleOperands {
            left: &original_left,
            right: right_sample,
        },
        comparison,
    )
}

pub(crate) fn apply_metric_binary_comparison_to_series(
    left_series: &mut Value,
    right_series: &Value,
    comparison: MetricComparison,
) -> bool {
    apply_metric_binary_to_series(left_series, right_series, |left_sample, right_sample| {
        apply_metric_binary_comparison_to_sample(left_sample, right_sample, comparison)
    })
}

pub(crate) fn apply_metric_binary_comparison_to_series_with_left_operand(
    output_series: &mut Value,
    left_series: &Value,
    comparison: MetricComparison,
) -> bool {
    apply_metric_binary_to_series_with_left_operand(
        output_series,
        left_series,
        |output_sample, left_sample, right_sample| {
            apply_metric_binary_comparison_to_sample_operands(
                output_sample,
                SampleOperands {
                    left: left_sample,
                    right: right_sample,
                },
                comparison,
            )
        },
    )
}

pub(crate) fn metric_binary_sample_timestamps_match(
    left_sample: &Value,
    right_sample: &Value,
) -> bool {
    let left_candidates = metric_binary_sample_timestamp_ns_candidates(left_sample);
    let right_candidates = metric_binary_sample_timestamp_ns_candidates(right_sample);
    if let Some(left) = left_candidates {
        if let Some(right) = right_candidates {
            left.iter()
                .any(|left_timestamp| right.contains(left_timestamp))
        } else {
            false
        }
    } else if right_candidates.is_none() {
        left_sample.as_array().and_then(|sample| sample.first())
            == right_sample.as_array().and_then(|sample| sample.first())
    } else {
        false
    }
}

/// A comparison between two vectors, and how their series pair up.
#[derive(Clone, Copy, Debug)]
pub(crate) struct VectorComparison<'a> {
    pub(crate) comparison: MetricComparison,
    pub(crate) matching: Option<&'a MetricVectorMatching>,
}

pub(crate) fn apply_metric_binary_comparison_to_loki_result(
    left: &mut Value,
    right: &Value,
    vector_comparison: VectorComparison<'_>,
) {
    let VectorComparison {
        comparison,
        matching,
    } = vector_comparison;
    MetricBinaryOperator {
        matching,
        apply_series: |left_series: &mut Value, right_series: &Value| {
            apply_metric_binary_comparison_to_series(left_series, right_series, comparison)
        },
        apply_series_with_left_operand: |output_series: &mut Value, left_series: &Value| {
            apply_metric_binary_comparison_to_series_with_left_operand(
                output_series,
                left_series,
                comparison,
            )
        },
    }
    .apply_to_loki_result(left, right);
}

pub(crate) fn apply_metric_binary_comparison_to_sample_operands(
    output_sample: &mut Value,
    operands: SampleOperands<'_>,
    comparison: MetricComparison,
) -> bool {
    let Some((output_values, scalar_operands)) =
        output_sample_scalar_operands(output_sample, operands)
    else {
        return false;
    };
    let matches = metric_scalar_comparison_matches(scalar_operands, comparison.op);
    match comparison.result {
        ComparisonResult::Bool => {
            if let Some(value) = output_values.get_mut(1) {
                *value = json!(if matches { "1" } else { "0" });
            }
            true
        }
        ComparisonResult::Filter => {
            if matches
                && let Some(left) = operands.left.get(1)
                && let Some(output) = output_values.get_mut(1)
            {
                *output = left.clone();
            }
            matches
        }
    }
}

// The series and result walks every vector-to-vector binary operator shares.
// Each operator supplies only what it does to one pair of samples.


/// The left and right sample of one binary operation.
#[derive(Clone, Copy)]
pub(crate) struct SampleOperands<'a> {
    pub(crate) left: &'a Value,
    pub(crate) right: &'a Value,
}

/// The values of the operands, or `None` when either is malformed or their
/// timestamps differ.
pub(crate) fn metric_binary_operand_values(
    operands: SampleOperands<'_>,
) -> Option<MetricOperandValues> {
    let SampleOperands {
        left: left_sample,
        right: right_sample,
    } = operands;
    let left_values = left_sample.as_array()?;
    let right_values = right_sample.as_array()?;
    if !metric_binary_sample_timestamps_match(left_sample, right_sample) {
        return None;
    }
    let left_value = left_values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)?;
    let right_value = right_values
        .get(1)
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)?;
    Some(MetricOperandValues {
        left: left_value,
        right: right_value,
    })
}

/// Applies `apply_sample` to each left sample and the right sample it matches,
/// in place, and keeps the left samples it accepts.
///
/// Returns whether the left series keeps any sample.
pub(crate) fn apply_metric_binary_to_series(
    left_series: &mut Value,
    right_series: &Value,
    apply_sample: impl Fn(&mut Value, &Value) -> bool,
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
            if apply_sample(&mut left_values[index], right_sample) {
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
    apply_sample(left_sample, right_sample)
}

/// Applies `apply_sample` to each sample of `output_series`, which starts as a
/// copy of the right series, with the left sample it matches as the left
/// operand. `apply_sample` takes the output sample, the left sample, and the
/// right sample.
///
/// Returns whether the output series keeps any sample.
pub(crate) fn apply_metric_binary_to_series_with_left_operand(
    output_series: &mut Value,
    left_series: &Value,
    apply_sample: impl Fn(&mut Value, &Value, &Value) -> bool,
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
            if apply_sample(&mut output_values[index], left_sample, &right_sample) {
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
    apply_sample(output_sample, left_sample, &right_sample)
}

/// A vector-to-vector operator, as the series walks it applies.
pub(crate) struct MetricBinaryOperator<'a, Series, SeriesWithLeftOperand> {
    pub(crate) matching: Option<&'a MetricVectorMatching>,
    /// Takes a left series and the right series it matches.
    pub(crate) apply_series: Series,
    /// Serves a `group_right` operator: takes a copy of a right series and the
    /// left series that matches it.
    pub(crate) apply_series_with_left_operand: SeriesWithLeftOperand,
}

impl<Series, SeriesWithLeftOperand> MetricBinaryOperator<'_, Series, SeriesWithLeftOperand>
where
    Series: Fn(&mut Value, &Value) -> bool,
    SeriesWithLeftOperand: Fn(&mut Value, &Value) -> bool,
{
    /// Applies the operator to two Loki results, in place in `left`.
    pub(crate) fn apply_to_loki_result(&self, left: &mut Value, right: &Value) {
        let matching = self.matching;
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
            self.apply_group_right(left_results, right_results, group_labels);
            return;
        }

        retain_matching_metric_result_series(
            left_results,
            right_results,
            matching,
            |left_series, right_series| {
                let Some(right_series) = right_series else {
                    return false;
                };
                if (self.apply_series)(left_series, right_series) {
                    if let Some(MetricVectorGroupModifier::Left(group_labels)) =
                        metric_vector_group_modifier(matching)
                    {
                        include_metric_group_labels(left_series, right_series, group_labels);
                    }
                    true
                } else {
                    false
                }
            },
        );
    }

    /// Replaces `left_results` with one output series per right series that a
    /// left series matches, for a `group_right` operator.
    fn apply_group_right(
        &self,
        left_results: &mut Vec<Value>,
        right_results: &[Value],
        group_labels: &[String],
    ) {
        let matching = self.matching;
        let original_left = std::mem::take(left_results);
        for right_series in right_results {
            let Some(right_labels) = metric_series_labels(right_series) else {
                continue;
            };
            let right_key = metric_vector_matching_key(&right_labels, matching);
            let Some(left_series) = original_left.iter().find(|series| {
                metric_series_labels(series).is_some_and(|labels| {
                    metric_vector_matching_key(&labels, matching) == right_key
                })
            }) else {
                continue;
            };
            let mut output_series = right_series.clone();
            if (self.apply_series_with_left_operand)(&mut output_series, left_series) {
                include_metric_group_labels(&mut output_series, left_series, group_labels);
                left_results.push(output_series);
            }
        }
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
