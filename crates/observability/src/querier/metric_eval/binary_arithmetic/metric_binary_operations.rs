//! The series and result walks every vector-to-vector binary operator shares.
//! Each operator supplies only what it does to one pair of samples.

use super::{
    MetricVectorGroupModifier, MetricVectorMatching, Value, include_metric_group_labels,
    matching_metric_binary_sample, metric_binary_sample_timestamps_match, metric_series_labels,
    metric_vector_group_modifier, metric_vector_matching_key, parse_metric_sample_value,
};
use crate::MetricValue;

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
) -> Option<(MetricValue, MetricValue)> {
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
    Some((left_value, right_value))
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

            if (self.apply_series)(&mut left_results[index], right_series) {
                if let Some(MetricVectorGroupModifier::Left(group_labels)) =
                    metric_vector_group_modifier(matching)
                {
                    include_metric_group_labels(
                        &mut left_results[index],
                        right_series,
                        group_labels,
                    );
                }
                index += 1;
            } else {
                left_results.remove(index);
            }
        }
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
