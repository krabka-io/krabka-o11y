#[cfg(test)]
use crate::{
    HttpQueryError, ParseError, QueryKind, TimeRange, add_loki_query_stats,
    loki_instant_scalar_or_vector_response, loki_range_vector_response, resolved_range_step,
    scalar_vector_expression_result,
};
use crate::{
    MetricBinaryOperator, MetricQuery, MetricScalarArithmeticOp, MetricValue, MetricVectorMatching,
    Ordering, Value, VectorAggregationOp, apply_metric_binary_arithmetic_to_sample,
    apply_metric_binary_arithmetic_to_series_with_left_operand, apply_metric_binary_to_series,
    parse_metric_sample_value, querier::metric_eval::for_each_loki_result_metric,
};

pub(crate) fn loki_vector_sample_value(sample: &Value) -> Option<MetricValue> {
    sample
        .pointer("/value/1")
        .and_then(Value::as_str)
        .and_then(parse_metric_sample_value)
}

pub(crate) fn metric_query_uses_approx_topk(query: &MetricQuery) -> bool {
    query
        .vector_aggregation
        .as_ref()
        .is_some_and(|aggregation| matches!(aggregation.op, VectorAggregationOp::ApproxTopK(_)))
}

pub(crate) fn metric_query_uses_count_values(query: &MetricQuery) -> bool {
    query
        .vector_aggregation
        .as_ref()
        .is_some_and(|aggregation| matches!(aggregation.op, VectorAggregationOp::CountValues(_)))
}

pub(crate) fn apply_metric_binary_arithmetic_to_series(
    left_series: &mut Value,
    right_series: &Value,
    op: MetricScalarArithmeticOp,
) -> bool {
    apply_metric_binary_to_series(left_series, right_series, |left_sample, right_sample| {
        apply_metric_binary_arithmetic_to_sample(left_sample, right_sample, op)
    })
}

pub(crate) fn sort_loki_vector_result(value: &mut Value, order: SampleOrder) {
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
        let left_value = loki_vector_sample_value(left);
        let right_value = loki_vector_sample_value(right);
        let ordering = if let Some(left_value) = left_value {
            if let Some(right_value) = right_value {
                left_value.cmp_value(right_value)
            } else {
                Ordering::Less
            }
        } else if right_value.is_some() {
            Ordering::Greater
        } else {
            Ordering::Equal
        };
        order.orient(ordering)
    });
}

/// A vector-to-vector arithmetic operator and its matching modifiers.
#[derive(Clone, Copy)]
pub(crate) struct VectorArithmetic<'a> {
    pub(crate) op: MetricScalarArithmeticOp,
    pub(crate) matching: Option<&'a MetricVectorMatching>,
}

pub(crate) fn apply_metric_binary_arithmetic_to_loki_result(
    left: &mut Value,
    right: &Value,
    vector_arithmetic: VectorArithmetic<'_>,
) {
    let VectorArithmetic { op, matching } = vector_arithmetic;
    MetricBinaryOperator {
        matching,
        apply_series: |left_series: &mut Value, right_series: &Value| {
            apply_metric_binary_arithmetic_to_series(left_series, right_series, op)
        },
        apply_series_with_left_operand: |output_series: &mut Value, left_series: &Value| {
            apply_metric_binary_arithmetic_to_series_with_left_operand(
                output_series,
                left_series,
                op,
            )
        },
    }
    .apply_to_loki_result(left, right);
}

#[cfg(test)]

pub(crate) fn execute_http_scalar_vector_expression_result(
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

pub(crate) fn retain_metric_binary_on_labels(
    value: &mut Value,
    matching: Option<&MetricVectorMatching>,
) {
    let Some(MetricVectorMatching::On {
        labels,
        group: None,
    }) = matching
    else {
        return;
    };
    for_each_loki_result_metric(value, |metric| {
        metric.retain(|label, _| labels.contains(label));
    });
}

/// The order a result's samples are ranked in, by value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SampleOrder {
    /// Smallest first: `sort`, and the samples `bottomk` keeps.
    Ascending,
    /// Largest first: `sort_desc`, and the samples `topk` keeps.
    Descending,
}

impl SampleOrder {
    /// Orients `ascending`, an ascending comparison, to this order.
    pub(crate) fn orient(self, ascending: Ordering) -> Ordering {
        match self {
            Self::Ascending => ascending,
            Self::Descending => ascending.reverse(),
        }
    }
}
