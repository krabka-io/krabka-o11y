use super::*;

/// Negates an already-evaluated instant query result.
///
/// This function mirrors the `PromQL` unary `-` operator. A scalar flips sign.
/// An instant vector flips each sample and drops `__name__`: floats flip by
/// negation, and native histograms flip through
/// `scaled_native_histogram(_, -1.0)`. A range-matrix or string input is a hard
/// error. Both the interpreter and the operator path route through this
/// function, so they cannot diverge.
pub(crate) fn negate_query_result(operand: QueryResult) -> Result<QueryResult> {
    match operand {
        QueryResult::Scalar { ts_ms, value } => Ok(QueryResult::Scalar {
            ts_ms,
            value: -value,
        }),
        QueryResult::InstantVector(samples) => Ok(QueryResult::InstantVector(
            samples
                .into_iter()
                .map(|mut sample| {
                    sample.value = match sample.value {
                        SampleValue::Float(value) => SampleValue::Float(-value),
                        SampleValue::Histogram(histogram) => {
                            SampleValue::Histogram(scaled_native_histogram(&histogram, -1.0))
                        }
                    };
                    sample.labels = labels_without_metric_name(&sample.labels);
                    sample
                })
                .collect(),
        )),
        QueryResult::RangeMatrix(_) => Err(PromqlError::Plan(
            "unary expression requires scalar or instant-vector input".to_string(),
        )),
        QueryResult::Str { .. } => Err(PromqlError::Plan(
            "unary expression does not support string input".to_string(),
        )),
    }
}
