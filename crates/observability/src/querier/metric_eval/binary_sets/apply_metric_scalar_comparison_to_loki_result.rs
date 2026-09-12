#[cfg(test)]
use super::MetricScalarComparison;
use super::{
    HttpQueryError, ParseError, Value, apply_scalar_comparison_to_series, parse_metric_sample_value,
};

#[cfg(test)]
pub(crate) fn apply_metric_scalar_comparison_to_loki_result(
    value: &mut Value,
    comparison: &MetricScalarComparison,
    query: &str,
) -> Result<(), HttpQueryError> {
    apply_scalar_comparison_to_loki_result(
        value,
        comparison.op,
        comparison.bool_modifier,
        &comparison.scalar,
        comparison.scalar_on_left,
        query,
    )
}

pub(crate) fn apply_scalar_comparison_to_loki_result(
    value: &mut Value,
    op: crate::ComparisonOp,
    bool_modifier: bool,
    scalar: &str,
    scalar_on_left: bool,
    query: &str,
) -> Result<(), HttpQueryError> {
    let scalar = parse_metric_sample_value(scalar).ok_or_else(|| HttpQueryError::LokiParse {
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
        if apply_scalar_comparison_to_series(
            &mut results[index],
            op,
            bool_modifier,
            scalar,
            scalar_on_left,
        ) {
            index += 1;
        } else {
            results.remove(index);
        }
    }
    Ok(())
}
