#[cfg(test)]
use super::MetricScalarComparison;
use super::{
    HttpQueryError, ScalarLiteral, Value, apply_scalar_comparison_to_series,
    apply_scalar_to_loki_result,
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
    let scalar = ScalarLiteral {
        text: scalar,
        query,
    }
    .parse()?;
    apply_scalar_to_loki_result(value, scalar, |series, scalar| {
        apply_scalar_comparison_to_series(series, op, bool_modifier, scalar, scalar_on_left)
    });
    Ok(())
}
