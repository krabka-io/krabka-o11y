#[cfg(test)]
use super::MetricScalarComparison;
use super::{
    HttpQueryError, ScalarComparison, ScalarLiteral, Value, apply_scalar_comparison_to_series,
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
        ScalarComparison::from(comparison),
        ScalarLiteral {
            text: &comparison.scalar,
            query,
        },
    )
}

pub(crate) fn apply_scalar_comparison_to_loki_result(
    value: &mut Value,
    comparison: ScalarComparison,
    scalar: ScalarLiteral<'_>,
) -> Result<(), HttpQueryError> {
    let scalar = scalar.parse()?;
    apply_scalar_to_loki_result(value, scalar, |series, scalar| {
        apply_scalar_comparison_to_series(series, comparison, scalar)
    });
    Ok(())
}
