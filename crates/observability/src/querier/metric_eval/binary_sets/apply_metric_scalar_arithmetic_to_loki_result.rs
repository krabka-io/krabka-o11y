#[cfg(test)]
use super::MetricScalarArithmetic;
use super::{
    HttpQueryError, ScalarLiteral, Value, apply_metric_scalar_arithmetic_to_series,
    apply_scalar_to_loki_result,
};

#[cfg(test)]
pub(crate) fn apply_metric_scalar_arithmetic_to_loki_result(
    value: &mut Value,
    arithmetic: &MetricScalarArithmetic,
    query: &str,
) -> Result<(), HttpQueryError> {
    apply_scalar_arithmetic_to_loki_result(
        value,
        arithmetic.op,
        &arithmetic.scalar,
        arithmetic.scalar_on_left,
        query,
    )
}

pub(crate) fn apply_scalar_arithmetic_to_loki_result(
    value: &mut Value,
    op: crate::MetricScalarArithmeticOp,
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
        apply_metric_scalar_arithmetic_to_series(series, op, scalar, scalar_on_left)
    });
    Ok(())
}
