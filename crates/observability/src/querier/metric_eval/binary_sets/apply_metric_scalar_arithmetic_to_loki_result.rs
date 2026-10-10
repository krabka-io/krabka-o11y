use super::{
    HttpQueryError, ScalarArithmetic, ScalarLiteral, Value,
    apply_metric_scalar_arithmetic_to_series, apply_scalar_to_loki_result,
};
#[cfg(test)]
use super::{MetricScalarArithmetic, ScalarSide};

#[cfg(test)]
pub(crate) fn apply_metric_scalar_arithmetic_to_loki_result(
    value: &mut Value,
    arithmetic: &MetricScalarArithmetic,
    query: &str,
) -> Result<(), HttpQueryError> {
    apply_scalar_arithmetic_to_loki_result(
        value,
        ScalarArithmetic {
            op: arithmetic.op,
            scalar_side: ScalarSide::from_scalar_on_left(arithmetic.scalar_on_left),
        },
        ScalarLiteral {
            text: &arithmetic.scalar,
            query,
        },
    )
}

pub(crate) fn apply_scalar_arithmetic_to_loki_result(
    value: &mut Value,
    arithmetic: ScalarArithmetic,
    scalar: ScalarLiteral<'_>,
) -> Result<(), HttpQueryError> {
    let scalar = scalar.parse()?;
    apply_scalar_to_loki_result(value, scalar, |series, scalar| {
        apply_metric_scalar_arithmetic_to_series(series, arithmetic, scalar)
    });
    Ok(())
}
