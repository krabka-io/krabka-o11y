use super::{AggregateExpr, Expr, PromqlError, Result};

pub(crate) fn aggregate_quantile(aggregate: &AggregateExpr) -> Result<f64> {
    let Some(param) = &aggregate.param else {
        return Err(PromqlError::Plan(
            "quantile requires a numeric parameter".to_string(),
        ));
    };
    let Expr::NumberLiteral(number) = param.as_ref() else {
        return Err(PromqlError::Plan(
            "quantile parameter must be numeric".to_string(),
        ));
    };
    // An out-of-range / NaN phi is NOT an error here: Prometheus returns signed
    // `+/-Inf` / `NaN` plus an `InvalidQuantileWarning` (emitted by
    // `apply_quantile_aggregate`), exactly like the `histogram_quantile` family.
    Ok(number.val)
}
