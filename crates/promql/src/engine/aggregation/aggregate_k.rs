use super::{AggregateExpr, Result, PromqlError, Expr};

pub(crate) fn aggregate_k(aggregate: &AggregateExpr) -> Result<usize> {
    let Some(param) = &aggregate.param else {
        return Err(PromqlError::Plan(format!(
            "{} requires a numeric parameter",
            aggregate.op
        )));
    };
    let Expr::NumberLiteral(number) = param.as_ref() else {
        return Err(PromqlError::Plan(format!(
            "{} parameter must be numeric",
            aggregate.op
        )));
    };
    if !number.val.is_finite() || number.val < 0.0 || number.val.fract() != 0.0 {
        return Err(PromqlError::Plan(format!(
            "{} parameter must be a non-negative integer",
            aggregate.op
        )));
    }
    number
        .val
        .to_string()
        .parse::<usize>()
        .map_err(|_| PromqlError::Plan(format!("{} parameter is too large", aggregate.op)))
}
