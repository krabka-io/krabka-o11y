use super::{AggregateExpr, Expr};

pub(crate) fn aggregate_k(aggregate: &AggregateExpr) -> Option<usize> {
    let param = aggregate.param.as_ref()?;
    let Expr::NumberLiteral(number) = param.as_ref() else {
        return None;
    };
    if !number.val.is_finite() || number.val < 0.0 || number.val.fract() != 0.0 {
        return None;
    }
    number.val.to_string().parse::<usize>().ok()
}
