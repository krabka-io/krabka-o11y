use super::{Expr, T_COUNT, T_GROUP, T_MAX, T_MIN, T_SUM};

pub(crate) fn expr_supports_frontend_sharding(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate(aggregate) => {
            matches!(aggregate.op.id(), T_SUM | T_COUNT | T_GROUP | T_MIN | T_MAX)
                && aggregate
                    .param
                    .as_ref()
                    .is_none_or(|param| expr_supports_frontend_sharding(param))
                && expr_supports_frontend_sharding(&aggregate.expr)
        }
        Expr::Unary(unary) => expr_supports_frontend_sharding(&unary.expr),
        Expr::Binary(binary) => {
            expr_supports_frontend_sharding(&binary.lhs)
                && expr_supports_frontend_sharding(&binary.rhs)
        }
        Expr::Paren(paren) => expr_supports_frontend_sharding(&paren.expr),
        Expr::Subquery(subquery) => expr_supports_frontend_sharding(&subquery.expr),
        Expr::Call(call) => call
            .args
            .args
            .iter()
            .all(|arg| expr_supports_frontend_sharding(arg)),
        Expr::VectorSelector(_)
        | Expr::MatrixSelector(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::Extension(_) => true,
    }
}
