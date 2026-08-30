use super::*;

pub(crate) fn expr_contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::Aggregate(_) => true,
        Expr::Unary(unary) => expr_contains_aggregate(&unary.expr),
        Expr::Binary(binary) => {
            expr_contains_aggregate(&binary.lhs) || expr_contains_aggregate(&binary.rhs)
        }
        Expr::Paren(paren) => expr_contains_aggregate(&paren.expr),
        Expr::Subquery(subquery) => expr_contains_aggregate(&subquery.expr),
        Expr::Call(call) => call
            .args
            .args
            .iter()
            .any(|arg| expr_contains_aggregate(arg)),
        Expr::VectorSelector(_)
        | Expr::MatrixSelector(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::Extension(_) => false,
    }
}
