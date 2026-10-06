use super::{
    Expr, T_AVG, T_COUNT, T_SUM, TokenType, expr_contains_aggregate,
    expr_supports_frontend_sharding,
};
use crate::format_promql_expr;

pub(crate) fn avg_partial_queries(expr: &Expr) -> Option<(String, String)> {
    match expr {
        Expr::Aggregate(aggregate)
            if aggregate.op.id() == T_AVG
                && aggregate.param.is_none()
                && !expr_contains_aggregate(&aggregate.expr)
                && expr_supports_frontend_sharding(&aggregate.expr) =>
        {
            let mut sum_aggregate = aggregate.clone();
            sum_aggregate.op = TokenType::new(T_SUM);
            let mut count_aggregate = aggregate.clone();
            count_aggregate.op = TokenType::new(T_COUNT);
            Some((
                format_promql_expr(&Expr::Aggregate(sum_aggregate)),
                format_promql_expr(&Expr::Aggregate(count_aggregate)),
            ))
        }
        Expr::Paren(paren) => avg_partial_queries(&paren.expr),
        _ => None,
    }
}
