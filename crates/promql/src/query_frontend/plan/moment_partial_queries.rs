use super::{
    Expr, MomentReduction, T_COUNT, T_STDDEV, T_STDVAR, T_SUM, TokenType, expr_contains_aggregate,
    expr_supports_frontend_sharding, parse_promql,
};
use crate::format_promql_expr;

pub(crate) fn moment_partial_queries(
    expr: &Expr,
) -> Option<(String, String, String, MomentReduction)> {
    match expr {
        Expr::Aggregate(aggregate)
            if matches!(aggregate.op.id(), T_STDDEV | T_STDVAR)
                && aggregate.param.is_none()
                && !expr_contains_aggregate(&aggregate.expr)
                && expr_supports_frontend_sharding(&aggregate.expr) =>
        {
            let kind = if aggregate.op.id() == T_STDDEV {
                MomentReduction::Stddev
            } else {
                MomentReduction::Stdvar
            };
            let mut sum_aggregate = aggregate.clone();
            sum_aggregate.op = TokenType::new(T_SUM);
            let mut count_aggregate = aggregate.clone();
            count_aggregate.op = TokenType::new(T_COUNT);
            let squared_expr = parse_promql(&format!(
                "({0}) * ({0})",
                format_promql_expr(&aggregate.expr)
            ))
            .ok()?;
            let mut sum_squares_aggregate = aggregate.clone();
            sum_squares_aggregate.op = TokenType::new(T_SUM);
            sum_squares_aggregate.expr = Box::new(squared_expr);
            Some((
                format_promql_expr(&Expr::Aggregate(sum_aggregate)),
                format_promql_expr(&Expr::Aggregate(count_aggregate)),
                format_promql_expr(&Expr::Aggregate(sum_squares_aggregate)),
                kind,
            ))
        }
        Expr::Paren(paren) => moment_partial_queries(&paren.expr),
        _ => None,
    }
}
