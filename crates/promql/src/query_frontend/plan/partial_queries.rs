use promql_parser::parser::{
    Expr, LabelModifier,
    token::{T_AVG, T_BOTTOMK, T_COUNT, T_STDDEV, T_STDVAR, T_SUM, T_TOPK, TokenType},
};

use super::{
    MomentReduction, RankReduction, aggregate_k, expr_contains_aggregate,
    expr_supports_frontend_sharding, parse_promql,
};
use crate::format_promql_expr;

pub(crate) struct AvgPartialQueries {
    pub(crate) sum: String,
    pub(crate) count: String,
}

pub(crate) fn avg_partial_queries(expr: &Expr) -> Option<AvgPartialQueries> {
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
            Some(AvgPartialQueries {
                sum: format_promql_expr(&Expr::Aggregate(sum_aggregate)),
                count: format_promql_expr(&Expr::Aggregate(count_aggregate)),
            })
        }
        Expr::Paren(paren) => avg_partial_queries(&paren.expr),
        _ => None,
    }
}

pub(crate) struct MomentPartialQueries {
    pub(crate) sum: String,
    pub(crate) count: String,
    pub(crate) sum_squares: String,
    pub(crate) reduction: MomentReduction,
}

pub(crate) fn moment_partial_queries(expr: &Expr) -> Option<MomentPartialQueries> {
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
            Some(MomentPartialQueries {
                sum: format_promql_expr(&Expr::Aggregate(sum_aggregate)),
                count: format_promql_expr(&Expr::Aggregate(count_aggregate)),
                sum_squares: format_promql_expr(&Expr::Aggregate(sum_squares_aggregate)),
                reduction: kind,
            })
        }
        Expr::Paren(paren) => moment_partial_queries(&paren.expr),
        _ => None,
    }
}

pub(crate) struct RankReductionPlan {
    pub(crate) k: usize,
    pub(crate) kind: RankReduction,
    pub(crate) modifier: Option<LabelModifier>,
}

pub(crate) fn rank_reduction(expr: &Expr) -> Option<RankReductionPlan> {
    match expr {
        Expr::Aggregate(aggregate)
            if matches!(aggregate.op.id(), T_BOTTOMK | T_TOPK)
                && !expr_contains_aggregate(&aggregate.expr)
                && expr_supports_frontend_sharding(&aggregate.expr) =>
        {
            let kind = if aggregate.op.id() == T_TOPK {
                RankReduction::Top
            } else {
                RankReduction::Bottom
            };
            Some(RankReductionPlan {
                k: aggregate_k(aggregate)?,
                kind,
                modifier: aggregate.modifier.clone(),
            })
        }
        Expr::Paren(paren) => rank_reduction(&paren.expr),
        _ => None,
    }
}
