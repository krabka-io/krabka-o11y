use super::{Expr, RankReduction, LabelModifier, T_BOTTOMK, T_TOPK, expr_contains_aggregate, expr_supports_frontend_sharding, aggregate_k};

pub(crate) fn rank_reduction(expr: &Expr) -> Option<(usize, RankReduction, Option<LabelModifier>)> {
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
            Some((aggregate_k(aggregate)?, kind, aggregate.modifier.clone()))
        }
        Expr::Paren(paren) => rank_reduction(&paren.expr),
        _ => None,
    }
}
