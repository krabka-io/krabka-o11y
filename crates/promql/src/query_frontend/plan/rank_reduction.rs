use super::{
    Expr, LabelModifier, RankReduction, T_BOTTOMK, T_TOPK, aggregate_k, expr_contains_aggregate,
    expr_supports_frontend_sharding,
};

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
