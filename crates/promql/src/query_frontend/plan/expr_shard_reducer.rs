use super::*;

pub(crate) fn expr_shard_reducer(expr: &Expr) -> QueryShardReducer {
    match expr {
        Expr::Aggregate(aggregate) => match aggregate.op.id() {
            T_SUM | T_COUNT => QueryShardReducer::Sum,
            T_MIN => QueryShardReducer::Min,
            T_MAX => QueryShardReducer::Max,
            _ => QueryShardReducer::First,
        },
        Expr::Paren(paren) => expr_shard_reducer(&paren.expr),
        _ => QueryShardReducer::First,
    }
}
