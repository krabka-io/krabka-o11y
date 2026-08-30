use super::*;

pub(crate) fn inject_shard_into_expr(expr: &mut Expr, shard: QueryShard) {
    match expr {
        Expr::Aggregate(aggregate) => {
            if let Some(param) = aggregate.param.as_mut() {
                inject_shard_into_expr(param, shard);
            }
            inject_shard_into_expr(&mut aggregate.expr, shard);
        }
        Expr::Unary(unary) => inject_shard_into_expr(&mut unary.expr, shard),
        Expr::Binary(binary) => {
            inject_shard_into_expr(&mut binary.lhs, shard);
            inject_shard_into_expr(&mut binary.rhs, shard);
        }
        Expr::Paren(paren) => inject_shard_into_expr(&mut paren.expr, shard),
        Expr::Subquery(subquery) => inject_shard_into_expr(&mut subquery.expr, shard),
        Expr::VectorSelector(selector) => inject_shard_into_selector(selector, shard),
        Expr::MatrixSelector(selector) => inject_shard_into_selector(&mut selector.vs, shard),
        Expr::Call(call) => {
            for arg in &mut call.args.args {
                inject_shard_into_expr(arg, shard);
            }
        }
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::Extension(_) => {}
    }
}
