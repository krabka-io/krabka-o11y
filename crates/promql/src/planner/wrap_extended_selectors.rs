use super::*;

pub(crate) fn wrap_extended_selectors(expr: Expr, modifier: ExtendedSelectorModifier) -> Expr {
    match expr {
        Expr::MatrixSelector(_) | Expr::VectorSelector(_) => Expr::Extension(Extension {
            expr: Arc::new(ExtendedSelectorExpr {
                modifier,
                children: vec![expr],
            }),
        }),
        Expr::Call(mut call) => {
            call.args.args = call
                .args
                .args
                .into_iter()
                .map(|arg| Box::new(wrap_extended_selectors(*arg, modifier)))
                .collect();
            Expr::Call(call)
        }
        Expr::Aggregate(mut aggregate) => {
            aggregate.expr = Box::new(wrap_extended_selectors(*aggregate.expr, modifier));
            Expr::Aggregate(aggregate)
        }
        Expr::Unary(mut unary) => {
            unary.expr = Box::new(wrap_extended_selectors(*unary.expr, modifier));
            Expr::Unary(unary)
        }
        Expr::Binary(mut binary) => {
            binary.lhs = Box::new(wrap_extended_selectors(*binary.lhs, modifier));
            binary.rhs = Box::new(wrap_extended_selectors(*binary.rhs, modifier));
            Expr::Binary(binary)
        }
        Expr::Paren(mut paren) => {
            paren.expr = Box::new(wrap_extended_selectors(*paren.expr, modifier));
            Expr::Paren(paren)
        }
        Expr::Subquery(mut subquery) => {
            subquery.expr = Box::new(wrap_extended_selectors(*subquery.expr, modifier));
            Expr::Subquery(subquery)
        }
        other => other,
    }
}
