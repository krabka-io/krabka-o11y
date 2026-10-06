use super::{Expr, Labels, Result, absent_labels_from_selector};

pub(crate) fn absent_labels(expr: &Expr) -> Result<Labels> {
    match expr {
        Expr::VectorSelector(selector) => Ok(absent_labels_from_selector(selector)),
        Expr::MatrixSelector(selector) => Ok(absent_labels_from_selector(&selector.vs)),
        Expr::Extension(extension) => {
            if let Some(selector) = extension
                .expr
                .as_any()
                .downcast_ref::<crate::planner::byte_selector_expr::ByteSelectorExpr>(
            ) {
                Ok(match selector.matcher_sets.as_slice() {
                    [matchers] => super::absent_labels_from_matchers(matchers),
                    _ => Labels::new(),
                })
            } else {
                Ok(Labels::new())
            }
        }
        Expr::Paren(paren) => absent_labels(&paren.expr),
        _ => Ok(Labels::new()),
    }
}
