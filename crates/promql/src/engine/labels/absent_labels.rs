use super::{Expr, Result, Labels, absent_labels_from_selector};

pub(crate) fn absent_labels(expr: &Expr) -> Result<Labels> {
    match expr {
        Expr::VectorSelector(selector) => Ok(absent_labels_from_selector(selector)),
        Expr::MatrixSelector(selector) => Ok(absent_labels_from_selector(&selector.vs)),
        Expr::Paren(paren) => absent_labels(&paren.expr),
        _ => Ok(Labels::new()),
    }
}
