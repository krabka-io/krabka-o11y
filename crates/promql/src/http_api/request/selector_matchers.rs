use super::{Expr, LabelMatcher, PromqlError, label_matcher_sets, parse_promql};

pub(crate) fn selector_matchers(selector: &str) -> Result<Vec<Vec<LabelMatcher>>, PromqlError> {
    match parse_promql(selector)? {
        Expr::VectorSelector(selector) => Ok(label_matcher_sets(&selector)),
        Expr::Extension(extension)
            if extension
                .expr
                .as_any()
                .is::<crate::planner::byte_selector_expr::ByteSelectorExpr>() =>
        {
            Ok(extension
                .expr
                .as_any()
                .downcast_ref::<crate::planner::byte_selector_expr::ByteSelectorExpr>()
                .expect("checked type")
                .matcher_sets
                .clone())
        }
        Expr::MatrixSelector(selector) => Ok(label_matcher_sets(&selector.vs)),
        other => Err(PromqlError::Plan(format!(
            "metadata matcher must be a vector selector, got {other}"
        ))),
    }
}
