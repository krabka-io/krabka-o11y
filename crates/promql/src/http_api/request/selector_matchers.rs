use super::{LabelMatcher, PromqlError, parse_promql, Expr, label_matcher_sets};

pub(crate) fn selector_matchers(selector: &str) -> Result<Vec<Vec<LabelMatcher>>, PromqlError> {
    match parse_promql(selector)? {
        Expr::VectorSelector(selector) => Ok(label_matcher_sets(&selector)),
        Expr::MatrixSelector(selector) => Ok(label_matcher_sets(&selector.vs)),
        other => Err(PromqlError::Plan(format!(
            "metadata matcher must be a vector selector, got {other}"
        ))),
    }
}
