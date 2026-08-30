use super::*;

pub(crate) fn is_template_comment_action(expression: &str) -> bool {
    expression.starts_with("/*") && expression.ends_with("*/")
}
