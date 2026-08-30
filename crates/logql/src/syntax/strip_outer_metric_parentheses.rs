use super::outer_metric_parentheses_inner;

pub(crate) fn strip_outer_metric_parentheses(input: &str) -> &str {
    let mut trimmed = input.trim();
    while let Some(inner) = outer_metric_parentheses_inner(trimmed) {
        if inner.len() >= trimmed.len() {
            break;
        }
        trimmed = inner.trim();
    }
    trimmed
}
