use super::*;

pub(crate) fn parse_vector_comparison_operator(
    query: &str,
    position: usize,
) -> Option<(&'static str, usize)> {
    for operator in [">=", "<=", "==", "!=", ">", "<"] {
        if query[position..].starts_with(operator) {
            return Some((operator, position + operator.len()));
        }
    }
    None
}
