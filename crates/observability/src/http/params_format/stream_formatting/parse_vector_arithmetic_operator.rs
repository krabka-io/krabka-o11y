use crate::http::params_format::ParsedValueAt;

pub(crate) fn parse_vector_arithmetic_operator(
    query: &str,
    position: usize,
) -> Option<ParsedValueAt<&'static str>> {
    for operator in ["+", "-", "*", "/", "%", "^"] {
        if query[position..].starts_with(operator) {
            return Some(ParsedValueAt {
                value: operator,
                position: position + operator.len(),
            });
        }
    }
    None
}
