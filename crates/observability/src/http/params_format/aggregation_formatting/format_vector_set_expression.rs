use super::{format_vector_binary_operands, parse_formatted_vector_function};

pub(crate) fn format_vector_set_expression(query: &str) -> Option<String> {
    let left = parse_formatted_vector_function(query, 0)?;
    for operator in ["unless", "and", "or"] {
        if let Some(rest) = query[left.position..].strip_prefix(operator) {
            if let Some(formatted) = format_vector_binary_operands(
                query,
                &left.value,
                operator,
                query.len() - rest.len(),
            ) {
                return Some(formatted);
            }
        }
    }
    None
}
