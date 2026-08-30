use super::{parse_formatted_vector_function, parse_vector_binary_modifiers};

pub(crate) fn format_vector_set_expression(query: &str) -> Option<String> {
    let (left, position) = parse_formatted_vector_function(query, 0)?;
    for operator in ["unless", "and", "or"] {
        if let Some(rest) = query[position..].strip_prefix(operator) {
            let mut right_position = query.len() - rest.len();
            let modifiers = if let Some((modifiers, next_position)) =
                parse_vector_binary_modifiers(query, right_position)
            {
                right_position = next_position;
                Some(modifiers)
            } else {
                None
            };
            let (right, end) = parse_formatted_vector_function(query, right_position)?;
            if end == query.len() {
                return Some(match modifiers {
                    Some(modifiers) => format!(
                        "({left} {operator} {}{}{right})",
                        modifiers.text, modifiers.right_separator
                    ),
                    None => format!("({left} {operator} {right})"),
                });
            }
        }
    }
    None
}
