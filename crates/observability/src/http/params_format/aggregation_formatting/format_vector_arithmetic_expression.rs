use super::{
    parse_formatted_vector_function, parse_vector_arithmetic_operator,
    parse_vector_binary_modifiers,
};

pub(crate) fn format_vector_arithmetic_expression(query: &str) -> Option<String> {
    let (left, position) = parse_formatted_vector_function(query, 0)?;
    let (operator, mut right_position) = parse_vector_arithmetic_operator(query, position)?;
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
        Some(match modifiers {
            Some(modifiers) => format!(
                "({left} {operator} {}{}{right})",
                modifiers.text, modifiers.right_separator
            ),
            None => format!("({left} {operator} {right})"),
        })
    } else {
        None
    }
}
