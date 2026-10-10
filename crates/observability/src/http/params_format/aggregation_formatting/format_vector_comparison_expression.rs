use super::{
    ComparisonResult, VectorComparisonText, format_vector_comparison_text,
    parse_formatted_vector_function, parse_vector_binary_modifiers,
    parse_vector_comparison_operator,
};

pub(crate) fn format_vector_comparison_expression(query: &str) -> Option<String> {
    let (left, position) = parse_formatted_vector_function(query, 0)?;
    let (operator, mut right_position) = parse_vector_comparison_operator(query, position)?;
    let bool_modifier = query[right_position..].starts_with("bool");
    if bool_modifier {
        right_position += "bool".len();
    }
    let modifiers = if let Some((modifiers, next_position)) =
        parse_vector_binary_modifiers(query, right_position)
    {
        right_position = next_position;
        Some(modifiers)
    } else {
        None
    };
    let (right, end) = parse_formatted_vector_function(query, right_position)?;
    if end != query.len() {
        return None;
    }
    Some(format_vector_comparison_text(VectorComparisonText {
        left: &left,
        operator,
        comparison_result: ComparisonResult::from_bool_modifier(bool_modifier),
        modifiers,
        right: &right,
    }))
}
