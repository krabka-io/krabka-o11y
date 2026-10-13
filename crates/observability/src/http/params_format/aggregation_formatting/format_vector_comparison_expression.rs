use super::{
    ComparisonResult, VectorComparisonText, format_vector_comparison_text,
    parse_formatted_vector_function, parse_vector_binary_right, parse_vector_comparison_operator,
};

pub(crate) fn format_vector_comparison_expression(query: &str) -> Option<String> {
    let left = parse_formatted_vector_function(query, 0)?;
    let operator = parse_vector_comparison_operator(query, left.position)?;
    let mut right_position = operator.position;
    let bool_modifier = query[right_position..].starts_with("bool");
    if bool_modifier {
        right_position += "bool".len();
    }
    let right = parse_vector_binary_right(query, right_position)?;
    Some(format_vector_comparison_text(VectorComparisonText {
        left: &left.value,
        operator: operator.value,
        comparison_result: ComparisonResult::from_bool_modifier(bool_modifier),
        modifiers: right.modifiers,
        right: &right.right,
    }))
}
