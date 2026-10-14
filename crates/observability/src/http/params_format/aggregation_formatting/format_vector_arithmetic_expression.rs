use super::{
    format_vector_binary_operands, parse_formatted_vector_function,
    parse_vector_arithmetic_operator,
};

pub(crate) fn format_vector_arithmetic_expression(query: &str) -> Option<String> {
    let left = parse_formatted_vector_function(query, 0)?;
    let operator = parse_vector_arithmetic_operator(query, left.position)?;
    format_vector_binary_operands(query, &left.value, operator.value, operator.position)
}
