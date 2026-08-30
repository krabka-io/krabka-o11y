use super::{
    MetricVectorArithmeticExpression, parse_leading_metric_vector_matching_modifier,
    parse_metric_arithmetic_operator, scalar_vector_query_is_vector,
    split_top_level_arithmetic_query,
};

pub(crate) fn parse_metric_vector_arithmetic_expression(
    query: &str,
) -> Option<MetricVectorArithmeticExpression> {
    let (left, operator, right) = split_top_level_arithmetic_query(query)?;
    let (matching, right) = parse_leading_metric_vector_matching_modifier(right, true)?;
    let left = left.trim();
    let right = right.trim();
    let left_is_vector = scalar_vector_query_is_vector(left);
    let right_is_vector = scalar_vector_query_is_vector(right);
    match (left_is_vector, right_is_vector) {
        (false, true) => Some(MetricVectorArithmeticExpression {
            metric_query: left.to_string(),
            vector_query: right.to_string(),
            vector_on_left: false,
            op: parse_metric_arithmetic_operator(operator)?,
            matching,
        }),
        (true, false) => Some(MetricVectorArithmeticExpression {
            metric_query: right.to_string(),
            vector_query: left.to_string(),
            vector_on_left: true,
            op: parse_metric_arithmetic_operator(operator)?,
            matching,
        }),
        _ => None,
    }
}
