use super::{
    MetricVectorComparisonExpression, parse_leading_metric_vector_matching_modifier,
    parse_metric_comparison_operator, scalar_vector_query_is_vector,
    split_top_level_comparison_query,
};

pub(crate) fn parse_metric_vector_comparison_expression(
    query: &str,
) -> Option<MetricVectorComparisonExpression> {
    let (left, operator, right) = split_top_level_comparison_query(query)?;
    let right = right.trim_start();
    let (bool_modifier, right) = if let Some(rest) = right.strip_prefix("bool") {
        (true, rest.trim_start())
    } else {
        (false, right)
    };
    let (matching, right) = parse_leading_metric_vector_matching_modifier(right, true)?;
    let left = left.trim();
    let right = right.trim();
    let left_is_vector = scalar_vector_query_is_vector(left);
    let right_is_vector = scalar_vector_query_is_vector(right);
    match (left_is_vector, right_is_vector) {
        (false, true) => Some(MetricVectorComparisonExpression {
            metric_query: left.to_string(),
            vector_query: right.to_string(),
            vector_on_left: false,
            op: parse_metric_comparison_operator(operator)?,
            bool_modifier,
            matching,
        }),
        (true, false) => Some(MetricVectorComparisonExpression {
            metric_query: right.to_string(),
            vector_query: left.to_string(),
            vector_on_left: true,
            op: parse_metric_comparison_operator(operator)?,
            bool_modifier,
            matching,
        }),
        _ => None,
    }
}
