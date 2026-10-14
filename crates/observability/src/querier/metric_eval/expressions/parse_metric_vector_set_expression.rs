use super::{
    MetricVectorSetExpression, parse_leading_metric_vector_matching_modifier,
    parse_metric_set_operator, scalar_vector_query_is_vector, split_top_level_set_query,
};

pub(crate) fn parse_metric_vector_set_expression(query: &str) -> Option<MetricVectorSetExpression> {
    let split = split_top_level_set_query(query)?;
    let parsed_matching = parse_leading_metric_vector_matching_modifier(split.right, false)?;
    let matching = parsed_matching.matching;
    let right = parsed_matching.remaining;
    let left = split.left.trim();
    let right = right.trim();
    let left_is_vector = scalar_vector_query_is_vector(left);
    let right_is_vector = scalar_vector_query_is_vector(right);
    if !left_is_vector && right_is_vector {
        Some(MetricVectorSetExpression {
            metric_query: left.to_string(),
            vector_query: right.to_string(),
            vector_on_left: false,
            op: parse_metric_set_operator(split.operator)?,
            matching,
        })
    } else if left_is_vector && !right_is_vector {
        Some(MetricVectorSetExpression {
            metric_query: right.to_string(),
            vector_query: left.to_string(),
            vector_on_left: true,
            op: parse_metric_set_operator(split.operator)?,
            matching,
        })
    } else {
        None
    }
}
