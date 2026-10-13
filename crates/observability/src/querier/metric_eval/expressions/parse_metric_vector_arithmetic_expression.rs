use super::{
    MetricVectorArithmeticExpression, metric_vector_order,
    parse_leading_metric_vector_matching_modifier, parse_metric_arithmetic_operator,
    split_top_level_arithmetic_query,
};

pub(crate) fn parse_metric_vector_arithmetic_expression(
    query: &str,
) -> Option<MetricVectorArithmeticExpression> {
    let split = split_top_level_arithmetic_query(query)?;
    let parsed_matching = parse_leading_metric_vector_matching_modifier(split.right, true)?;
    let matching = parsed_matching.matching;
    let right = parsed_matching.remaining;
    let ordered = metric_vector_order(split.left.trim(), right.trim())?;
    Some(MetricVectorArithmeticExpression {
        metric_query: ordered.left.to_string(),
        vector_query: ordered.right.to_string(),
        vector_on_left: ordered.scalar_on_left,
        op: parse_metric_arithmetic_operator(split.operator)?,
        matching,
    })
}
