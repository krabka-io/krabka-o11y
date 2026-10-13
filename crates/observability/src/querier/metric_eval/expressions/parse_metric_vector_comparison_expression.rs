use super::{
    MetricVectorComparisonExpression, metric_vector_order, parse_leading_bool_modifier,
    parse_leading_metric_vector_matching_modifier, parse_metric_comparison_operator,
    split_top_level_comparison_query,
};

pub(crate) fn parse_metric_vector_comparison_expression(
    query: &str,
) -> Option<MetricVectorComparisonExpression> {
    let split = split_top_level_comparison_query(query)?;
    let bool_modifier = parse_leading_bool_modifier(split.right);
    let parsed_matching =
        parse_leading_metric_vector_matching_modifier(bool_modifier.remaining, true)?;
    let matching = parsed_matching.matching;
    let right = parsed_matching.remaining;
    let ordered = metric_vector_order(split.left.trim(), right.trim())?;
    Some(MetricVectorComparisonExpression {
        metric_query: ordered.left.to_string(),
        vector_query: ordered.right.to_string(),
        vector_on_left: ordered.scalar_on_left,
        op: parse_metric_comparison_operator(split.operator)?,
        bool_modifier: bool_modifier.bool_modifier,
        matching,
    })
}
