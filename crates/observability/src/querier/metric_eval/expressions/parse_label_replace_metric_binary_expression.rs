use super::{
    LabelReplaceMetricBinaryExpression, parse_label_replace_expression,
    parse_leading_bool_modifier, parse_leading_metric_vector_matching_modifier,
    parse_metric_arithmetic_operator, parse_metric_comparison_operator, parse_metric_set_operator,
    split_top_level_arithmetic_query, split_top_level_comparison_query, split_top_level_set_query,
};

pub(crate) fn parse_label_replace_metric_binary_expression(
    query: &str,
) -> Option<LabelReplaceMetricBinaryExpression> {
    if let Some(split) = split_top_level_arithmetic_query(query) {
        let parsed_matching = parse_leading_metric_vector_matching_modifier(split.right, true)?;
        let matching = parsed_matching.matching;
        let right = parsed_matching.remaining;
        let right = right.trim();
        let left = split.left.trim();
        if parse_label_replace_expression(left).is_some()
            || parse_label_replace_expression(right).is_some()
        {
            return Some(LabelReplaceMetricBinaryExpression::Arithmetic {
                left: left.to_string(),
                op: parse_metric_arithmetic_operator(split.operator)?,
                matching,
                right: right.to_string(),
            });
        }
    }

    if let Some(split) = split_top_level_comparison_query(query) {
        let bool_modifier = parse_leading_bool_modifier(split.right);
        let parsed_matching =
            parse_leading_metric_vector_matching_modifier(bool_modifier.remaining, true)?;
        let matching = parsed_matching.matching;
        let right = parsed_matching.remaining;
        let right = right.trim();
        let left = split.left.trim();
        if parse_label_replace_expression(left).is_some()
            || parse_label_replace_expression(right).is_some()
        {
            return Some(LabelReplaceMetricBinaryExpression::Comparison {
                left: left.to_string(),
                op: parse_metric_comparison_operator(split.operator)?,
                bool_modifier: bool_modifier.bool_modifier,
                matching,
                right: right.to_string(),
            });
        }
    }

    if let Some(split) = split_top_level_set_query(query) {
        let parsed_matching = parse_leading_metric_vector_matching_modifier(split.right, false)?;
        let matching = parsed_matching.matching;
        let right = parsed_matching.remaining;
        let right = right.trim();
        let left = split.left.trim();
        if parse_label_replace_expression(left).is_some()
            || parse_label_replace_expression(right).is_some()
        {
            return Some(LabelReplaceMetricBinaryExpression::Set {
                left: left.to_string(),
                op: parse_metric_set_operator(split.operator)?,
                matching,
                right: right.to_string(),
            });
        }
    }

    None
}
