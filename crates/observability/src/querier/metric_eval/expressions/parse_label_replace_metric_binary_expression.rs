use super::{
    LabelReplaceMetricBinaryExpression, parse_label_replace_expression,
    parse_leading_metric_vector_matching_modifier, parse_metric_arithmetic_operator,
    parse_metric_comparison_operator, parse_metric_set_operator, split_top_level_arithmetic_query,
    split_top_level_comparison_query, split_top_level_set_query,
};

pub(crate) fn parse_label_replace_metric_binary_expression(
    query: &str,
) -> Option<LabelReplaceMetricBinaryExpression> {
    if let Some((left, operator, right)) = split_top_level_arithmetic_query(query) {
        let (matching, right) = parse_leading_metric_vector_matching_modifier(right, true)?;
        let right = right.trim();
        let left = left.trim();
        if parse_label_replace_expression(left).is_some()
            || parse_label_replace_expression(right).is_some()
        {
            return Some(LabelReplaceMetricBinaryExpression::Arithmetic {
                left: left.to_string(),
                op: parse_metric_arithmetic_operator(operator)?,
                matching,
                right: right.to_string(),
            });
        }
    }

    if let Some((left, operator, right)) = split_top_level_comparison_query(query) {
        let right = right.trim_start();
        let (bool_modifier, right) = if let Some(rest) = right.strip_prefix("bool") {
            (true, rest.trim_start())
        } else {
            (false, right)
        };
        let (matching, right) = parse_leading_metric_vector_matching_modifier(right, true)?;
        let right = right.trim();
        let left = left.trim();
        if parse_label_replace_expression(left).is_some()
            || parse_label_replace_expression(right).is_some()
        {
            return Some(LabelReplaceMetricBinaryExpression::Comparison {
                left: left.to_string(),
                op: parse_metric_comparison_operator(operator)?,
                bool_modifier,
                matching,
                right: right.to_string(),
            });
        }
    }

    if let Some((left, operator, right)) = split_top_level_set_query(query) {
        let (matching, right) = parse_leading_metric_vector_matching_modifier(right, false)?;
        let right = right.trim();
        let left = left.trim();
        if parse_label_replace_expression(left).is_some()
            || parse_label_replace_expression(right).is_some()
        {
            return Some(LabelReplaceMetricBinaryExpression::Set {
                left: left.to_string(),
                op: parse_metric_set_operator(operator)?,
                matching,
                right: right.to_string(),
            });
        }
    }

    None
}
