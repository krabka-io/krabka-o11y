use super::{
    LabelReplaceMetricBinaryExpression, parse_label_replace_expression,
    parse_leading_bool_modifier, parse_leading_metric_vector_matching_modifier,
    parse_metric_arithmetic_operator, parse_metric_comparison_operator, parse_metric_set_operator,
    split_top_level_arithmetic_query, split_top_level_comparison_query, split_top_level_set_query,
};

struct ParsedLabelReplaceOperands<'a> {
    left: &'a str,
    right: &'a str,
    matching: Option<super::MetricVectorMatching>,
    bool_modifier: bool,
}

fn parse_label_replace_operands<'a>(
    left: &'a str,
    right: &'a str,
    allow_group_modifier: bool,
    has_bool_modifier: bool,
) -> Option<ParsedLabelReplaceOperands<'a>> {
    let bool_modifier = if has_bool_modifier {
        parse_leading_bool_modifier(right)
    } else {
        super::ParsedBoolModifier {
            bool_modifier: false,
            remaining: right,
        }
    };
    let parsed_matching = parse_leading_metric_vector_matching_modifier(
        bool_modifier.remaining,
        allow_group_modifier,
    )?;
    let left = left.trim();
    let right = parsed_matching.remaining.trim();
    (parse_label_replace_expression(left).is_some()
        || parse_label_replace_expression(right).is_some())
    .then_some(ParsedLabelReplaceOperands {
        left,
        right,
        matching: parsed_matching.matching,
        bool_modifier: bool_modifier.bool_modifier,
    })
}

pub(crate) fn parse_label_replace_metric_binary_expression(
    query: &str,
) -> Option<LabelReplaceMetricBinaryExpression> {
    if let Some(split) = split_top_level_arithmetic_query(query) {
        if let Some(operands) = parse_label_replace_operands(split.left, split.right, true, false) {
            return Some(LabelReplaceMetricBinaryExpression::Arithmetic {
                left: operands.left.to_string(),
                op: parse_metric_arithmetic_operator(split.operator)?,
                matching: operands.matching,
                right: operands.right.to_string(),
            });
        }
    }

    if let Some(split) = split_top_level_comparison_query(query) {
        if let Some(operands) = parse_label_replace_operands(split.left, split.right, true, true) {
            return Some(LabelReplaceMetricBinaryExpression::Comparison {
                left: operands.left.to_string(),
                op: parse_metric_comparison_operator(split.operator)?,
                bool_modifier: operands.bool_modifier,
                matching: operands.matching,
                right: operands.right.to_string(),
            });
        }
    }

    if let Some(split) = split_top_level_set_query(query) {
        if let Some(operands) = parse_label_replace_operands(split.left, split.right, false, false)
        {
            return Some(LabelReplaceMetricBinaryExpression::Set {
                left: operands.left.to_string(),
                op: parse_metric_set_operator(split.operator)?,
                matching: operands.matching,
                right: operands.right.to_string(),
            });
        }
    }

    None
}
