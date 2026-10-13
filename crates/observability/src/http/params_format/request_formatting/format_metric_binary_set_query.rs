use super::{
    ComparisonResult, MetricBinaryExpressionText, format_metric_binary_expression,
    format_metric_binary_set_operator, format_metric_query, parse_metric_binary_set_query,
    split_top_level_set_query, validate_metric_binary_query_operands,
};

pub(crate) fn format_metric_binary_set_query(query: &str) -> Option<String> {
    validate_metric_binary_query_operands(split_top_level_set_query(query)?)?;
    let set = parse_metric_binary_set_query(query).ok()?;
    let left = format_metric_query(&set.left)?;
    let right = format_metric_query(&set.right)?;
    let operator = format_metric_binary_set_operator(set.op);
    Some(format_metric_binary_expression(
        &MetricBinaryExpressionText {
            left: &left,
            operator,
            comparison_result: ComparisonResult::Filter,
            matching: set.matching.as_ref(),
            right: &right,
        },
    ))
}
