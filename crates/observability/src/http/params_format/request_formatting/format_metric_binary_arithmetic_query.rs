use super::{
    ComparisonResult, MetricBinaryExpressionText, format_metric_binary_expression,
    format_metric_query, format_metric_scalar_arithmetic_operator,
    parse_metric_binary_arithmetic_query, split_top_level_arithmetic_query,
    validate_metric_binary_query_operands,
};

pub(crate) fn format_metric_binary_arithmetic_query(query: &str) -> Option<String> {
    validate_metric_binary_query_operands(split_top_level_arithmetic_query(query)?)?;
    let arithmetic = parse_metric_binary_arithmetic_query(query).ok()?;
    let left = format_metric_query(&arithmetic.left)?;
    let right = format_metric_query(&arithmetic.right)?;
    let operator = format_metric_scalar_arithmetic_operator(arithmetic.op);
    Some(format_metric_binary_expression(
        &MetricBinaryExpressionText {
            left: &left,
            operator,
            comparison_result: ComparisonResult::Filter,
            matching: arithmetic.matching.as_ref(),
            right: &right,
        },
    ))
}
