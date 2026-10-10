use super::{ComparisonResult, MetricVectorMatching, format_metric_vector_matching};

/// The formatted parts of one vector-to-vector binary expression.
pub(crate) struct MetricBinaryExpressionText<'a> {
    pub(crate) left: &'a str,
    pub(crate) operator: &'a str,
    /// [`ComparisonResult::Bool`] writes the `bool` modifier. Arithmetic and
    /// set operators take no modifier, so they use
    /// [`ComparisonResult::Filter`].
    pub(crate) comparison_result: ComparisonResult,
    pub(crate) matching: Option<&'a MetricVectorMatching>,
    pub(crate) right: &'a str,
}

pub(crate) fn format_metric_binary_expression(
    expression: &MetricBinaryExpressionText<'_>,
) -> String {
    let MetricBinaryExpressionText {
        left,
        operator,
        comparison_result,
        matching,
        right,
    } = expression;
    let bool_text = match comparison_result {
        ComparisonResult::Bool => " bool",
        ComparisonResult::Filter => "",
    };
    let Some(matching) = matching else {
        return format!("({left} {operator}{bool_text} {right})");
    };
    let matching = format_metric_vector_matching(matching);
    if matching.has_group {
        return format!(
            "  {left}\n{operator}{bool_text} {}\n  {right}",
            matching.text
        );
    }
    format!("({left} {operator}{bool_text} {}  {right})", matching.text)
}
