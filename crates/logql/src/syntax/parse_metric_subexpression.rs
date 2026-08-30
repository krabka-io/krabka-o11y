use super::{MetricQuery, ParseError, Parser, strip_outer_metric_parentheses};

pub(crate) fn parse_metric_subexpression(input: &str) -> Result<MetricQuery, ParseError> {
    Parser::new(strip_outer_metric_parentheses(input)).parse_metric()
}
