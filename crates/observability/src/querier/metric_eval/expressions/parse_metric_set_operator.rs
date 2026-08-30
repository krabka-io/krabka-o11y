use super::*;

pub(crate) fn parse_metric_set_operator(operator: &str) -> Option<MetricBinarySetOp> {
    match operator {
        "and" => Some(MetricBinarySetOp::And),
        "or" => Some(MetricBinarySetOp::Or),
        "unless" => Some(MetricBinarySetOp::Unless),
        _ => None,
    }
}
