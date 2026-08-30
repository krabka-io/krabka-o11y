use super::MetricBinarySetOp;

pub(crate) fn format_metric_binary_set_operator(op: MetricBinarySetOp) -> &'static str {
    match op {
        MetricBinarySetOp::And => "and",
        MetricBinarySetOp::Or => "or",
        MetricBinarySetOp::Unless => "unless",
    }
}
