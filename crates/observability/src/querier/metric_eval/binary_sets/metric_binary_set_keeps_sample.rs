use super::MetricBinarySetOp;

pub(crate) fn metric_binary_set_keeps_sample(op: MetricBinarySetOp, matched: bool) -> bool {
    match op {
        MetricBinarySetOp::And => matched,
        MetricBinarySetOp::Or => true,
        MetricBinarySetOp::Unless => !matched,
    }
}
