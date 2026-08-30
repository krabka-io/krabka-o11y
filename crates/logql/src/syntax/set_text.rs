use super::*;

pub(crate) fn set_text(op: MetricBinarySetOp) -> &'static str {
    match op {
        MetricBinarySetOp::And => "and",
        MetricBinarySetOp::Or => "or",
        MetricBinarySetOp::Unless => "unless",
    }
}
