use super::{
    MetricValue, SampleValueSlot, ScalarArithmetic, ScalarOperands, Value, format_metric_value,
    json, metric_scalar_arithmetic_value, sample_value_slot,
};

pub(crate) fn apply_metric_scalar_arithmetic_to_sample(
    sample: &mut Value,
    arithmetic: ScalarArithmetic,
    scalar: MetricValue,
) -> bool {
    let Some(SampleValueSlot { slot, sample_value }) = sample_value_slot(sample) else {
        return false;
    };
    let operands = ScalarOperands {
        sample: sample_value,
        scalar,
        scalar_side: arithmetic.scalar_side,
    };
    let Some(result) = metric_scalar_arithmetic_value(operands, arithmetic.op) else {
        return false;
    };
    *slot = json!(format_metric_value(result));
    true
}
