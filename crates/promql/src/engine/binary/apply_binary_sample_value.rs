use super::*;

pub(crate) fn apply_binary_sample_value(
    left: &InstantSample,
    right: &InstantSample,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
) -> Result<Option<SampleValue>> {
    match (&left.value, &right.value) {
        (SampleValue::Float(left), SampleValue::Float(right)) => Ok(op
            .apply_scalar(*left, *right, modifier)
            .map(SampleValue::Float)),
        (SampleValue::Histogram(left), SampleValue::Histogram(right)) => {
            apply_histogram_histogram_binary(left, right, op, modifier)
        }
        (SampleValue::Float(left), SampleValue::Histogram(right)) => {
            if op.is_comparison() {
                emit_info(incompatible_types_in_binop_info(
                    "float",
                    op.symbol(),
                    "histogram",
                ));
                return Ok(None);
            }
            Ok(apply_histogram_float_binary(
                right,
                *left,
                op,
                ScalarSide::Left,
            ))
        }
        (SampleValue::Histogram(left), SampleValue::Float(right)) => {
            if op.is_comparison() {
                emit_info(incompatible_types_in_binop_info(
                    "histogram",
                    op.symbol(),
                    "float",
                ));
                return Ok(None);
            }
            Ok(apply_histogram_float_binary(
                left,
                *right,
                op,
                ScalarSide::Right,
            ))
        }
    }
}
