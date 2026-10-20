use super::{
    BinModifier, BinaryOp, InstantSample, Result, SampleValue, ScalarSide,
    apply_histogram_float_binary, apply_histogram_histogram_binary, emit_info,
    incompatible_types_in_binop_info,
};

pub(crate) fn apply_binary_sample_value(
    left: &InstantSample,
    right: &InstantSample,
    op: BinaryOp,
    modifier: Option<&BinModifier>,
) -> Result<Option<SampleValue>> {
    match &left.value {
        SampleValue::Float(left_float) => match &right.value {
            SampleValue::Float(right_float) => Ok(op
                .apply_scalar(*left_float, *right_float, modifier)
                .map(SampleValue::Float)),
            SampleValue::Histogram(right_histogram) => {
                if op.is_comparison() {
                    emit_info(incompatible_types_in_binop_info(
                        "float",
                        op.symbol(),
                        "histogram",
                    ));
                    return Ok(None);
                }
                let out = apply_histogram_float_binary(
                    right_histogram,
                    *left_float,
                    op,
                    ScalarSide::Left,
                );
                if out.is_none() {
                    // `+`, `-`, `%`, `^` and `atan2` are undefined between a float
                    // and a histogram: Prometheus drops the pair and says why.
                    emit_info(incompatible_types_in_binop_info(
                        "float",
                        op.symbol(),
                        "histogram",
                    ));
                }
                Ok(out)
            }
        },
        SampleValue::Histogram(left_histogram) => match &right.value {
            SampleValue::Float(right_float) => {
                if op.is_comparison() {
                    emit_info(incompatible_types_in_binop_info(
                        "histogram",
                        op.symbol(),
                        "float",
                    ));
                    return Ok(None);
                }
                let out = apply_histogram_float_binary(
                    left_histogram,
                    *right_float,
                    op,
                    ScalarSide::Right,
                );
                if out.is_none() {
                    emit_info(incompatible_types_in_binop_info(
                        "histogram",
                        op.symbol(),
                        "float",
                    ));
                }
                Ok(out)
            }
            SampleValue::Histogram(right_histogram) => {
                apply_histogram_histogram_binary(left_histogram, right_histogram, op, modifier)
            }
        },
    }
}
