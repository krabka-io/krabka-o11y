use super::*;

pub(crate) fn apply_histogram_float_binary(
    histogram: &NativeHistogram,
    scalar: f64,
    op: BinaryOp,
    scalar_side: ScalarSide,
) -> Option<SampleValue> {
    let factor = match (op, scalar_side) {
        (BinaryOp::Mul, ScalarSide::Left | ScalarSide::Right) => scalar,
        (BinaryOp::Div, ScalarSide::Right) => 1.0 / scalar,
        _ => return None,
    };
    Some(SampleValue::Histogram(scaled_native_histogram(
        histogram, factor,
    )))
}
