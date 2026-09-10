use super::{BinaryOp, NativeHistogram, SampleValue, ScalarSide, scaled_native_histogram};

/// Applies a binary operator between a native histogram and a float.
///
/// Only `*` (either way round) and `histogram / scalar` are defined; every
/// other operator returns `None` and the caller reports the incompatibility.
///
/// Division by ZERO removes the buckets outright rather than filling them with
/// infinities: `FloatHistogram.Div` does that, because a histogram of nothing
/// but infinite buckets says less than one with none.
pub(crate) fn apply_histogram_float_binary(
    histogram: &NativeHistogram,
    scalar: f64,
    op: BinaryOp,
    scalar_side: ScalarSide,
) -> Option<SampleValue> {
    let divide_by_zero = matches!((op, scalar_side), (BinaryOp::Div, ScalarSide::Right))
        && matches!(scalar.classify(), std::num::FpCategory::Zero);
    let factor = match (op, scalar_side) {
        (BinaryOp::Mul, ScalarSide::Left | ScalarSide::Right) => scalar,
        (BinaryOp::Div, ScalarSide::Right) => 1.0 / scalar,
        _ => return None,
    };
    let mut out = scaled_native_histogram(histogram, factor);
    if divide_by_zero {
        out.positive_spans.clear();
        out.positive_counts.clear();
        out.negative_spans.clear();
        out.negative_counts.clear();
    }
    Some(SampleValue::Histogram(out))
}
