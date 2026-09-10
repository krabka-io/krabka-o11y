use std::cmp::Ordering;

/// Whether two floats are equal to within a relative `tolerance`.
///
/// This is Prometheus' `almost.Equal`. Values at or below the smallest normal
/// double compare against an absolute band instead, because a relative one
/// stops meaning anything there.
pub(crate) fn almost_equal(left: f64, right: f64, tolerance: f64) -> bool {
    // `partial_cmp` asks the same question as `==` -- `+0.0` and `-0.0` are one
    // value, two NaNs are not -- without spelling a float comparison Clippy
    // then has to be told to allow.
    if left.partial_cmp(&right) == Some(Ordering::Equal) {
        return true;
    }
    let sum = left.abs() + right.abs();
    let difference = (left - right).abs();
    if matches!(left.classify(), std::num::FpCategory::Zero)
        || matches!(right.classify(), std::num::FpCategory::Zero)
        || sum < f64::MIN_POSITIVE
    {
        return difference < tolerance * f64::MIN_POSITIVE;
    }
    difference / sum.min(f64::MAX) < tolerance
}
