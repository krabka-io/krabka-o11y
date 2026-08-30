use super::*;

/// `MetricValue::sqrt` returns zero rather than an error for anything with
/// no real root, and it FLOORS to nine decimal places rather than rounding
/// -- so an irrational root is truncated, not nudged up. A NaN reaching a
/// series would poison every aggregation over it.
///
/// The `!is_finite() || <= 0.0` guard cannot be tested from outside, and
/// is kept for what it says rather than what it does: every input it
/// catches also reaches zero through the fall-through, because
/// `i128::from_f64(NaN)` defaults to 0 and `MetricValue::new` maps a zero
/// numerator to zero. Relaxing or removing the guard is an equivalent
/// mutation. It stays because it states the intent -- no real root means
/// zero -- where the fall-through only arrives there by accident.
#[test]
pub(crate) fn a_metric_square_root_floors_and_refuses_what_has_no_root() {
    let value = |numerator, denominator| MetricValue::new(numerator, denominator);

    check!(value(4, 1).sqrt() == value(2, 1));
    check!(value(9, 1).sqrt() == value(3, 1));
    check!(value(1, 4).sqrt() == value(1, 2), "a fractional root");

    // sqrt(2) is irrational: floored at nine places, not rounded. The
    // tenth digit is a 3, so flooring and rounding agree here -- and
    // sqrt(3) at 1.732050807... has a 5 next, where they differ.
    check!(value(2, 1).sqrt() == MetricValue::new(1_414_213_562, METRIC_DECIMAL_SCALE));
    check!(value(3, 1).sqrt() == MetricValue::new(1_732_050_807, METRIC_DECIMAL_SCALE));

    // Zero and negatives have no positive root, and both answer zero
    // rather than propagating a NaN into the series.
    check!(value(0, 1).sqrt() == MetricValue::zero());
    check!(value(-4, 1).sqrt() == MetricValue::zero());
    check!(value(-1, 1).sqrt() == MetricValue::zero());
}
