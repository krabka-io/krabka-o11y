use super::*;

/// `MetricValue::subtract` is exact rational arithmetic, so it must not
/// round-trip through a float. The operands are chosen with different
/// denominators, since equal ones let the cross-multiplication cancel out
/// and hide a swapped operand.
#[test]
pub(crate) fn a_metric_subtraction_stays_exact_across_denominators() {
    let value = |numerator, denominator| MetricValue::new(numerator, denominator);

    check!(value(5, 1).subtract(value(3, 1)) == value(2, 1));
    check!(
        value(3, 1).subtract(value(5, 1)) == value(-2, 1),
        "and the other way"
    );

    // 1/2 - 1/3 is exactly 1/6, which no float can hold.
    check!(value(1, 2).subtract(value(1, 3)) == value(1, 6));
    check!(value(1, 3).subtract(value(1, 2)) == value(-1, 6));

    // Subtracting from itself is zero however it is spelled.
    check!(value(7, 3).subtract(value(7, 3)) == MetricValue::zero());
    check!(value(2, 4).subtract(value(1, 2)) == MetricValue::zero());
}
