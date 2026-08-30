use super::*;

/// `format_metric_value` renders a rational as a decimal, capped at nine
/// places and with trailing zeros trimmed. A whole number gets no decimal
/// point at all, which is a different branch from one whose decimals all
/// trim away -- both are checked, since they produce the same text by
/// different routes.
#[test]
pub(crate) fn a_metric_value_renders_without_trailing_zeros() {
    let render = |numerator, denominator| {
        super::super::prelude::format_metric_value(MetricValue::new(numerator, denominator))
    };

    // Whole numbers take the early return and carry no point.
    check!(render(5, 1) == "5");
    check!(render(0, 1) == "0");
    check!(render(-5, 1) == "-5");
    // A fraction that reduces to a whole number takes the same branch.
    check!(render(10, 5) == "2");

    // Exact decimals keep only the digits they need.
    check!(render(1, 2) == "0.5");
    check!(render(-1, 2) == "-0.5");
    check!(render(1, 4) == "0.25");
    check!(render(3, 2) == "1.5");
    check!(render(-3, 2) == "-1.5");

    // The sign is on the whole part, and survives a zero whole part --
    // "-0.5" rather than "0.5" with the minus lost on the way through
    // `unsigned_abs`.
    check!(render(-1, 4) == "-0.25");

    // A repeating fraction is cut at nine places, not rounded up: a third
    // is nine 3s, and two thirds is nine 6s rather than ...667.
    check!(render(1, 3) == "0.333333333");
    check!(render(2, 3) == "0.666666666");

    // Trailing zeros are trimmed even when the division produces them.
    check!(render(1, 8) == "0.125");
    check!(render(1, 5) == "0.2", "not 0.200000000");

    // The trim only has anything to do when the nine-digit cap lands on a
    // zero: a terminating fraction stops as soon as the remainder does, so
    // it never appends one. 1/11 is 0.090909090... -- nine digits ending
    // in a zero that must come off.
    check!(render(1, 11) == "0.09090909");
}
