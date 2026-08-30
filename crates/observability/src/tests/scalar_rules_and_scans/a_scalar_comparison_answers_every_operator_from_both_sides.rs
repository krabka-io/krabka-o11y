use super::*;

/// `metric_scalar_comparison_matches` compares a sample against a scalar,
/// with a flag saying which side the scalar was written on. That flag only
/// matters for the four ordered operators -- `1 > x` and `x > 1` disagree
/// where `1 == x` and `x == 1` do not -- so every operator is checked at
/// all three orderings AND on both sides.
///
/// The two regex operators are always false here: a regex against a number
/// is not a comparison `LogQL` can evaluate, and answering either way would
/// silently filter samples on a predicate nobody wrote.
#[test]
pub(crate) fn a_scalar_comparison_answers_every_operator_from_both_sides() {
    use std::cmp::Ordering;

    use krabka_logql::ComparisonOp;

    let one = MetricValue::new(1, 1);
    let two = MetricValue::new(2, 1);
    let matches = |sample, op, scalar, scalar_on_left| {
        super::super::prelude::metric_scalar_comparison_matches(sample, op, scalar, scalar_on_left)
    };

    // (ordering of left against right, sample, scalar, scalar_on_left)
    let cases = [
        (Ordering::Less, one, two, false),
        (Ordering::Greater, one, two, true),
        (Ordering::Greater, two, one, false),
        (Ordering::Less, two, one, true),
        (Ordering::Equal, one, one, false),
        (Ordering::Equal, one, one, true),
    ];
    for (ordering, sample, scalar, scalar_on_left) in cases {
        let want = |op| match op {
            ComparisonOp::Equal => ordering == Ordering::Equal,
            ComparisonOp::NotEqual => ordering != Ordering::Equal,
            ComparisonOp::Greater => ordering == Ordering::Greater,
            ComparisonOp::GreaterEqual => ordering != Ordering::Less,
            ComparisonOp::Less => ordering == Ordering::Less,
            ComparisonOp::LessEqual => ordering != Ordering::Greater,
            ComparisonOp::RegexEqual | ComparisonOp::RegexNotEqual => false,
        };
        for op in [
            ComparisonOp::Equal,
            ComparisonOp::NotEqual,
            ComparisonOp::Greater,
            ComparisonOp::GreaterEqual,
            ComparisonOp::Less,
            ComparisonOp::LessEqual,
            ComparisonOp::RegexEqual,
            ComparisonOp::RegexNotEqual,
        ] {
            check!(
                matches(sample, op, scalar, scalar_on_left) == want(op),
                "{op:?} at {ordering:?} with scalar_on_left={scalar_on_left}"
            );
        }
    }

    // Spelled out for the case the table exists to protect: the scalar's
    // side changes the answer for an ordered operator and not for equality.
    check!(
        matches(one, ComparisonOp::Less, two, false),
        "x < 1 where x is smaller"
    );
    check!(
        !matches(one, ComparisonOp::Less, two, true),
        "but 1 < x is not"
    );
    check!(matches(one, ComparisonOp::Equal, one, false));
    check!(
        matches(one, ComparisonOp::Equal, one, true),
        "equality is side-blind"
    );
}
