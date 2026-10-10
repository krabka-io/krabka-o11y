use super::*;

/// `metric_scalar_comparison_matches` compares a sample against a scalar,
/// given the side the scalar was written on. That side only
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

    use super::super::prelude::{ScalarOperands, ScalarSide};

    let one = MetricValue::new(1, 1);
    let two = MetricValue::new(2, 1);
    let matches = |sample, op, scalar, scalar_side| {
        super::super::prelude::metric_scalar_comparison_matches(
            ScalarOperands {
                sample,
                scalar,
                scalar_side,
            },
            op,
        )
    };

    // (ordering of left against right, sample, scalar, scalar_side)
    let cases = [
        (Ordering::Less, one, two, ScalarSide::Right),
        (Ordering::Greater, one, two, ScalarSide::Left),
        (Ordering::Greater, two, one, ScalarSide::Right),
        (Ordering::Less, two, one, ScalarSide::Left),
        (Ordering::Equal, one, one, ScalarSide::Right),
        (Ordering::Equal, one, one, ScalarSide::Left),
    ];
    for (ordering, sample, scalar, scalar_side) in cases {
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
                matches(sample, op, scalar, scalar_side) == want(op),
                "{op:?} at {ordering:?} with the scalar on the {scalar_side:?}"
            );
        }
    }

    // Spelled out for the case the table exists to protect: the scalar's
    // side changes the answer for an ordered operator and not for equality.
    check!(
        matches(one, ComparisonOp::Less, two, ScalarSide::Right),
        "x < 1 where x is smaller"
    );
    check!(
        !matches(one, ComparisonOp::Less, two, ScalarSide::Left),
        "but 1 < x is not"
    );
    check!(matches(one, ComparisonOp::Equal, one, ScalarSide::Right));
    check!(
        matches(one, ComparisonOp::Equal, one, ScalarSide::Left),
        "equality is side-blind"
    );
}
