use super::*;

struct ComparisonCase {
    ordering: std::cmp::Ordering,
    sample: MetricValue,
    scalar: MetricValue,
    scalar_side: super::super::prelude::ScalarSide,
}

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

    // The ordering of left against right, the sample, scalar, and scalar side.
    let cases = [
        ComparisonCase {
            ordering: Ordering::Less,
            sample: one,
            scalar: two,
            scalar_side: ScalarSide::Right,
        },
        ComparisonCase {
            ordering: Ordering::Greater,
            sample: one,
            scalar: two,
            scalar_side: ScalarSide::Left,
        },
        ComparisonCase {
            ordering: Ordering::Greater,
            sample: two,
            scalar: one,
            scalar_side: ScalarSide::Right,
        },
        ComparisonCase {
            ordering: Ordering::Less,
            sample: two,
            scalar: one,
            scalar_side: ScalarSide::Left,
        },
        ComparisonCase {
            ordering: Ordering::Equal,
            sample: one,
            scalar: one,
            scalar_side: ScalarSide::Right,
        },
        ComparisonCase {
            ordering: Ordering::Equal,
            sample: one,
            scalar: one,
            scalar_side: ScalarSide::Left,
        },
    ];
    for case in cases {
        let want = |op| match op {
            ComparisonOp::Equal => case.ordering == Ordering::Equal,
            ComparisonOp::NotEqual => case.ordering != Ordering::Equal,
            ComparisonOp::Greater => case.ordering == Ordering::Greater,
            ComparisonOp::GreaterEqual => case.ordering != Ordering::Less,
            ComparisonOp::Less => case.ordering == Ordering::Less,
            ComparisonOp::LessEqual => case.ordering != Ordering::Greater,
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
                matches(case.sample, op, case.scalar, case.scalar_side) == want(op),
                "{op:?} at {:?} with the scalar on the {:?}",
                case.ordering,
                case.scalar_side
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
