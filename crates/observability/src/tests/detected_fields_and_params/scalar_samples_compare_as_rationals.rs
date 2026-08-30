use super::*;

/// `ScalarSample::compare` orders two rationals by cross-multiplication,
/// so the fractions below are chosen not to be decided by their numerators
/// alone: 1/2 against 2/3 orders one way and 2/3 against 1/2 the other,
/// while 1/2 and 2/4 are equal without being identical. A comparison that
/// forgot to cross-multiply would still get many pairs right.
#[test]
pub(crate) fn scalar_samples_compare_as_rationals() {
    use super::super::prelude::{ScalarComparisonOp as Op, ScalarSample};

    let cmp = |n1: i128, d1: u128, op, n2: i128, d2: u128| {
        ScalarSample::new(n1, d1).compare(op, ScalarSample::new(n2, d2))
    };

    // 1/2 < 2/3, which no comparison of numerators alone would decide.
    check!(cmp(1, 2, Op::Less, 2, 3) == Some(true));
    check!(cmp(1, 2, Op::Greater, 2, 3) == Some(false));
    check!(cmp(2, 3, Op::Greater, 1, 2) == Some(true));

    // Equal values with different representations.
    check!(cmp(1, 2, Op::Equal, 2, 4) == Some(true));
    check!(cmp(1, 2, Op::NotEqual, 2, 4) == Some(false));
    check!(
        cmp(1, 2, Op::LessOrEqual, 2, 4) == Some(true),
        "equal satisfies <="
    );
    check!(cmp(1, 2, Op::GreaterOrEqual, 2, 4) == Some(true), "and >=");
    check!(cmp(1, 2, Op::Less, 2, 4) == Some(false), "but not <");
    check!(cmp(1, 2, Op::Greater, 2, 4) == Some(false), "nor >");

    // Each strict operator against its non-strict twin, on a pair that is
    // not equal, so the two cannot be confused for one another.
    check!(cmp(1, 3, Op::Less, 1, 2) == Some(true));
    check!(cmp(1, 3, Op::LessOrEqual, 1, 2) == Some(true));
    check!(cmp(1, 2, Op::Greater, 1, 3) == Some(true));
    check!(cmp(1, 2, Op::GreaterOrEqual, 1, 3) == Some(true));

    // Signs, including a negative on either side of zero.
    check!(cmp(-1, 2, Op::Less, 1, 2) == Some(true));
    check!(
        cmp(-1, 2, Op::Less, -1, 3) == Some(true),
        "-1/2 is below -1/3"
    );
    check!(cmp(-1, 2, Op::Equal, -2, 4) == Some(true));
    check!(
        cmp(0, 1, Op::Equal, 0, 5) == Some(true),
        "zero is zero at any scale"
    );

    // A product that cannot fit answers nothing rather than wrapping.
    check!(cmp(i128::MAX, 1, Op::Greater, 1, 2) == None);
}
