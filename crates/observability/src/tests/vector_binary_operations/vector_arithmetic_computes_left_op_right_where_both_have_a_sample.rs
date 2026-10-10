use super::*;

/// Vector arithmetic replaces each sample's value with `left op right`,
/// keeping only the timestamps both sides carry. The operator is
/// non-commutative here on purpose: subtraction and division both give a
/// different answer with the operands swapped, which a fixture using only
/// `+` or `*` could never show.
///
/// Like its comparison twin, this removes in place at two sites and the
/// index must not advance on either, so the dropped samples are adjacent.
#[test]
pub(crate) fn vector_arithmetic_computes_left_op_right_where_both_have_a_sample() {
    use krabka_logql::MetricScalarArithmeticOp;

    let left = range_series(&[
        timed_sample(1, "10"),
        timed_sample(4, "20"),
        timed_sample(5, "20"),
    ]);
    // 2, 3 and 6 have no left sample; 2 and 3 are adjacent.
    let right = || {
        range_series(&[
            timed_sample(1, "2"),
            timed_sample(2, "1"),
            timed_sample(3, "1"),
            timed_sample(4, "5"),
            timed_sample(5, "5"),
            timed_sample(6, "1"),
        ])
    };
    let apply = |op| {
        let mut output = right();
        let kept =
            super::super::prelude::apply_metric_binary_arithmetic_to_series_with_left_operand(
                &mut output,
                &left,
                op,
            );
        (kept, range_pairs(&output))
    };

    // 10-2, 20-5, 20-5 -- not 2-10, which is what a swap would give.
    check!(
        apply(MetricScalarArithmeticOp::Subtract)
            == (
                true,
                vec![
                    (1, "8".to_string()),
                    (4, "15".to_string()),
                    (5, "15".to_string()),
                ]
            )
    );
    check!(
        apply(MetricScalarArithmeticOp::Divide)
            == (
                true,
                vec![
                    (1, "5".to_string()),
                    (4, "4".to_string()),
                    (5, "4".to_string()),
                ]
            )
    );
    check!(
        apply(MetricScalarArithmeticOp::Multiply)
            == (
                true,
                vec![
                    (1, "20".to_string()),
                    (4, "100".to_string()),
                    (5, "100".to_string()),
                ]
            )
    );

    // A division with no answer drops its sample rather than emitting one.
    let mut output = range_series(&[timed_sample(1, "0")]);
    check!(
        !super::super::prelude::apply_metric_binary_arithmetic_to_series_with_left_operand(
            &mut output,
            &left,
            MetricScalarArithmeticOp::Divide,
        )
    );

    // The instant shape again, where nothing pre-matches the timestamps.
    let mut output = instant_series(timed_sample(1, "2"));
    check!(
        super::super::prelude::apply_metric_binary_arithmetic_to_series_with_left_operand(
            &mut output,
            &instant_series(timed_sample(1, "10")),
            MetricScalarArithmeticOp::Subtract,
        )
    );
    check!(output["value"][1] == "8");

    let mut output = instant_series(timed_sample(1, "2"));
    check!(
        !super::super::prelude::apply_metric_binary_arithmetic_to_series_with_left_operand(
            &mut output,
            &instant_series(timed_sample(2, "10")),
            MetricScalarArithmeticOp::Subtract,
        ),
        "two different instants have no arithmetic between them"
    );
}
