use super::*;

/// The in-place form of vector arithmetic: the left series is both the
/// left operand and the output, so it is cloned before being written to.
/// That clone is what keeps `a - b` from computing against a value it has
/// already overwritten, and it only shows when the operator is
/// non-commutative AND the result differs from the operand.
#[test]
pub(crate) fn in_place_vector_arithmetic_reads_the_left_operand_before_writing_it() {
    use krabka_logql::MetricScalarArithmeticOp;

    let series = |samples: &[(i64, &str)]| {
        serde_json::json!({
            "metric": {"app": "api"},
            "values": samples
                .iter()
                .map(|(ts, value)| serde_json::json!([ts, value]))
                .collect::<Vec<_>>(),
        })
    };
    let pairs = |value: &serde_json::Value| {
        value
            .get("values")
            .and_then(serde_json::Value::as_array)
            .expect("the series has values")
            .iter()
            .map(|sample| {
                (
                    sample[0].as_i64().expect("a timestamp"),
                    sample[1].as_str().expect("a value").to_string(),
                )
            })
            .collect::<Vec<_>>()
    };
    // 2 and 3 have no right sample and are adjacent, so an index that
    // advanced on removal would keep one of them.
    let right = series(&[(1, "2"), (6, "1")]);
    let apply = |op| {
        let mut left = series(&[(1, "10"), (2, "20"), (3, "20"), (6, "7")]);
        let kept = super::super::prelude::apply_metric_binary_arithmetic_to_series(&mut left, &right, op);
        (kept, pairs(&left))
    };

    check!(
        apply(MetricScalarArithmeticOp::Subtract)
            == (true, vec![(1, "8".to_string()), (6, "6".to_string())]),
        "10-2 and 7-1, and the unmatched pair dropped"
    );
    check!(
        apply(MetricScalarArithmeticOp::Divide)
            == (true, vec![(1, "5".to_string()), (6, "7".to_string())])
    );

    // Everything dropped reports false so the caller can discard the
    // series rather than emit one with no samples.
    let mut orphan = series(&[(9, "1")]);
    check!(!super::super::prelude::apply_metric_binary_arithmetic_to_series(
        &mut orphan,
        &right,
        MetricScalarArithmeticOp::Subtract,
    ));

    // A right series with no values matches nothing at all.
    let mut left = series(&[(1, "10")]);
    check!(!super::super::prelude::apply_metric_binary_arithmetic_to_series(
        &mut left,
        &serde_json::json!({"metric": {}}),
        MetricScalarArithmeticOp::Subtract,
    ));

    // The instant shape, where the same clone-before-write applies to the
    // single sample.
    let instant = |ts: i64, value: &str| serde_json::json!({"metric": {}, "value": [ts, value]});
    let mut left = instant(1, "10");
    check!(super::super::prelude::apply_metric_binary_arithmetic_to_series(
        &mut left,
        &instant(1, "2"),
        MetricScalarArithmeticOp::Subtract,
    ));
    check!(left["value"][1] == "8", "10-2, not 2-10 and not 0");
}
