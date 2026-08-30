use super::*;

/// A `PromQL` comparison between two vectors drops the samples that fail it
/// and gives the survivors the LEFT operand's value -- the comparison is a
/// filter, not a rewrite to a boolean. With the `bool` modifier it becomes
/// the opposite: nothing is dropped and every value becomes "1" or "0".
/// Both modes are checked over the same pair, since a mutant that ignores
/// the modifier agrees with whichever mode it happens to implement.
///
/// Samples are removed in place from two different sites -- one for a
/// timestamp the left side lacks, one for a failed comparison -- and
/// neither may advance the index. So the fixture drops an ADJACENT PAIR at
/// each site: a lone drop cannot show a skipped neighbour.
#[test]
pub(crate) fn a_vector_comparison_filters_and_takes_the_left_operand() {
    use krabka_logql::ComparisonOp;

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
    let left = series(&[(1, "10"), (4, "20"), (5, "20")]);
    // 2 and 3 have no left sample; 4 and 5 fail the comparison. Each pair
    // is adjacent, so an index that advanced on removal would keep one.
    let right = || series(&[(1, "1"), (2, "1"), (3, "1"), (4, "30"), (5, "30"), (6, "1")]);

    let mut output = right();
    check!(
        super::super::prelude::apply_metric_binary_comparison_to_series_with_left_operand(
            &mut output,
            &left,
            ComparisonOp::Greater,
            false,
        )
    );
    check!(
        pairs(&output) == vec![(1, "10".to_string())],
        "only 10 > 1 survives, carrying the LEFT value"
    );

    // With `bool`, the failures stay and report 0 -- but a sample the left
    // side never had is still dropped, because there is nothing to compare.
    let mut output = right();
    check!(
        super::super::prelude::apply_metric_binary_comparison_to_series_with_left_operand(
            &mut output,
            &left,
            ComparisonOp::Greater,
            true,
        )
    );
    check!(
        pairs(&output)
            == vec![
                (1, "1".to_string()),
                (4, "0".to_string()),
                (5, "0".to_string()),
            ]
    );

    // The operator is honoured, not assumed: the same pair under `<`
    // keeps exactly the samples `>` dropped.
    let mut output = right();
    check!(
        super::super::prelude::apply_metric_binary_comparison_to_series_with_left_operand(
            &mut output,
            &left,
            ComparisonOp::Less,
            false,
        )
    );
    check!(pairs(&output) == vec![(4, "20".to_string()), (5, "20".to_string())]);

    // Everything filtered out reports false so the caller drops the series.
    let mut output = series(&[(1, "99")]);
    check!(
        !super::super::prelude::apply_metric_binary_comparison_to_series_with_left_operand(
            &mut output,
            &left,
            ComparisonOp::Greater,
            false,
        )
    );

    // A left series with no values at all matches nothing.
    let mut output = right();
    check!(
        !super::super::prelude::apply_metric_binary_comparison_to_series_with_left_operand(
            &mut output,
            &serde_json::json!({"metric": {}}),
            ComparisonOp::Greater,
            false,
        )
    );

    // The instant-vector shape carries one `value`, and nothing pre-matches
    // its timestamp the way the range path does -- so the comparison itself
    // has to refuse two samples from different instants. Comparing them
    // would report a result for a moment neither side observed.
    let instant = |ts: i64, value: &str| serde_json::json!({"metric": {}, "value": [ts, value]});
    let mut output = instant(1, "1");
    check!(
        super::super::prelude::apply_metric_binary_comparison_to_series_with_left_operand(
            &mut output,
            &instant(1, "10"),
            ComparisonOp::Greater,
            false,
        ),
        "same instant, and 10 > 1"
    );
    check!(output["value"][1] == "10", "and it takes the left value");

    let mut output = instant(1, "1");
    check!(
        !super::super::prelude::apply_metric_binary_comparison_to_series_with_left_operand(
            &mut output,
            &instant(2, "10"),
            ComparisonOp::Greater,
            false,
        ),
        "different instants do not compare, however the values order"
    );
}
