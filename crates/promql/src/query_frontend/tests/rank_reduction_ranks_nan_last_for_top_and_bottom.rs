use super::*;

/// A `NaN` is neither a top value nor a bottom value. The sharded rank
/// reduction therefore drops it before it drops any number, which is what the
/// unsharded engine path does. A `topk` answered from shards then selects what
/// the same `topk` answered whole selects. `None` stands for the `NaN` sample
/// in the table below, because no float compares equal to a `NaN`.
#[test]
fn rank_reduction_ranks_nan_last_for_top_and_bottom() {
    let series = |name: &str, value: f64| RangeSeries {
        labels: labels(&[("__name__", "up"), ("series", name)]),
        samples: vec![(0, SampleValue::Float(value))],
    };
    for (kind, k, expected) in [
        (RankReduction::Top, 1, vec![("a", Some(10.0))]),
        (RankReduction::Bottom, 1, vec![("b", Some(2.0))]),
        (
            RankReduction::Top,
            3,
            vec![("a", Some(10.0)), ("b", Some(2.0)), ("c", Some(9.0))],
        ),
        (
            RankReduction::Bottom,
            3,
            vec![("a", Some(10.0)), ("b", Some(2.0)), ("c", Some(9.0))],
        ),
        (
            RankReduction::Top,
            4,
            vec![
                ("a", Some(10.0)),
                ("b", Some(2.0)),
                ("c", Some(9.0)),
                ("n", None),
            ],
        ),
    ] {
        let result = reduce_rank_range_query_results(
            QueryResult::RangeMatrix(vec![
                series("a", 10.0),
                series("b", 2.0),
                series("c", 9.0),
                series("n", f64::NAN),
            ]),
            k,
            kind,
            None,
        )
        .unwrap();

        let QueryResult::RangeMatrix(kept) = result else {
            panic!("rank reduction range matrix");
        };
        let selected = kept
            .iter()
            .map(|series| {
                let SampleValue::Float(value) = series.samples[0].1 else {
                    panic!("rank reduction float sample");
                };
                (
                    series.labels.get("series").unwrap(),
                    (!value.is_nan()).then_some(value),
                )
            })
            .collect::<Vec<_>>();
        assert2::assert!(selected == expected);
    }
}
