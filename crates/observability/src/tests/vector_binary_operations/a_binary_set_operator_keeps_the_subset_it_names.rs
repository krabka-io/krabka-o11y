use super::*;

/// `apply_metric_binary_set_to_series` filters one series against another
/// by the set operator. All three operators are applied to the SAME pair,
/// so each keeps a different subset -- with one operator alone, a rule
/// that returns a constant looks correct on whichever half it agrees with.
///
/// The filter walks with an index and removes in place, only advancing
/// when it keeps. A removal that also advanced would skip the sample that
/// slid into the gap, so the dropped samples are adjacent here.
#[test]
pub(crate) fn a_binary_set_operator_keeps_the_subset_it_names() {
    use krabka_logql::MetricBinarySetOp;

    let range = |samples: &[i64]| {
        serde_json::json!({
            "metric": {"app": "api"},
            "values": samples
                .iter()
                .map(|ts| serde_json::json!([ts, ts.to_string()]))
                .collect::<Vec<_>>(),
        })
    };
    let timestamps = |series: &serde_json::Value| {
        series
            .get("values")
            .and_then(serde_json::Value::as_array)
            .expect("the series has values")
            .iter()
            .map(|sample| sample[0].as_i64().expect("a timestamp"))
            .collect::<Vec<_>>()
    };
    // 2 and 3 are adjacent, so `and` and `unless` each drop a run rather
    // than isolated samples.
    let right = range(&[2, 3]);
    let apply = |op| {
        let mut left = range(&[1, 2, 3, 4]);
        let kept = super::super::prelude::apply_metric_binary_set_to_series(&mut left, &right, op);
        (kept, timestamps(&left))
    };

    check!(apply(MetricBinarySetOp::And) == (true, vec![2, 3]));
    check!(apply(MetricBinarySetOp::Unless) == (true, vec![1, 4]));
    check!(apply(MetricBinarySetOp::Or) == (true, vec![1, 2, 3, 4]));

    // When the filter empties the series it reports false, so the caller
    // can drop it rather than emitting an empty series.
    let mut left = range(&[1, 4]);
    check!(!super::super::prelude::apply_metric_binary_set_to_series(
        &mut left,
        &right,
        MetricBinarySetOp::And
    ));
    check!(timestamps(&left).is_empty());

    // `or` keeps a series the right side never matches at all.
    let mut left = range(&[9]);
    check!(super::super::prelude::apply_metric_binary_set_to_series(
        &mut left,
        &right,
        MetricBinarySetOp::Or
    ));

    // An instant vector carries one `value` rather than `values`, and the
    // same three rules apply to it.
    let instant = |ts: i64| serde_json::json!({"metric": {}, "value": [ts, ts.to_string()]});
    let mut matching = instant(5);
    check!(super::super::prelude::apply_metric_binary_set_to_series(
        &mut matching,
        &instant(5),
        MetricBinarySetOp::And
    ));
    let mut differing = instant(5);
    check!(!super::super::prelude::apply_metric_binary_set_to_series(
        &mut differing,
        &instant(6),
        MetricBinarySetOp::And
    ));
    let mut differing = instant(5);
    check!(super::super::prelude::apply_metric_binary_set_to_series(
        &mut differing,
        &instant(6),
        MetricBinarySetOp::Unless
    ));

    // A series with neither shape matches nothing.
    let mut empty = serde_json::json!({"metric": {}});
    check!(!super::super::prelude::apply_metric_binary_set_to_series(
        &mut empty,
        &right,
        MetricBinarySetOp::Or
    ));
}
