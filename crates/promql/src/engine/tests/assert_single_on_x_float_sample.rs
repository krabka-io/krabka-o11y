use super::*;

pub(crate) fn assert_single_on_x_float_sample(result: &QueryResult, expected: f64, context: &str) {
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector for {context}");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].labels.get("__name__") == None);
    assert2::assert!(samples[0].labels.get("job") == None);
    assert2::assert!(samples[0].labels.get("x") == Some("1"));
    assert2::assert!(approx_eq(float_value(&samples[0].value), expected));
}

/// A histogram-valued query and the count and sum of its one `on (x)` result.
pub(crate) struct OnXHistogramStats<'a> {
    pub(crate) query: &'a str,
    pub(crate) count: f64,
    pub(crate) sum: f64,
}

/// Checks `histogram_count` and `histogram_sum` of `expected.query` at 10s
/// for `tenant-a`: each one unnamed `x="1"` sample without `job`.
pub(crate) async fn assert_on_x_histogram_stats<S: crate::MetricStore>(
    engine: &PromqlEngine<S>,
    expected: OnXHistogramStats<'_>,
) {
    let OnXHistogramStats { query, count, sum } = expected;
    for (stat_fn, expected_value) in [("histogram_count", count), ("histogram_sum", sum)] {
        let result = engine
            .query_instant(
                &tenant_id("tenant-a"),
                &format!("{stat_fn}({query})"),
                10_000,
            )
            .await
            .unwrap();
        assert_single_on_x_float_sample(&result, expected_value, query);
    }
}
