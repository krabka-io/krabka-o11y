use super::*;

/// A comparison without `bool` keeps the operand's own labels, metric name
/// and all; every other binary form drops the metric name. The rule is
/// written out separately on five paths -- one-to-one matched and filled,
/// many-to-one filled, one-to-many matched and filled -- and each pairs a
/// comparison with the arithmetic form of the same query, because a
/// comparison case alone cannot tell `is_comparison() && !bool` from
/// `is_comparison() || !bool`.
#[tokio::test]
pub(crate) async fn a_comparison_keeps_the_metric_name_that_arithmetic_drops() {
    let mut store = InMemoryMetricStore::new();
    for (name, job, extra, value) in [
        ("a", "x", Some(("extra", "1")), 5.0),
        ("a", "y", Some(("extra", "2")), 5.0),
        ("b", "x", None, 3.0),
        ("m", "x", Some(("inst", "1")), 7.0),
        ("m", "y", Some(("inst", "9")), 7.0),
    ] {
        let mut pairs = vec![("__name__", name), ("job", job)];
        if let Some(extra) = extra {
            pairs.push(extra);
        }
        store.push_float("tenant-a", labels(&pairs), 10_000, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for (query, want) in [
        // One-to-one: `x` matches, `y` takes the right-fill path.
        (
            "a > on (job) fill_right(0) b",
            vec![
                vec![("__name__", "a"), ("extra", "1"), ("job", "x")],
                vec![("__name__", "a"), ("extra", "2"), ("job", "y")],
            ],
        ),
        (
            "a + on (job) fill_right(0) b",
            vec![vec![("job", "x")], vec![("job", "y")]],
        ),
        // The mirror of the pair above: here the *left* side is filled, so the
        // surviving row keeps the right operand's labels rather than the
        // left's, and only a left-fill query reaches that branch.
        (
            "b < on (job) fill_left(0) a",
            vec![
                vec![("__name__", "a"), ("extra", "2"), ("job", "y")],
                vec![("__name__", "b"), ("job", "x")],
            ],
        ),
        (
            "b + on (job) fill_left(0) a",
            vec![vec![("job", "x")], vec![("job", "y")]],
        ),
        // `bool` puts a comparison back on the arithmetic rule.
        ("a > bool on (job) b", vec![vec![("job", "x")]]),
        // Many-to-one: `x` matches, `y` takes the right-fill path.
        (
            "m > on (job) group_left fill_right(0) b",
            vec![
                vec![("__name__", "m"), ("inst", "1"), ("job", "x")],
                vec![("__name__", "m"), ("inst", "9"), ("job", "y")],
            ],
        ),
        (
            "m + on (job) group_left fill_right(0) b",
            vec![
                vec![("inst", "1"), ("job", "x")],
                vec![("inst", "9"), ("job", "y")],
            ],
        ),
        // One-to-many: `x` matches, `y` takes the left-fill path.
        (
            "b < on (job) group_right fill_left(0) m",
            vec![
                vec![("__name__", "m"), ("inst", "1"), ("job", "x")],
                vec![("__name__", "m"), ("inst", "9"), ("job", "y")],
            ],
        ),
        (
            "b + on (job) group_right fill_left(0) m",
            vec![
                vec![("inst", "1"), ("job", "x")],
                vec![("inst", "9"), ("job", "y")],
            ],
        ),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected a vector for {query}");
        };
        let mut got = samples
            .iter()
            .map(|sample| {
                sample
                    .labels
                    .iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        got.sort();
        let want = want
            .iter()
            .map(|pairs| {
                pairs
                    .iter()
                    .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert2::assert!(got == want, "{query}");
    }
}
