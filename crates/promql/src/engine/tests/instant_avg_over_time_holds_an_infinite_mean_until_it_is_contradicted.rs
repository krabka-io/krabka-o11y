use super::*;

/// Prometheus' mean keeps an infinite running value infinite, and only a
/// contradicting sample turns it into NaN. Nothing else in the suite drives
/// the kernel's infinity arms, which decide when a sample is skipped.
#[tokio::test]
pub(crate) async fn instant_avg_over_time_holds_an_infinite_mean_until_it_is_contradicted() {
    for (name, values, expected) in [
        (
            "two infinities of the same sign stay that infinity",
            vec![f64::INFINITY, f64::INFINITY],
            f64::INFINITY,
        ),
        (
            "a finite sample cannot pull it back",
            vec![f64::INFINITY, 5.0],
            f64::INFINITY,
        ),
        (
            "the other infinity contradicts it",
            vec![f64::INFINITY, f64::NEG_INFINITY],
            f64::NAN,
        ),
        ("so does a NaN", vec![f64::INFINITY, f64::NAN], f64::NAN),
        (
            "and it works negative too",
            vec![f64::NEG_INFINITY, -5.0],
            f64::NEG_INFINITY,
        ),
        // The 1e-16 rounds away as the running mean absorbs it, and only the
        // compensation still holds it. Adding that back lands on the nearest
        // double to two thirds; subtracting it, or dropping it, lands one ulp
        // either side.
        (
            "the compensation is added back, not subtracted",
            vec![1.0, 1e-16, 1.0],
            0.666_666_666_666_666_7,
        ),
    ] {
        let mut store = InMemoryMetricStore::new();
        for (index, value) in values.iter().enumerate() {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "queue_depth"), ("job", "api")]),
                i64::try_from(index).unwrap() * 1_000,
                *value,
            );
        }
        // A native histogram under the same name routes the selector through
        // the interpreter's range kernel rather than the operator leaf.
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", "queue_depth"), ("job", "cache")]),
            1_000,
            native_histogram(4.0, 1.0),
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "avg_over_time(queue_depth[5m])", 60_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        let sample = samples
            .iter()
            .find(|sample| sample.labels.get("job") == Some("api"))
            .unwrap_or_else(|| panic!("{name}: the float series is missing"));
        let got = float_value(&sample.value);
        if expected.is_nan() {
            check!(got.is_nan(), "{name}: {got}");
        } else {
            // Exact, not approximate: the compensation case differs from its
            // mutants by a single ulp.
            check!(got.to_bits() == expected.to_bits(), "{name}: {got}");
        }
    }
}
