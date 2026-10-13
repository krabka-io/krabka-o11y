use super::*;

#[derive(bon::Builder)]
struct AverageCase {
    name: &'static str,
    values: Vec<f64>,
    expected: f64,
}

/// Prometheus' mean keeps an infinite running value infinite, and only a
/// contradicting sample turns it into NaN. Nothing else in the suite drives
/// the kernel's infinity arms, which decide when a sample is skipped.
#[tokio::test]
pub(crate) async fn instant_avg_over_time_holds_an_infinite_mean_until_it_is_contradicted() {
    let cases = [
        AverageCase::builder()
            .name("two infinities of the same sign stay that infinity")
            .values(vec![f64::INFINITY, f64::INFINITY])
            .expected(f64::INFINITY)
            .build(),
        AverageCase::builder()
            .name("a finite sample cannot pull it back")
            .values(vec![f64::INFINITY, 5.0])
            .expected(f64::INFINITY)
            .build(),
        AverageCase::builder()
            .name("the other infinity contradicts it")
            .values(vec![f64::INFINITY, f64::NEG_INFINITY])
            .expected(f64::NAN)
            .build(),
        AverageCase::builder()
            .name("so does a NaN")
            .values(vec![f64::INFINITY, f64::NAN])
            .expected(f64::NAN)
            .build(),
        AverageCase::builder()
            .name("and it works negative too")
            .values(vec![f64::NEG_INFINITY, -5.0])
            .expected(f64::NEG_INFINITY)
            .build(),
        // `1.0 + 0.1 + 0.1` rounds up to `1.2000000000000002`, and only the
        // compensation carries the lost bit back. Adding it lands the mean on
        // `0.4`; subtracting it, or dropping it, lands one ulp either side.
        AverageCase::builder()
            .name("the compensation is added back, not subtracted")
            .values(vec![1.0, 0.1, 0.1])
            .expected(0.4)
            .build(),
    ];
    for case_index in 0..cases.len() {
        let case = &cases[case_index];
        let mut store = InMemoryMetricStore::new();
        for index in 0..case.values.len() {
            let value = case.values[index];
            store.push_float(
                "tenant-a",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "queue_depth"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                ]),
                i64::try_from(index).unwrap() * 1_000,
                value,
            );
        }
        // A native histogram under the same name routes the selector through
        // the interpreter's range kernel rather than the operator leaf.
        store.push_histogram(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "queue_depth"),
                crate::test_support::TestLabelPair::new("job", "cache"),
            ]),
            1_000,
            native_histogram(4.0, 1.0),
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(&engine, "avg_over_time(queue_depth[5m])", 60_000).await;
        let sample = samples
            .iter()
            .find(|sample| sample.labels.get("job") == Some("api"))
            .unwrap_or_else(|| panic!("{}: the float series is missing", case.name));
        let got = float_value(&sample.value);
        if case.expected.is_nan() {
            check!(got.is_nan(), "{}: {got}", case.name);
        } else {
            // Exact, not approximate: the compensation case differs from its
            // mutants by a single ulp.
            check!(
                got.to_bits() == case.expected.to_bits(),
                "{}: {got}",
                case.name
            );
        }
    }
}
