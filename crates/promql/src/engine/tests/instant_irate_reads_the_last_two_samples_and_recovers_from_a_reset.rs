use super::*;

#[tokio::test]
pub(crate) async fn instant_irate_reads_the_last_two_samples_and_recovers_from_a_reset() {
    for (name, samples, expected) in [
        (
            "two samples are enough",
            vec![(0_i64, 10.0), (60_000, 70.0)],
            1.0,
        ),
        (
            "a flat counter rates to zero",
            vec![(0, 10.0), (60_000, 10.0)],
            0.0,
        ),
        (
            "a reset takes the last value alone",
            vec![(0, 100.0), (60_000, 10.0)],
            10.0 / 60.0,
        ),
    ] {
        let mut store = InMemoryMetricStore::new();
        for (ts_ms, value) in samples {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", "http_requests_total"), ("job", "api")]),
                ts_ms,
                value,
            );
        }
        // A native histogram under the same name routes the selector through
        // the interpreter's range kernel rather than the operator leaf. It
        // folds to nothing itself -- `irate` wants floats -- so the one series
        // that comes back is the counter.
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", "http_requests_total"), ("job", "cache")]),
            60_000,
            native_histogram(4.0, 1.0),
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let result = engine
            .query_instant("tenant-a", "irate(http_requests_total[5m])", 60_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        check!(samples.len() == 1, "{name}");
        check!(
            approx_eq(float_value(&samples[0].value), expected),
            "{name}: {}",
            float_value(&samples[0].value)
        );
    }
}
