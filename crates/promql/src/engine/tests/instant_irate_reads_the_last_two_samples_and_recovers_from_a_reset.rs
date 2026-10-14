use super::*;

#[derive(bon::Builder)]
struct CounterSample {
    timestamp_ms: i64,
    value: f64,
}

#[derive(bon::Builder)]
struct IrateCase {
    name: &'static str,
    samples: Vec<CounterSample>,
    expected: f64,
}

#[tokio::test]
pub(crate) async fn instant_irate_reads_the_last_two_samples_and_recovers_from_a_reset() {
    for case in [
        IrateCase::builder()
            .name("two samples are enough")
            .samples(vec![
                CounterSample::builder().timestamp_ms(0).value(10.0).build(),
                CounterSample::builder()
                    .timestamp_ms(60_000)
                    .value(70.0)
                    .build(),
            ])
            .expected(1.0)
            .build(),
        IrateCase::builder()
            .name("a flat counter rates to zero")
            .samples(vec![
                CounterSample::builder().timestamp_ms(0).value(10.0).build(),
                CounterSample::builder()
                    .timestamp_ms(60_000)
                    .value(10.0)
                    .build(),
            ])
            .expected(0.0)
            .build(),
        IrateCase::builder()
            .name("a reset takes the last value alone")
            .samples(vec![
                CounterSample::builder()
                    .timestamp_ms(0)
                    .value(100.0)
                    .build(),
                CounterSample::builder()
                    .timestamp_ms(60_000)
                    .value(10.0)
                    .build(),
            ])
            .expected(10.0 / 60.0)
            .build(),
    ] {
        let mut store = InMemoryMetricStore::new();
        for sample in case.samples {
            store.push_float(
                "tenant-a",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                ]),
                sample.timestamp_ms,
                sample.value,
            );
        }
        // A native histogram under the same name routes the selector through
        // the interpreter's range kernel rather than the operator leaf. It
        // folds to nothing itself -- `irate` wants floats -- so the one series
        // that comes back is the counter.
        store.push_histogram(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
                crate::test_support::TestLabelPair::new("job", "cache"),
            ]),
            60_000,
            native_histogram(4.0, 1.0),
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(&engine, "irate(http_requests_total[5m])", 60_000).await;
        check!(samples.len() == 1, "{}", case.name);
        check!(
            approx_eq(float_value(&samples[0].value), case.expected),
            "{}: {}",
            case.name,
            float_value(&samples[0].value)
        );
    }
}
