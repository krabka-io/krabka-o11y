use super::*;

struct SmoothedSample {
    timestamp_ms: i64,
    value: f64,
}

struct DeltaExpectation {
    description: &'static str,
    eval_ms: i64,
    value: f64,
}

/// Smoothed ranges interpolate between observed samples and carry the last
/// observed value at the right boundary. They do not extend the last slope.
#[tokio::test]
pub(crate) async fn smoothed_delta_uses_the_observed_right_boundary() {
    let mut store = InMemoryMetricStore::new();
    for sample in [
        SmoothedSample {
            timestamp_ms: 0,
            value: 0.0,
        },
        SmoothedSample {
            timestamp_ms: 60_000,
            value: 60.0,
        },
    ] {
        store.push_float(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "m")]),
            sample.timestamp_ms,
            sample.value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for case in [
        DeltaExpectation {
            description: "start between samples at 3s",
            eval_ms: 123_000,
            value: 57.0,
        },
        DeltaExpectation {
            description: "start between samples at 6s",
            eval_ms: 126_000,
            value: 54.0,
        },
        DeltaExpectation {
            description: "start between samples at 10s",
            eval_ms: 130_000,
            value: 50.0,
        },
    ] {
        let samples = instant_vector(&engine, "delta(smoothed(m[2m]))", case.eval_ms).await;
        assert2::assert!(samples.len() == 1, "{}", case.description);
        assert2::assert!(
            approx_eq(float_value(&samples[0].value), case.value),
            "{}",
            case.description
        );
    }

    // `delta` stops at the difference; only `rate` divides by the range.
    let QueryResult::InstantVector(samples) = engine
        .query_instant(&tenant_id("tenant-a"), "rate(smoothed(m[3m]))", 123_000)
        .await
        .expect("a smoothed rate")
    else {
        panic!("expected a vector");
    };
    assert2::assert!(approx_eq(float_value(&samples[0].value), 1.0 / 3.0));
}
