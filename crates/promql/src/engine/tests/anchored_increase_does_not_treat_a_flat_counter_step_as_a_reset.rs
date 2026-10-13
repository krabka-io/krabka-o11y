use super::*;

#[derive(bon::Builder)]
struct CounterPoint {
    timestamp_ms: i64,
    value: f64,
}

/// A counter that holds its value across a step has not reset. The anchored
/// `increase` correction fires only on a strict decrease, so a flat pair must
/// contribute nothing -- counting it as a reset adds the whole pre-step value
/// back and more than triples the answer here.
#[tokio::test]
pub(crate) async fn anchored_increase_does_not_treat_a_flat_counter_step_as_a_reset() {
    let mut store = InMemoryMetricStore::new();
    for point in [
        CounterPoint::builder().timestamp_ms(0).value(5.0).build(),
        CounterPoint::builder()
            .timestamp_ms(60_000)
            .value(5.0)
            .build(),
        CounterPoint::builder()
            .timestamp_ms(120_000)
            .value(7.0)
            .build(),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "ctr")]),
            point.timestamp_ms,
            point.value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "increase(anchored(ctr[5m]))", 120_000).await;
    assert2::assert!(samples.len() == 1);
    assert2::assert!(approx_eq(float_value(&samples[0].value), 2.0));

    // The same fold as `rate`, divided by the range in seconds. `increase`
    // never reaches that division.
    let QueryResult::InstantVector(samples) = engine
        .query_instant(&tenant_id("tenant-a"), "rate(anchored(ctr[5m]))", 120_000)
        .await
        .expect("an anchored rate")
    else {
        panic!("expected a vector");
    };
    assert2::assert!(approx_eq(
        float_value(&samples[0].value),
        0.006_666_666_666_666_667
    ));
}
