use super::*;

/// A counter that holds its value across a step has not reset. The anchored
/// `increase` correction fires only on a strict decrease, so a flat pair must
/// contribute nothing -- counting it as a reset adds the whole pre-step value
/// back and more than triples the answer here.
#[tokio::test]
pub(crate) async fn anchored_increase_does_not_treat_a_flat_counter_step_as_a_reset() {
    let mut store = InMemoryMetricStore::new();
    for (ts, value) in [(0, 5.0), (60_000, 5.0), (120_000, 7.0)] {
        store.push_float("tenant-a", labels(&[("__name__", "ctr")]), ts, value);
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "increase(anchored(ctr[5m]))", 120_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(approx_eq(float_value(&samples[0].value), 2.0));

    // The same fold as `rate`, divided by the range in seconds. `increase`
    // never reaches that division.
    let QueryResult::InstantVector(samples) = engine
        .query_instant("tenant-a", "rate(anchored(ctr[5m]))", 120_000)
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
