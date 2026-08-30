use super::*;

#[tokio::test]
pub(crate) async fn range_query_counts_sample_present_in_both_stores_once() {
    // The same (fingerprint, timestamp) sample lives in both cold and hot —
    // the steady state, since hot retention is time-based and independent of
    // compaction. Without (fp, ts) dedup the merged scan double-counts it.
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    let labels = labels(&[("__name__", "up"), ("job", "api")]);
    cold.push_float("tenant-a", labels.clone(), 10_000, 1.0);
    cold.push_float("tenant-a", labels.clone(), 20_000, 1.0);
    // Hot re-reports the 20s sample (still within hot retention) and adds 30s.
    hot.push_float("tenant-a", labels.clone(), 20_000, 1.0);
    hot.push_float("tenant-a", labels.clone(), 30_000, 1.0);

    let store = MergedMetricStore::new(cold, hot);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let result = engine
        .query_instant("tenant-a", "count_over_time(up[1m])", 30_000)
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected instant vector");
    };
    assert2::assert!(samples.len() == 1);
    // Three distinct timestamps (10s, 20s, 30s); the duplicated 20s sample
    // must be counted once, not twice.
    assert2::assert!(samples[0].value == SampleValue::Float(3.0));

    // A windowed sum must likewise see each timestamp once.
    let result = engine
        .query_instant("tenant-a", "sum_over_time(up[1m])", 30_000)
        .await
        .unwrap();
    let QueryResult::InstantVector(samples) = result else {
        panic!("expected instant vector");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].value == SampleValue::Float(3.0));
}
