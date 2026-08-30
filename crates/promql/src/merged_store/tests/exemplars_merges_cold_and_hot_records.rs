use super::*;

#[tokio::test]
pub(crate) async fn exemplars_merges_cold_and_hot_records() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    let series = labels(&[("__name__", "request_latency"), ("job", "api")]);
    cold.push_exemplar(
        "tenant-a",
        series.clone(),
        labels(&[("trace_id", "cold")]),
        10_000,
        1.0,
    );
    hot.push_exemplar(
        "tenant-a",
        series.clone(),
        labels(&[("trace_id", "hot")]),
        20_000,
        2.0,
    );

    let store = MergedMetricStore::new(cold, hot);
    let exemplars = store.exemplars("tenant-a", &[], 0, 30_000).await.unwrap();

    assert2::assert!(
        exemplars
            == vec![
                ExemplarRecord {
                    series_labels: series.clone(),
                    labels: labels(&[("trace_id", "cold")]),
                    ts_ms: 10_000,
                    value: 1.0,
                },
                ExemplarRecord {
                    series_labels: series,
                    labels: labels(&[("trace_id", "hot")]),
                    ts_ms: 20_000,
                    value: 2.0,
                },
            ]
    );
}
