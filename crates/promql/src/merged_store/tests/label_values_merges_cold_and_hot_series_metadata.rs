use super::*;

#[tokio::test]
pub(crate) async fn label_values_merges_cold_and_hot_series_metadata() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    cold.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        10_000,
        1.0,
    );
    hot.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "worker")]),
        20_000,
        2.0,
    );
    hot.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        30_000,
        3.0,
    );

    let store = MergedMetricStore::new(cold, hot);
    let values = store
        .label_values("tenant-a", "job", &[], 0, 30_000)
        .await
        .unwrap();

    assert2::assert!(values == vec!["api", "worker"]);
}
