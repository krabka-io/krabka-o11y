use super::*;

#[tokio::test]
pub(crate) async fn tsdb_stats_ignore_empty_side_min_time() {
    let mut hot_only = InMemoryMetricStore::new();
    hot_only.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        40_000,
        1.0,
    );
    let store = MergedMetricStore::new(InMemoryMetricStore::new(), hot_only);
    let stats = store.tsdb_stats("tenant-a").await.unwrap();
    assert2::assert!(stats.head_stats.min_time == 40_000);

    let mut cold_only = InMemoryMetricStore::new();
    cold_only.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("job", "api")]),
        50_000,
        1.0,
    );
    let store = MergedMetricStore::new(cold_only, InMemoryMetricStore::new());
    let stats = store.tsdb_stats("tenant-a").await.unwrap();
    assert2::assert!(stats.head_stats.min_time == 50_000);
}
