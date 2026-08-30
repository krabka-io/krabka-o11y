use super::*;

#[tokio::test]
pub(crate) async fn label_names_merges_cold_and_hot_series_metadata() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    cold.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("instance", "a"), ("job", "api")]),
        10_000,
        1.0,
    );
    hot.push_float(
        "tenant-a",
        labels(&[("__name__", "up"), ("cluster", "prod"), ("job", "api")]),
        20_000,
        2.0,
    );

    let store = MergedMetricStore::new(cold, hot);
    let names = store.label_names("tenant-a", &[], 0, 30_000).await.unwrap();

    assert2::assert!(names == vec!["__name__", "cluster", "instance", "job"]);
}
