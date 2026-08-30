use super::*;

#[tokio::test]
pub(crate) async fn cardinality_methods_merge_cold_and_hot_series() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    let api = labels(&[("__name__", "up"), ("instance", "a"), ("job", "api")]);
    let worker = labels(&[("__name__", "up"), ("instance", "b"), ("job", "worker")]);
    cold.push_float("tenant-a", api.clone(), 10_000, 1.0);
    hot.push_float("tenant-a", worker.clone(), 20_000, 2.0);

    let store = MergedMetricStore::new(cold, hot);
    let mut active_series = store.cardinality_active_series("tenant-a").await.unwrap();
    active_series.sort_by_key(|labels| labels.get("instance").unwrap_or("").to_string());
    assert2::assert!(active_series == vec![api, worker]);

    let label_names = store.cardinality_label_names("tenant-a").await.unwrap();
    let name_counts = label_names
        .iter()
        .map(|stat| (stat.name.as_str(), stat.series_count))
        .collect::<Vec<_>>();
    assert2::assert!(name_counts == vec![("__name__", 2), ("instance", 2), ("job", 2)]);

    let label_values = store.cardinality_label_values("tenant-a").await.unwrap();
    let value_counts = label_values
        .iter()
        .map(|stat| {
            (
                stat.label_name.as_str(),
                stat.label_value.as_str(),
                stat.series_count,
            )
        })
        .collect::<Vec<_>>();
    assert2::assert!(
        value_counts
            == vec![
                ("__name__", "up", 2),
                ("instance", "a", 1),
                ("instance", "b", 1),
                ("job", "api", 1),
                ("job", "worker", 1),
            ]
    );
}
