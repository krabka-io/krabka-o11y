use super::*;

#[tokio::test]
pub(crate) async fn metadata_merges_cold_and_hot_records() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    cold.push_metadata(
        "tenant-a",
        "requests_total",
        "counter",
        "requests",
        "requests",
    );
    cold.push_metadata("tenant-a", "up", "gauge", "availability", "");
    hot.push_metadata(
        "tenant-a",
        "requests_total",
        "counter",
        "requests",
        "requests",
    );
    hot.push_metadata(
        "tenant-a",
        "latency_seconds",
        "histogram",
        "latency",
        "seconds",
    );

    let store = MergedMetricStore::new(cold, hot);
    let metadata = store.metadata("tenant-a", None).await.unwrap();
    let fields = metadata
        .iter()
        .map(|record| {
            (
                record.metric_family_name.as_str(),
                record.metric_type.as_str(),
                record.help.as_str(),
                record.unit.as_str(),
            )
        })
        .collect::<Vec<_>>();

    assert2::assert!(
        fields
            == vec![
                ("latency_seconds", "histogram", "latency", "seconds"),
                ("requests_total", "counter", "requests", "requests"),
                ("up", "gauge", "availability", ""),
            ]
    );
}
