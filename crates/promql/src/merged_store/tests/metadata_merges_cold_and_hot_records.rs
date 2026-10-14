use super::*;

#[derive(Debug, PartialEq)]
struct MetadataFields<'a> {
    family_name: &'a str,
    metric_type: &'a str,
    help: &'a str,
    unit: &'a str,
}

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
    let metadata = store.metadata("tenant-a", None).await.unwrap().metadata;
    let fields = metadata
        .iter()
        .map(|record| MetadataFields {
            family_name: record.metric_family_name.as_str(),
            metric_type: record.metric_type.as_str(),
            help: record.help.as_str(),
            unit: record.unit.as_str(),
        })
        .collect::<Vec<_>>();

    assert2::assert!(
        fields
            == vec![
                MetadataFields {
                    family_name: "latency_seconds",
                    metric_type: "histogram",
                    help: "latency",
                    unit: "seconds",
                },
                MetadataFields {
                    family_name: "requests_total",
                    metric_type: "counter",
                    help: "requests",
                    unit: "requests",
                },
                MetadataFields {
                    family_name: "up",
                    metric_type: "gauge",
                    help: "availability",
                    unit: "",
                },
            ]
    );
}
