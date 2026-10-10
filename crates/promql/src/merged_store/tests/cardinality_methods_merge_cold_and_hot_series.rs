use super::*;
use crate::test_support::{ExpectedCardinality, assert_tenant_cardinality};

#[tokio::test]
pub(crate) async fn cardinality_methods_merge_cold_and_hot_series() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    let api = labels(&[("__name__", "up"), ("instance", "a"), ("job", "api")]);
    let worker = labels(&[("__name__", "up"), ("instance", "b"), ("job", "worker")]);
    cold.push_float("tenant-a", api.clone(), 10_000, 1.0);
    hot.push_float("tenant-a", worker.clone(), 20_000, 2.0);

    let store = MergedMetricStore::new(cold, hot);
    assert_tenant_cardinality(
        &store,
        ExpectedCardinality {
            active_series: vec![api, worker],
            label_name_counts: &[("__name__", 2), ("instance", 2), ("job", 2)],
            label_value_counts: &[
                ("__name__", "up", 2),
                ("instance", "a", 1),
                ("instance", "b", 1),
                ("job", "api", 1),
                ("job", "worker", 1),
            ],
        },
    )
    .await;
}
