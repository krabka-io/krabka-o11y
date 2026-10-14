use super::*;
use crate::test_support::{
    ExpectedCardinality, ExpectedLabelNameCount, ExpectedLabelValueCount, assert_tenant_cardinality,
};

#[tokio::test]
pub(crate) async fn cardinality_methods_merge_cold_and_hot_series() {
    let mut cold = InMemoryMetricStore::new();
    let mut hot = InMemoryMetricStore::new();
    let api = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("instance", "a"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    let worker = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("instance", "b"),
        crate::test_support::TestLabelPair::new("job", "worker"),
    ]);
    cold.push_float("tenant-a", api.clone(), 10_000, 1.0);
    hot.push_float("tenant-a", worker.clone(), 20_000, 2.0);

    let store = MergedMetricStore::new(cold, hot);
    assert_tenant_cardinality(
        &store,
        ExpectedCardinality {
            active_series: vec![api, worker],
            label_name_counts: &[
                ExpectedLabelNameCount {
                    name: "__name__",
                    series_count: 2,
                },
                ExpectedLabelNameCount {
                    name: "instance",
                    series_count: 2,
                },
                ExpectedLabelNameCount {
                    name: "job",
                    series_count: 2,
                },
            ],
            label_value_counts: &[
                ExpectedLabelValueCount {
                    name: "__name__",
                    value: "up",
                    series_count: 2,
                },
                ExpectedLabelValueCount {
                    name: "instance",
                    value: "a",
                    series_count: 1,
                },
                ExpectedLabelValueCount {
                    name: "instance",
                    value: "b",
                    series_count: 1,
                },
                ExpectedLabelValueCount {
                    name: "job",
                    value: "api",
                    series_count: 1,
                },
                ExpectedLabelValueCount {
                    name: "job",
                    value: "worker",
                    series_count: 1,
                },
            ],
        },
    )
    .await;
}
