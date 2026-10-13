use super::*;

/// `group_left (...)` copies the named labels off the one side. A metadata
/// label named there must still be skipped, or every result row inherits the
/// *other* operand's metric name.
#[tokio::test]
pub(crate) async fn group_left_does_not_copy_a_metadata_label_from_the_one_side() {
    let mut store = InMemoryMetricStore::new();
    for case in [
        InstanceValueCase {
            instance: "1",
            value: 3.0,
        },
        InstanceValueCase {
            instance: "2",
            value: 4.0,
        },
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "a"),
                crate::test_support::TestLabelPair::new("job", "x"),
                crate::test_support::TestLabelPair::new("instance", case.instance),
            ]),
            10_000,
            case.value,
        );
    }
    store.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "b"),
            crate::test_support::TestLabelPair::new("job", "x"),
        ]),
        10_000,
        10.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "a + on (job) group_left(__name__) b", 10_000).await;
    assert2::assert!(samples.len() == 2);
    for sample in &samples {
        assert2::assert!(sample.labels.get("__name__") == None);
    }
}
