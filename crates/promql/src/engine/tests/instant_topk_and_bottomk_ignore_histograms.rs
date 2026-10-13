use super::*;

struct FloatSeries {
    instance: &'static str,
    value: f64,
}

struct RankingCase {
    query: &'static str,
    expected_instance: &'static str,
    expected_value: f64,
}

#[tokio::test]
pub(crate) async fn instant_topk_and_bottomk_ignore_histograms() {
    let mut store = InMemoryMetricStore::new();
    for sample in [
        FloatSeries {
            instance: "a",
            value: 1.0,
        },
        FloatSeries {
            instance: "b",
            value: 3.0,
        },
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "memory_bytes"),
                crate::test_support::TestLabelPair::new("instance", sample.instance),
            ]),
            10_000,
            sample.value,
        );
    }
    store.push_histogram(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "memory_bytes"),
            crate::test_support::TestLabelPair::new("instance", "hist"),
        ]),
        10_000,
        native_histogram(4.0, 10.0),
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for case in [
        RankingCase {
            query: "topk(1, memory_bytes)",
            expected_instance: "b",
            expected_value: 3.0,
        },
        RankingCase {
            query: "bottomk(1, memory_bytes)",
            expected_instance: "a",
            expected_value: 1.0,
        },
    ] {
        let samples = instant_vector(&engine, case.query, 10_000).await;
        assert2::assert!(samples.len() == 1);
        assert2::assert!(samples[0].labels.get("__name__") == Some("memory_bytes"));
        assert2::assert!(samples[0].labels.get("instance") == Some(case.expected_instance));
        assert2::assert!(approx_eq(
            float_value(&samples[0].value),
            case.expected_value
        ));
    }
}
