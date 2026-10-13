use super::*;

struct SortOrderExpectation {
    query: &'static str,
    instances: [&'static str; 3],
}

#[tokio::test]
pub(crate) async fn instant_sort_functions_order_vector_by_sample_value() {
    let engine = zoned_queue_depth_engine(&[
        ZonedQueueDepth {
            instance: "api-b",
            zone: "us-west-2b",
            depth: 3.0,
        },
        ZonedQueueDepth {
            instance: "api-a",
            zone: "us-east-1a",
            depth: 1.0,
        },
        ZonedQueueDepth {
            instance: "api-c",
            zone: "us-east-1a",
            depth: 2.0,
        },
    ]);
    for expectation in [
        SortOrderExpectation {
            query: "sort(queue_depth)",
            instances: ["api-a", "api-c", "api-b"],
        },
        SortOrderExpectation {
            query: "sort_desc(queue_depth)",
            instances: ["api-b", "api-c", "api-a"],
        },
        SortOrderExpectation {
            query: r#"sort_by_label(queue_depth, "zone", "instance")"#,
            instances: ["api-a", "api-c", "api-b"],
        },
        SortOrderExpectation {
            query: r#"sort_by_label_desc(queue_depth, "zone", "instance")"#,
            instances: ["api-b", "api-c", "api-a"],
        },
    ] {
        let samples = instant_vector(&engine, expectation.query, 10_000).await;
        assert2::assert!(samples.len() == 3);
        let instances = samples
            .iter()
            .map(|sample| sample.labels.get("instance").unwrap().to_string())
            .collect::<Vec<_>>();
        assert2::assert!(instances == expectation.instances);
        assert2::assert!(
            samples
                .iter()
                .all(|sample| sample.labels.get("__name__") == Some("queue_depth"))
        );
    }
}
