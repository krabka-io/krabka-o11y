use super::*;

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
    for (query, expected_instances) in [
        ("sort(queue_depth)", ["api-a", "api-c", "api-b"]),
        ("sort_desc(queue_depth)", ["api-b", "api-c", "api-a"]),
        (
            r#"sort_by_label(queue_depth, "zone", "instance")"#,
            ["api-a", "api-c", "api-b"],
        ),
        (
            r#"sort_by_label_desc(queue_depth, "zone", "instance")"#,
            ["api-b", "api-c", "api-a"],
        ),
    ] {
        let samples = instant_vector(&engine, query, 10_000).await;
        assert2::assert!(samples.len() == 3);
        let instances = samples
            .iter()
            .map(|sample| sample.labels.get("instance").unwrap().to_string())
            .collect::<Vec<_>>();
        assert2::assert!(instances == expected_instances);
        assert2::assert!(
            samples
                .iter()
                .all(|sample| sample.labels.get("__name__") == Some("queue_depth"))
        );
    }
}
