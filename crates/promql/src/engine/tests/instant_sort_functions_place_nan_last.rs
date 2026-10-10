use super::*;

/// Prometheus sends a `NaN` to the bottom under `sort` and under `sort_desc`.
/// A `NaN` is neither the largest value nor the smallest one, so no direction
/// puts it first. The `sort_by_label` family orders by label and never reads
/// the value, so the `NaN` series takes the position its labels give it.
#[tokio::test]
pub(crate) async fn instant_sort_functions_place_nan_last() {
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
        ZonedQueueDepth {
            instance: "api-n",
            zone: "us-east-1a",
            depth: f64::NAN,
        },
    ]);
    for (query, expected_instances) in [
        ("sort(queue_depth)", ["api-a", "api-c", "api-b", "api-n"]),
        (
            "sort_desc(queue_depth)",
            ["api-b", "api-c", "api-a", "api-n"],
        ),
        (
            r#"sort_by_label(queue_depth, "zone", "instance")"#,
            ["api-a", "api-c", "api-n", "api-b"],
        ),
        (
            r#"sort_by_label_desc(queue_depth, "zone", "instance")"#,
            ["api-b", "api-n", "api-c", "api-a"],
        ),
    ] {
        let samples = instant_vector(&engine, query, 10_000).await;
        let instances = samples
            .iter()
            .map(|sample| sample.labels.get("instance").unwrap())
            .collect::<Vec<_>>();
        assert2::assert!(instances == expected_instances);
        let nan_position = instances
            .iter()
            .position(|instance| *instance == "api-n")
            .unwrap();
        assert2::assert!(
            matches!(samples[nan_position].value, SampleValue::Float(value) if value.is_nan())
        );
    }
}
