use super::*;

struct SortExpectation {
    query: &'static str,
    instances: [&'static str; 4],
}

/// Prometheus sends a `NaN` to the bottom under `sort` and under `sort_desc`.
/// A `NaN` is neither the largest value nor the smallest one, so no direction
/// puts it first. The `sort_by_label` family orders by label and never reads
/// the value, so the `NaN` series takes the position its labels give it.
#[tokio::test]
pub(crate) async fn instant_sort_functions_place_nan_last() {
    let mut series = standard_zoned_queue_depth_series().to_vec();
    series.push(ZonedQueueDepth {
        instance: "api-n",
        zone: "us-east-1a",
        depth: f64::NAN,
    });
    let engine = zoned_queue_depth_engine(&series);
    for expectation in [
        SortExpectation {
            query: "sort(queue_depth)",
            instances: ["api-a", "api-c", "api-b", "api-n"],
        },
        SortExpectation {
            query: "sort_desc(queue_depth)",
            instances: ["api-b", "api-c", "api-a", "api-n"],
        },
        SortExpectation {
            query: r#"sort_by_label(queue_depth, "zone", "instance")"#,
            instances: ["api-a", "api-c", "api-n", "api-b"],
        },
        SortExpectation {
            query: r#"sort_by_label_desc(queue_depth, "zone", "instance")"#,
            instances: ["api-b", "api-n", "api-c", "api-a"],
        },
    ] {
        let samples = instant_vector(&engine, expectation.query, 10_000).await;
        let instances = samples
            .iter()
            .map(|sample| sample.labels.get("instance").unwrap())
            .collect::<Vec<_>>();
        assert2::assert!(instances == expectation.instances);
        let nan_position = instances
            .iter()
            .position(|instance| *instance == "api-n")
            .unwrap();
        assert2::assert!(
            matches!(samples[nan_position].value, SampleValue::Float(value) if value.is_nan())
        );
    }
}
