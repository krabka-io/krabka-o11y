use super::*;

#[tokio::test]
pub(crate) async fn vector_vector_group_left_carries_labels_from_one_side() {
    let engine = region_info_engine(&[
        InstanceRequests {
            job: "api",
            instance: "a",
            total: 100.0,
        },
        InstanceRequests {
            job: "api",
            instance: "b",
            total: 50.0,
        },
    ]);
    let samples = instant_vector(
        &engine,
        "http_requests_total / on (job) group_left(region) target_info",
        10_000,
    )
    .await;
    assert2::assert!(samples.len() == 2);
    for sample in samples {
        check!(sample.labels.get("__name__").is_none());
        check!(sample.labels.get("job") == Some("api"));
        check!(sample.labels.get("region") == Some("east"));
    }
}
