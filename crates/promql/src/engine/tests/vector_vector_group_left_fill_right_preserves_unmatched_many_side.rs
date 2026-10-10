use super::*;

#[tokio::test]
pub(crate) async fn vector_vector_group_left_fill_right_preserves_unmatched_many_side() {
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
        InstanceRequests {
            job: "worker",
            instance: "c",
            total: 7.0,
        },
    ]);
    let samples = instant_vector(
        &engine,
        "http_requests_total + on (job) group_left(region) fill_right(0) target_info",
        10_000,
    )
    .await;
    assert_filled_many_side(&samples);
}
