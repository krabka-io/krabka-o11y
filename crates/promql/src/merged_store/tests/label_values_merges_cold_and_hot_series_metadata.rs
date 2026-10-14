use super::*;

#[tokio::test]
pub(crate) async fn label_values_merges_cold_and_hot_series_metadata() {
    let store = merged_store_with_samples(
        &[MergedFloatSample::builder()
            .metric("up")
            .job("api")
            .instance("")
            .cluster("")
            .timestamp_ms(10_000)
            .value(1.0)
            .build()],
        &[
            MergedFloatSample::builder()
                .metric("up")
                .job("worker")
                .instance("")
                .cluster("")
                .timestamp_ms(20_000)
                .value(2.0)
                .build(),
            MergedFloatSample::builder()
                .metric("up")
                .job("api")
                .instance("")
                .cluster("")
                .timestamp_ms(30_000)
                .value(3.0)
                .build(),
        ],
    );
    let values = store
        .label_values("tenant-a", "job", &[], 0, 30_000)
        .await
        .unwrap();
    assert2::assert!(values == vec!["api", "worker"]);
}
