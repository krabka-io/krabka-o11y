use super::*;

#[tokio::test]
pub(crate) async fn label_names_merges_cold_and_hot_series_metadata() {
    let store = merged_store_with_samples(
        &[MergedFloatSample::builder()
            .metric("up")
            .job("api")
            .instance("a")
            .cluster("")
            .timestamp_ms(10_000)
            .value(1.0)
            .build()],
        &[MergedFloatSample::builder()
            .metric("up")
            .job("api")
            .instance("")
            .cluster("prod")
            .timestamp_ms(20_000)
            .value(2.0)
            .build()],
    );
    let names = store.label_names("tenant-a", &[], 0, 30_000).await.unwrap();
    assert2::assert!(names == vec!["__name__", "cluster", "instance", "job"]);
}
