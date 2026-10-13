use super::*;

#[tokio::test]
pub(crate) async fn tsdb_blocks_reports_compaction_manifest_blocks() {
    let series_labels = up_api_labels();
    let batch = up_api_sample_batch(&[(1_000, 1.0), (2_000, 0.0)]);
    let store = manifest_store(ManifestBlock {
        kind: MetricBlockKind::Float,
        block_key: "metrics/float/0002.parquet",
        schema: float_sample_schema(),
        batch,
        last_offset: 1,
        series_labels,
    })
    .await;
    let blocks = store.tsdb_blocks("tenant-a").await.unwrap();

    assert2::assert!(
        blocks
            == vec![TsdbBlock {
                id: "metrics/float/0002.parquet".to_string(),
                min_time: 1_000,
                max_time: 2_000,
                num_samples: 2,
                num_series: 1,
            }]
    );
}
