use super::*;

#[tokio::test]
pub(crate) async fn prometheus_query_reads_float_samples_from_blockstore() {
    let mut block_store = MemoryBlockStoreFixture::build().writer();

    let series_labels = up_api_labels();
    let fp = series_labels.fingerprint();
    let batch = up_api_sample_batch(&[(1_000, 1.0)]);
    let block_meta = block_store
        .writer()
        .write_block(
            "tenant-a",
            "metrics/float/0001.parquet",
            float_sample_schema(),
            &[batch],
        )
        .await
        .unwrap();
    block_store
        .index_mut()
        .add_series("tenant-a", fp, &series_labels);
    block_store.index_mut().add_block(&block_meta);

    assert_up_is_one(MetricBlockStore::new(block_store), series_labels).await;
}
