use super::*;

#[tokio::test]
pub(crate) async fn prometheus_query_reads_float_samples_from_blockstore() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let mut block_store = BlockStore::new(object_store, base);

    let series_labels = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    let fp = series_labels.fingerprint();
    let batch = encode_float_samples(&[float_sample_row(fp, 1_000, 1.0, None)]).unwrap();
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
