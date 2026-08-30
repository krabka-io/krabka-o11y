use super::*;

#[tokio::test]
pub(crate) async fn prometheus_query_reads_float_samples_from_blockstore() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let mut block_store = BlockStore::new(object_store, base);

    let series_labels = labels(&[("__name__", "up"), ("job", "api")]);
    let fp = series_labels.fingerprint();
    let batch = encode_float_samples(&[(fp, 1_000, 1.0)]).unwrap();
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

    let store = MetricBlockStore::new(block_store);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine.query_instant("tenant-a", "up", 1_000).await.unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected instant vector");
    };
    assert2::assert!(
        samples
            == vec![InstantSample {
                labels: series_labels,
                ts_ms: 1_000,
                value: SampleValue::Float(1.0),
            }]
    );
}
