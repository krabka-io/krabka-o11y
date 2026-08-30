use super::*;

#[tokio::test]
pub(crate) async fn prometheus_query_rebuilds_float_index_from_compaction_manifest() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let writer_store = BlockStore::new(object_store.clone(), base.clone());

    let series_labels = labels(&[("__name__", "up"), ("job", "api")]);
    let fp = series_labels.fingerprint();
    let batch = encode_float_samples(&[(fp, 1_000, 1.0)]).unwrap();
    let block_meta = writer_store
        .writer()
        .write_block(
            "tenant-a",
            "metrics/float/0001.parquet",
            float_sample_schema(),
            &[batch],
        )
        .await
        .unwrap();
    let manifest = CompactionIndexManifest::from_block_meta(
        MetricBlockKind::Float,
        &CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: "metrics/float/0001.index".to_string(),
            first_offset: 0,
            last_offset: 0,
            row_count: block_meta.row_count,
        },
        &block_meta,
        vec![CompactionSeriesLabels {
            fingerprint: fp,
            labels: series_labels.clone(),
        }],
    );

    let fresh_store = BlockStore::new(object_store, base);
    let store = MetricBlockStore::from_compaction_manifests(fresh_store, None, &[manifest]);
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
