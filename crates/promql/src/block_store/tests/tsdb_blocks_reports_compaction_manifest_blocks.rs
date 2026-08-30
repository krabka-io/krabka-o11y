use super::*;

#[tokio::test]
pub(crate) async fn tsdb_blocks_reports_compaction_manifest_blocks() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let writer_store = BlockStore::new(object_store.clone(), base.clone());

    let series_labels = labels(&[("__name__", "up"), ("job", "api")]);
    let fp = series_labels.fingerprint();
    let batch = encode_float_samples(&[(fp, 1_000, 1.0), (fp, 2_000, 0.0)]).unwrap();
    let block_meta = writer_store
        .writer()
        .write_block(
            "tenant-a",
            "metrics/float/0002.parquet",
            float_sample_schema(),
            &[batch],
        )
        .await
        .unwrap();
    let manifest = CompactionIndexManifest::from_block_meta(
        MetricBlockKind::Float,
        &CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: "metrics/float/0002.index".to_string(),
            first_offset: 0,
            last_offset: 1,
            row_count: block_meta.row_count,
        },
        &block_meta,
        vec![CompactionSeriesLabels {
            fingerprint: fp,
            labels: series_labels,
        }],
    );

    let fresh_store = BlockStore::new(object_store, base);
    let store = MetricBlockStore::from_compaction_manifests(fresh_store, None, &[manifest]);
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
