use super::*;

#[tokio::test]
pub(crate) async fn metadata_reads_compacted_metadata_sidecar_blocks() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let writer_store = BlockStore::new(object_store.clone(), base.clone());

    let series_labels = labels(&[("__name__", "http_requests_total")]);
    let fp = series_labels.fingerprint();
    let batch = metadata_batch(
        fp,
        "http_requests_total",
        "counter",
        "Total HTTP requests.",
        "requests",
    );
    let block_meta = writer_store
        .writer()
        .write_block(
            "tenant-a",
            "metrics/metadata/0004.parquet",
            metadata_schema(),
            &[batch],
        )
        .await
        .unwrap();
    let manifest = CompactionIndexManifest::from_block_meta(
        MetricBlockKind::Metadata,
        &CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: "metrics/metadata/0004.index".to_string(),
            first_offset: 0,
            last_offset: 0,
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
    let metadata = store
        .metadata("tenant-a", Some("http_requests_total"))
        .await
        .unwrap();

    assert2::assert!(
        metadata
            == vec![MetadataRecord {
                metric_family_name: "http_requests_total".to_string(),
                metric_type: "counter".to_string(),
                help: "Total HTTP requests.".to_string(),
                unit: "requests".to_string(),
            }]
    );
}
