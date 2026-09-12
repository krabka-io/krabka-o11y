use super::*;

#[tokio::test]
pub(crate) async fn metadata_answers_around_a_deleted_block_and_warns() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let writer_store = BlockStore::new(object_store.clone(), base.clone());

    // One metric family, described by two blocks. The index names both, and one
    // of the two objects is gone.
    let series = labels(&[("__name__", "http_requests_total")]);
    let deleted = sidecar_manifest(
        &writer_store,
        MetricBlockKind::Metadata,
        "metrics/metadata/0001.parquet",
        metadata_schema(),
        metadata_batch(
            series.fingerprint(),
            "http_requests_total",
            "counter",
            "Requests served, before the help text was corrected.",
            "requests",
        ),
        &series,
    )
    .await;
    let kept = sidecar_manifest(
        &writer_store,
        MetricBlockKind::Metadata,
        "metrics/metadata/0002.parquet",
        metadata_schema(),
        metadata_batch(
            series.fingerprint(),
            "http_requests_total",
            "counter",
            "Total HTTP requests.",
            "requests",
        ),
        &series,
    )
    .await;
    object_store
        .delete(&ObjectPath::from("metrics/metadata/0001.parquet"))
        .await
        .unwrap();

    let store = MetricBlockStore::from_compaction_manifests(
        BlockStore::new(object_store, base),
        None,
        &[deleted, kept],
    );
    let scan = store
        .metadata("tenant-a", Some("http_requests_total"))
        .await
        .unwrap();

    assert2::assert!(
        scan.metadata
            == vec![MetadataRecord {
                metric_family_name: "http_requests_total".to_string(),
                metric_type: "counter".to_string(),
                help: "Total HTTP requests.".to_string(),
                unit: "requests".to_string(),
            }]
    );
    check!(scan.warnings.len() == 1);
    check!(scan.warnings[0].contains("metrics/metadata/0001.parquet"));
    check!(scan.warnings[0].contains("missing"));
}
