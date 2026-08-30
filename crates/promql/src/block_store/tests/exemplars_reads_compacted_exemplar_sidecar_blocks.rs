use super::*;

#[tokio::test]
pub(crate) async fn exemplars_reads_compacted_exemplar_sidecar_blocks() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let writer_store = BlockStore::new(object_store.clone(), base.clone());

    let series_labels = labels(&[("__name__", "http_requests_total"), ("job", "api")]);
    let fp = series_labels.fingerprint();
    let batch = exemplar_batch(fp, 10_500, 7.0, "abc", "def", "kind", "slow");
    let block_meta = writer_store
        .writer()
        .write_block(
            "tenant-a",
            "metrics/exemplars/0003.parquet",
            exemplar_schema(),
            &[batch],
        )
        .await
        .unwrap();
    let manifest = CompactionIndexManifest::from_block_meta(
        MetricBlockKind::Exemplars,
        &CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: "metrics/exemplars/0003.index".to_string(),
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
    let exemplars = store
        .exemplars(
            "tenant-a",
            &[krabka_blockstore::LabelMatcher {
                name: "job".to_string(),
                op: krabka_blockstore::MatchOp::Eq,
                value: "api".to_string(),
            }],
            10_000,
            11_000,
        )
        .await
        .unwrap();

    check!(exemplars.len() == 1);
    check!(exemplars[0].series_labels == series_labels);
    check!(exemplars[0].labels.get("trace_id") == Some("abc"));
    check!(exemplars[0].labels.get("span_id") == Some("def"));
    check!(exemplars[0].labels.get("kind") == Some("slow"));
    check!(exemplars[0].ts_ms == 10_500);
    check!((exemplars[0].value - 7.0).abs() < f64::EPSILON);
}
