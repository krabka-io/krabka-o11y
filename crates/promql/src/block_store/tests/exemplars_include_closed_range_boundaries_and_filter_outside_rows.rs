use super::*;

#[tokio::test]
pub(crate) async fn exemplars_include_closed_range_boundaries_and_filter_outside_rows() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let writer_store = BlockStore::new(object_store.clone(), base.clone());

    let series_labels = labels(&[("__name__", "http_requests_total"), ("job", "api")]);
    let fp = series_labels.fingerprint();
    let batch = exemplar_batch_from_rows(&[
        (fp, 9_999, 1.0, "too-low", "s1", "kind", "outside"),
        (fp, 10_000, 2.0, "start", "s2", "kind", "inside"),
        (fp, 11_000, 3.0, "end", "s3", "kind", "inside"),
        (fp, 11_001, 4.0, "too-high", "s4", "kind", "outside"),
    ]);
    let block_meta = writer_store
        .writer()
        .write_block(
            "tenant-a",
            "metrics/exemplars/0005.parquet",
            exemplar_schema(),
            &[batch],
        )
        .await
        .unwrap();
    let manifest = CompactionIndexManifest::from_block_meta(
        MetricBlockKind::Exemplars,
        &CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: "metrics/exemplars/0005.index".to_string(),
            first_offset: 0,
            last_offset: 3,
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

    check!(exemplars.len() == 2);
    for (row, trace_id, span_id, ts_ms, value) in [
        (0_usize, "start", "s2", 10_000_i64, 2.0_f64),
        (1, "end", "s3", 11_000, 3.0),
    ] {
        check!(exemplars[row].series_labels == series_labels, "row {row}");
        check!(
            exemplars[row].labels.get("trace_id") == Some(trace_id),
            "row {row}"
        );
        check!(
            exemplars[row].labels.get("span_id") == Some(span_id),
            "row {row}"
        );
        check!(
            exemplars[row].labels.get("kind") == Some("inside"),
            "row {row}"
        );
        check!(exemplars[row].ts_ms == ts_ms, "row {row}");
        check!(
            exemplars[row].value.to_bits() == value.to_bits(),
            "row {row}"
        );
    }
}
