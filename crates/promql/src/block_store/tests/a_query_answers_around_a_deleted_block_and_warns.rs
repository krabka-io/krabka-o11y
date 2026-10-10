use super::*;

#[tokio::test]
pub(crate) async fn a_query_answers_around_a_deleted_block_and_warns() {
    let (block_store, kept_series) = deleted_api_block_store().await;

    let store = MetricBlockStore::new(block_store);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let (result, annotations) = engine
        .query_instant_with_annotations(&tenant_id("tenant-a"), "up", 1_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected instant vector");
    };
    assert2::assert!(
        samples
            == vec![InstantSample {
                labels: kept_series.into(),
                ts_ms: 1_000,
                value: SampleValue::Float(2.0),
                drop_name: false,
            }]
    );
    check!(annotations.warnings.len() == 1);
    check!(annotations.warnings[0].contains("metrics/float/0001.parquet"));
    check!(annotations.warnings[0].contains("missing"));
    check!(annotations.infos.is_empty());
}

#[tokio::test]
async fn byte_series_selection_precedes_missing_block_warnings_for_every_kind() {
    use krabka_metrics::{
        ExemplarRow, FloatRow, MetadataRow, NativeHistogramRow, ObjectStoreCompactionIndexSink,
        TenantCompactionRows, write_compacted_tenant_blocks,
    };
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let writer_store = BlockStore::new(object_store.clone(), base.clone());
    let sink = ObjectStoreCompactionIndexSink::new(object_store.clone());
    let mut manifests = Vec::new();
    for (offset, raw) in [(0, 0xff), (1, 0xfe)] {
        let mut series = crate::PromqlLabels::from_pairs([("__name__", "byte_input")]);
        series.insert("raw", crate::PromqlString::from(vec![raw]));
        let fingerprint = series.fingerprint();
        let histogram = krabka_metrics::NativeHistogram {
            schema: 0,
            is_float: false,
            reset_hint: krabka_metrics::ResetHint::No,
            zero_threshold: 0.0,
            zero_count: 1.0,
            count: 1.0,
            sum: 0.0,
            positive_spans: Vec::new(),
            positive_counts: Vec::new(),
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: None,
            start_timestamp_ms: None,
        };
        let rows = TenantCompactionRows {
            tenant: "tenant-a".into(),
            series_labels: std::collections::BTreeMap::from([(fingerprint, series)]),
            float_rows: vec![FloatRow {
                fingerprint,
                timestamp_ms: 1_000,
                value: 2.0,
                start_timestamp_ms: None,
            }],
            histogram_rows: vec![NativeHistogramRow {
                fingerprint,
                timestamp_ms: 1_000,
                hist: histogram,
            }],
            exemplar_rows: vec![ExemplarRow {
                fingerprint,
                timestamp_ms: 1_000,
                value: 2.0,
                trace_id: Some("abc".into()),
                span_id: None,
                labels: Vec::new(),
            }],
            metadata_rows: vec![MetadataRow {
                fingerprint,
                metric_family_name: "byte_input".into(),
                metric_type: "gauge".into(),
                help: "fixture".into(),
                unit: String::new(),
            }],
            clock_rows: Vec::new(),
        };
        manifests.extend(
            write_compacted_tenant_blocks(&writer_store.writer(), &sink, &rows, offset, offset)
                .await
                .unwrap()
                .into_iter()
                .map(|written| written.manifest),
        );
    }
    for manifest in &manifests {
        if manifest.first_offset == 1 {
            object_store
                .delete(&ObjectPath::from(manifest.block_key.as_str()))
                .await
                .unwrap();
        }
    }
    let store = MetricBlockStore::from_compaction_manifests(
        BlockStore::new(object_store.clone(), base.clone()),
        Some(BlockStore::new(object_store.clone(), base)),
        &manifests,
    );
    let selected = [crate::PromqlMatcher::new(
        "raw",
        krabka_blockstore::MatchOp::Eq,
        crate::PromqlString::from(vec![0xff]),
    )];
    let scan = store.scan("tenant-a", &selected, 0, 2_000).await.unwrap();
    assert2::assert!(
        scan.float_table.is_some() && scan.histogram_table.is_some() && scan.warnings.is_empty()
    );
    let all = store.scan("tenant-a", &[], 0, 2_000).await.unwrap();
    assert2::assert!(all.warnings.len() == 2);
    let exemplars = store
        .exemplars("tenant-a", &selected, 0, 2_000)
        .await
        .unwrap();
    assert2::assert!(
        exemplars.warnings.is_empty()
            && exemplars.exemplars.len() == 1
            && exemplars.exemplars[0].value.to_bits() == 2.0_f64.to_bits()
    );
    let all_exemplars = store.exemplars("tenant-a", &[], 0, 2_000).await.unwrap();
    assert2::assert!(all_exemplars.warnings.len() == 1 && all_exemplars.exemplars.len() == 1);
    let other = store
        .metadata("tenant-a", Some("absent_metric"))
        .await
        .unwrap();
    assert2::assert!(other.warnings.is_empty() && other.metadata.is_empty());
    let matching = store
        .metadata("tenant-a", Some("byte_input"))
        .await
        .unwrap();
    assert2::assert!(matching.warnings.len() == 1 && matching.metadata.len() == 1);
}
