use krabka_blockstore::{
    BlockLevel, BlockTimestampUnit, BlockWriter, CompactionPolicy, DEFAULT_BLOCK_READ_MAX,
    ERASURE_REQUEST_PREFIX, ErasureRequest, LabelMatcher, MatchOp, put_erasure_request,
};
use krabka_metrics::{
    DeferredBlockDeletions, FloatRow, ObjectStoreCompactionIndexSink, TenantCompactionRows,
    compact_metric_blocks_once, list_compaction_manifests, write_compacted_tenant_blocks,
};
use krabka_units::hours;

use super::*;

#[tokio::test]
pub(crate) async fn prometheus_query_rebuilds_float_index_from_compaction_manifest() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let base = url::Url::parse("memory:///").unwrap();
    let writer_store = BlockStore::new(object_store.clone(), base.clone());

    let series_labels = labels(&[("__name__", "up"), ("job", "api")]);
    let fp = series_labels.fingerprint();
    let batch = encode_float_samples(&[(fp, 1_000, 1.0, None)]).unwrap();
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
    let result = engine
        .query_instant(&tenant_id("tenant-a"), "up", 1_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected instant vector");
    };
    assert2::assert!(
        samples
            == vec![InstantSample {
                labels: series_labels,
                ts_ms: 1_000,
                value: SampleValue::Float(1.0),
                drop_name: false,
            }]
    );
}

#[tokio::test]
async fn composed_queries_survive_compaction_publication_deletion_and_reload() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let series = labels(&[("__name__", "requests_total"), ("job", "api")]);
    // The replay duplicates one timestamp. The foreign tenant deliberately has
    // the same fingerprint and timestamps, with different values.
    for (tenant, offset, samples) in [
        ("tenant-a", 1, vec![(30_000, 2.0), (60_000, 5.0)]),
        ("tenant-a", 3, vec![(60_000, 5.0), (90_000, 9.0)]),
        (
            "tenant-b",
            5,
            vec![(30_000, 100.0), (60_000, 200.0), (90_000, 300.0)],
        ),
    ] {
        let rows = TenantCompactionRows {
            tenant: tenant.to_string(),
            series_labels: std::collections::BTreeMap::from([(
                series.fingerprint(),
                series.clone(),
            )]),
            float_rows: samples
                .into_iter()
                .map(|(timestamp_ms, value)| FloatRow {
                    fingerprint: series.fingerprint(),
                    timestamp_ms,
                    value,
                    start_timestamp_ms: None,
                })
                .collect(),
            histogram_rows: Vec::new(),
            exemplar_rows: Vec::new(),
            metadata_rows: Vec::new(),
            clock_rows: Vec::new(),
        };
        write_compacted_tenant_blocks(
            &BlockWriter::new(object_store.clone()),
            &ObjectStoreCompactionIndexSink::new(object_store.clone()),
            &rows,
            offset,
            offset + 1,
        )
        .await
        .unwrap();
    }
    let policy = CompactionPolicy::new(
        8,
        1_000_000,
        BlockLevel(4),
        hours(24),
        BlockTimestampUnit::Millis,
    );
    let mut deferred = DeferredBlockDeletions::new();
    let mut retired = Vec::new();
    for stage in 0..5 {
        if stage == 3 {
            let request = ErasureRequest::new(
                "tenant-a",
                "requests_total",
                vec![vec![LabelMatcher::new(
                    "__name__",
                    MatchOp::Eq,
                    "requests_total",
                )]],
                60_000_000_000,
                60_000_000_000,
                90_000_000_000,
            );
            put_erasure_request(&object_store, ERASURE_REQUEST_PREFIX, &request)
                .await
                .unwrap();
        }
        if stage > 0 {
            compact_metric_blocks_once(
                &object_store,
                &BlockWriter::new(object_store.clone()),
                &ObjectStoreCompactionIndexSink::new(object_store.clone()),
                policy,
                DEFAULT_BLOCK_READ_MAX,
                &mut deferred,
            )
            .await
            .unwrap();
            if stage == 1 {
                retired = deferred.keys().to_vec();
                assert2::assert!(retired.len() == 2);
            }
            if stage == 3 {
                assert2::assert!(deferred.keys().len() == 1);
            }
            if stage == 4 {
                assert2::assert!(deferred.is_empty());
            }
            for key in &retired {
                assert2::assert!(
                    object_store
                        .head(&ObjectPath::from(key.clone()))
                        .await
                        .is_ok()
                        == (stage == 1)
                );
            }
        }
        let manifests = list_compaction_manifests(&object_store).await.unwrap();
        let cold = MetricBlockStore::from_compaction_manifests(
            BlockStore::new(object_store.clone(), url::Url::parse("memory:///").unwrap()),
            None,
            &manifests,
        );
        let engine = PromqlEngine::new(Arc::new(cold), EngineOpts::default());
        // These constants come from the independent three-point ledger, rather
        // than a previous execution of the query engine.
        for (tenant, total, delta) in [
            ("tenant-a", if stage < 3 { 16.0 } else { 11.0 }, 7.0),
            ("tenant-b", 600.0, 200.0),
        ] {
            for (query, expected) in [
                ("sum(sum_over_time(requests_total[90s]))", total),
                (
                    r#"sum(sum_over_time({__name__="requests_total", job="api" or __name__="requests_total"}[90s]))"#,
                    total,
                ),
                (
                    "sum(count_over_time(requests_total[90s]))",
                    if tenant == "tenant-a" && stage >= 3 {
                        2.0
                    } else {
                        3.0
                    },
                ),
                (
                    "sum(idelta(requests_total[90s]))",
                    if tenant == "tenant-a" {
                        if stage < 3 { 4.0 } else { 7.0 }
                    } else {
                        100.0
                    },
                ),
                (
                    "sum(max_over_time(requests_total[90s])-min_over_time(requests_total[90s]))",
                    delta,
                ),
            ] {
                let actual = engine
                    .query_instant(&tenant_id(tenant), query, 90_000)
                    .await
                    .unwrap();
                assert2::assert!(
                    actual
                        == QueryResult::InstantVector(vec![InstantSample {
                            labels: Labels::new(),
                            ts_ms: 90_000,
                            value: SampleValue::Float(expected),
                            drop_name: false
                        }]),
                    "stage={stage} tenant={tenant} query={query}"
                );
            }
        }
    }
}
