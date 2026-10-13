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

struct FloatRowCase {
    timestamp_ms: i64,
    value: f64,
}

struct TenantCompactionCase {
    tenant: &'static str,
    offset: i64,
    samples: Vec<FloatRowCase>,
}

struct ExpectedQueryCase {
    query: &'static str,
    value: f64,
}

struct TenantLedgerCase {
    tenant: &'static str,
    total: f64,
    delta: f64,
}

#[tokio::test]
pub(crate) async fn prometheus_query_rebuilds_float_index_from_compaction_manifest() {
    let series_labels = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    let batch = encode_float_samples(&[float_sample_row(
        series_labels.fingerprint(),
        1_000,
        1.0,
        None,
    )])
    .unwrap();
    let store = manifest_store(ManifestBlock {
        kind: MetricBlockKind::Float,
        block_key: "metrics/float/0001.parquet",
        schema: float_sample_schema(),
        batch,
        last_offset: 0,
        series_labels: series_labels.clone(),
    })
    .await;
    assert_up_is_one(store, series_labels).await;
}

#[tokio::test]
async fn composed_queries_survive_compaction_publication_deletion_and_reload() {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let series = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "requests_total"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    // The replay duplicates one timestamp. The foreign tenant deliberately has
    // the same fingerprint and timestamps, with different values.
    for case in [
        TenantCompactionCase {
            tenant: "tenant-a",
            offset: 1,
            samples: vec![
                FloatRowCase {
                    timestamp_ms: 30_000,
                    value: 2.0,
                },
                FloatRowCase {
                    timestamp_ms: 60_000,
                    value: 5.0,
                },
            ],
        },
        TenantCompactionCase {
            tenant: "tenant-a",
            offset: 3,
            samples: vec![
                FloatRowCase {
                    timestamp_ms: 60_000,
                    value: 5.0,
                },
                FloatRowCase {
                    timestamp_ms: 90_000,
                    value: 9.0,
                },
            ],
        },
        TenantCompactionCase {
            tenant: "tenant-b",
            offset: 5,
            samples: vec![
                FloatRowCase {
                    timestamp_ms: 30_000,
                    value: 100.0,
                },
                FloatRowCase {
                    timestamp_ms: 60_000,
                    value: 200.0,
                },
                FloatRowCase {
                    timestamp_ms: 90_000,
                    value: 300.0,
                },
            ],
        },
    ] {
        let mut series_labels_by_fingerprint = std::collections::BTreeMap::new();
        series_labels_by_fingerprint.insert(series.fingerprint(), series.clone().into());
        let rows = TenantCompactionRows {
            tenant: case.tenant.to_string(),
            series_labels: series_labels_by_fingerprint,
            float_rows: case
                .samples
                .into_iter()
                .map(|sample| FloatRow {
                    fingerprint: series.fingerprint(),
                    timestamp_ms: sample.timestamp_ms,
                    value: sample.value,
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
            case.offset,
            case.offset + 1,
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
        for tenant_case in [
            TenantLedgerCase {
                tenant: "tenant-a",
                total: if stage < 3 { 16.0 } else { 11.0 },
                delta: 7.0,
            },
            TenantLedgerCase {
                tenant: "tenant-b",
                total: 600.0,
                delta: 200.0,
            },
        ] {
            for query_case in [
                ExpectedQueryCase {
                    query: "sum(sum_over_time(requests_total[90s]))",
                    value: tenant_case.total,
                },
                ExpectedQueryCase {
                    query: r#"sum(sum_over_time({__name__="requests_total", job="api" or __name__="requests_total"}[90s]))"#,
                    value: tenant_case.total,
                },
                ExpectedQueryCase {
                    query: "sum(count_over_time(requests_total[90s]))",
                    value: if tenant_case.tenant == "tenant-a" && stage >= 3 {
                        2.0
                    } else {
                        3.0
                    },
                },
                ExpectedQueryCase {
                    query: "sum(idelta(requests_total[90s]))",
                    value: if tenant_case.tenant == "tenant-a" {
                        if stage < 3 { 4.0 } else { 7.0 }
                    } else {
                        100.0
                    },
                },
                ExpectedQueryCase {
                    query: "sum(max_over_time(requests_total[90s])-min_over_time(requests_total[90s]))",
                    value: tenant_case.delta,
                },
            ] {
                let actual = engine
                    .query_instant(&tenant_id(tenant_case.tenant), query_case.query, 90_000)
                    .await
                    .unwrap();
                assert2::assert!(
                    actual
                        == QueryResult::InstantVector(vec![InstantSample {
                            labels: Labels::new().into(),
                            ts_ms: 90_000,
                            value: SampleValue::Float(query_case.value),
                            drop_name: false
                        }]),
                    "stage={stage} tenant={} query={}",
                    tenant_case.tenant,
                    query_case.query
                );
            }
        }
    }
}
