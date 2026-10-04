use krabka_metrics::{
    BucketSpan, NativeHistogram, NativeHistogramRow, ObjectStoreCompactionIndexSink, ResetHint,
    TenantCompactionRows, list_compaction_manifests, write_compacted_tenant_blocks,
};

use super::*;

fn histogram(count: f64) -> NativeHistogram {
    NativeHistogram {
        schema: 0,
        is_float: true,
        reset_hint: ResetHint::No,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count,
        sum: count,
        positive_spans: vec![BucketSpan {
            offset: 0,
            length: 1,
        }],
        positive_counts: vec![count],
        negative_spans: Vec::new(),
        negative_counts: Vec::new(),
        custom_values: None,
        start_timestamp_ms: None,
    }
}

/// A store whose only blocks hold native histograms, written the way the
/// compactor writes them, so no float block exists.
async fn histogram_only_store(series: &Labels) -> MetricBlockStore {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let rows = TenantCompactionRows {
        tenant: "tenant-a".to_string(),
        series_labels: std::collections::BTreeMap::from([(series.fingerprint(), series.clone())]),
        float_rows: Vec::new(),
        histogram_rows: [(30_000, 1.0), (60_000, 3.0), (90_000, 5.0)]
            .into_iter()
            .map(|(timestamp_ms, count)| NativeHistogramRow {
                fingerprint: series.fingerprint(),
                timestamp_ms,
                hist: histogram(count),
            })
            .collect(),
        exemplar_rows: Vec::new(),
        metadata_rows: Vec::new(),
        clock_rows: Vec::new(),
    };
    write_compacted_tenant_blocks(
        &krabka_blockstore::BlockWriter::new(Arc::clone(&object_store)),
        &ObjectStoreCompactionIndexSink::new(Arc::clone(&object_store)),
        &rows,
        0,
        0,
    )
    .await
    .unwrap();
    let manifests = list_compaction_manifests(&object_store).await.unwrap();
    let base = url::Url::parse("memory:///").unwrap();
    MetricBlockStore::from_compaction_manifests(
        BlockStore::new(Arc::clone(&object_store), base.clone()),
        Some(BlockStore::new(object_store, base)),
        &manifests,
    )
}

#[tokio::test]
pub(crate) async fn histogram_blocks_answer_count_over_time_and_rate() {
    let series = labels(&[("__name__", "request_duration_seconds"), ("job", "api")]);
    let store = histogram_only_store(&series).await;
    let matchers = [krabka_blockstore::LabelMatcher {
        name: "__name__".to_string(),
        op: krabka_blockstore::MatchOp::Eq,
        value: "request_duration_seconds".to_string(),
    }];

    // The store says where histograms can be, and says it from its index.
    for (start_ms, end_ms, expected) in [(0, 100_000, true), (100_001, 200_000, false)] {
        check!(
            store
                .may_have_histograms("tenant-a", &matchers, start_ms, end_ms)
                .await
                .unwrap()
                == expected,
            "[{start_ms}, {end_ms}]"
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let job = labels(&[("job", "api")]);
    for (query, expected) in [
        (
            "count_over_time(request_duration_seconds[2m])",
            SampleValue::Float(3.0),
        ),
        // The samples rise by 4 over 60s. Prometheus extrapolates the rise
        // by half a sample interval toward the window start, to 5 over the
        // 120s window.
        (
            "rate(request_duration_seconds[2m])",
            SampleValue::Histogram(NativeHistogram {
                reset_hint: ResetHint::Gauge,
                ..histogram(5.0 / 120.0)
            }),
        ),
    ] {
        let result = engine
            .query_instant(&tenant_id("tenant-a"), query, 90_000)
            .await
            .unwrap();
        check!(
            result
                == QueryResult::InstantVector(vec![InstantSample {
                    labels: job.clone(),
                    ts_ms: 90_000,
                    value: expected,
                    drop_name: false,
                }]),
            "{query}"
        );
    }
}
