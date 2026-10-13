use krabka_metrics::{
    NativeHistogram, NativeHistogramRow, ObjectStoreCompactionIndexSink, ResetHint,
    TenantCompactionRows, list_compaction_manifests, write_compacted_tenant_blocks,
};

use super::*;
use crate::test_support::nh;

struct HistogramPoint {
    timestamp_ms: i64,
    count: f64,
}

struct AvailabilityWindow {
    start_ms: i64,
    end_ms: i64,
    expected: bool,
}

struct HistogramQueryCase {
    query: &'static str,
    expected: SampleValue,
}

fn histogram(count: f64) -> NativeHistogram {
    nh(
        count,
        count,
        0,
        &[crate::test_support::BucketCount { index: 0, count }],
    )
}

/// A store whose only blocks hold native histograms, written the way the
/// compactor writes them, so no float block exists.
async fn histogram_only_store(series: &Labels) -> MetricBlockStore {
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let rows = compaction_rows_for_series(
        series,
        Vec::new(),
        [
            HistogramPoint {
                timestamp_ms: 30_000,
                count: 1.0,
            },
            HistogramPoint {
                timestamp_ms: 60_000,
                count: 3.0,
            },
            HistogramPoint {
                timestamp_ms: 90_000,
                count: 5.0,
            },
        ]
        .into_iter()
        .map(|point| NativeHistogramRow {
            fingerprint: series.fingerprint(),
            timestamp_ms: point.timestamp_ms,
            hist: histogram(point.count),
        })
        .collect(),
    );
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
    let series = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "request_duration_seconds"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    let store = histogram_only_store(&series).await;
    let matchers = [crate::PromqlMatcher {
        name: "__name__".to_string(),
        op: krabka_blockstore::MatchOp::Eq,
        value: "request_duration_seconds".to_string().into(),
    }];

    // The store says where histograms can be, and says it from its index.
    for window in [
        AvailabilityWindow {
            start_ms: 0,
            end_ms: 100_000,
            expected: true,
        },
        AvailabilityWindow {
            start_ms: 100_001,
            end_ms: 200_000,
            expected: false,
        },
    ] {
        check!(
            store
                .may_have_histograms("tenant-a", &matchers, window.start_ms, window.end_ms)
                .await
                .unwrap()
                == window.expected,
            "[{}, {}]",
            window.start_ms,
            window.end_ms
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let job = labels(&[crate::test_support::TestLabelPair::new("job", "api")]);
    for case in [
        HistogramQueryCase {
            query: "count_over_time(request_duration_seconds[2m])",
            expected: SampleValue::Float(3.0),
        },
        // The samples rise by 4 over 60s. Prometheus extrapolates the rise
        // by half a sample interval toward the window start, to 5 over the
        // 120s window.
        HistogramQueryCase {
            query: "rate(request_duration_seconds[2m])",
            expected: SampleValue::Histogram(NativeHistogram {
                reset_hint: ResetHint::Gauge,
                ..histogram(5.0 / 120.0)
            }),
        },
    ] {
        let result = engine
            .query_instant(&tenant_id("tenant-a"), case.query, 90_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("{}: expected vector", case.query);
        };
        assert2::assert!(samples.len() == 1, "{}", case.query);
        let expected_labels: crate::PromqlLabels = job.clone().into();
        assert2::assert!(samples[0].labels == expected_labels);
        assert2::assert!(samples[0].ts_ms == 90_000);
        assert2::assert!(samples[0].value == case.expected, "{}", case.query);
    }
}
