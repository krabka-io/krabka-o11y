use super::*;
use crate::PromqlMatcher as LabelMatcher;

#[tokio::test]
pub(crate) async fn range_query_scans_store_once_per_matcher_set_not_per_step() {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::{
        error::Result,
        store::{
            ExemplarScan, LabelNameCardinality, LabelValueCardinality, MetadataScan, MetricStore,
            ScanResult, TsdbBlock, TsdbStats,
        },
    };

    // Wraps the in-memory store and counts store-level scans / series
    // resolutions, to prove the range driver no longer re-scans per step.
    struct CountingStore {
        inner: InMemoryMetricStore,
        scans: Arc<AtomicUsize>,
        series_calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl MetricStore for CountingStore {
        async fn scan(&self, t: &str, m: &[LabelMatcher], s: i64, e: i64) -> Result<ScanResult> {
            self.scans.fetch_add(1, Ordering::SeqCst);
            self.inner.scan(t, m, s, e).await
        }
        async fn series(
            &self,
            t: &str,
            m: &[LabelMatcher],
            s: i64,
            e: i64,
        ) -> Result<Vec<crate::PromqlLabels>> {
            self.series_calls.fetch_add(1, Ordering::SeqCst);
            self.inner.series(t, m, s, e).await
        }
        async fn label_names(
            &self,
            t: &str,
            m: &[LabelMatcher],
            s: i64,
            e: i64,
        ) -> Result<Vec<String>> {
            self.inner.label_names(t, m, s, e).await
        }
        async fn label_values(
            &self,
            t: &str,
            name: &str,
            m: &[LabelMatcher],
            s: i64,
            e: i64,
        ) -> Result<Vec<krabka_metrics::MetricString>> {
            self.inner.label_values(t, name, m, s, e).await
        }
        async fn exemplars(
            &self,
            t: &str,
            m: &[LabelMatcher],
            s: i64,
            e: i64,
        ) -> Result<ExemplarScan> {
            self.inner.exemplars(t, m, s, e).await
        }
        async fn metadata(&self, t: &str, metric: Option<&str>) -> Result<MetadataScan> {
            self.inner.metadata(t, metric).await
        }
        async fn cardinality_label_names(&self, t: &str) -> Result<Vec<LabelNameCardinality>> {
            self.inner.cardinality_label_names(t).await
        }
        async fn cardinality_label_values(&self, t: &str) -> Result<Vec<LabelValueCardinality>> {
            self.inner.cardinality_label_values(t).await
        }
        async fn cardinality_active_series(&self, t: &str) -> Result<Vec<crate::PromqlLabels>> {
            self.inner.cardinality_active_series(t).await
        }
        async fn tsdb_stats(&self, t: &str) -> Result<TsdbStats> {
            self.inner.tsdb_stats(t).await
        }
        async fn tsdb_blocks(&self, t: &str) -> Result<Vec<TsdbBlock>> {
            self.inner.tsdb_blocks(t).await
        }
    }

    let mut inner = InMemoryMetricStore::new();
    for i in 0..20 {
        inner.push_float(
            "t",
            labels(&[("__name__", "up"), ("job", "broker")]),
            i * 15_000,
            1.0,
        );
    }
    let scans = Arc::new(AtomicUsize::new(0));
    let series_calls = Arc::new(AtomicUsize::new(0));
    let engine = PromqlEngine::new(
        Arc::new(CountingStore {
            inner,
            scans: Arc::clone(&scans),
            series_calls: Arc::clone(&series_calls),
        }),
        EngineOpts::default(),
    );

    // 20 steps at 15s. Without the union-window cache the driver scans the
    // store about twice per step (float and histogram probe) and resolves the
    // series once per step. With it, one store scan answers the histogram
    // probe and the float read of every step, and one series resolution
    // answers every step.
    let result = engine
        .eval_range_via_planner_forced(
            "t",
            "count({job=\"broker\"})",
            0,
            19 * 15_000,
            millis(15_000),
        )
        .await
        .unwrap();
    assert2::assert!(matches!(result, QueryResult::RangeMatrix(_)));
    check!(
        scans.load(Ordering::SeqCst) == 1,
        "store scans should collapse to one union scan, got {}",
        scans.load(Ordering::SeqCst)
    );
    check!(
        series_calls.load(Ordering::SeqCst) == 1,
        "series resolution should be cached across steps, got {}",
        series_calls.load(Ordering::SeqCst)
    );
}
