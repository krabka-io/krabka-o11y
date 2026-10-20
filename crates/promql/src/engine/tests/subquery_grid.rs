use std::sync::atomic::{AtomicUsize, Ordering};

use super::super::subquery_grid::SUBQUERY_GRID_DISABLED;
use super::super::{PerStepSampleStats, collect_query_sample_stats};
use super::*;
use crate::{
    PromqlMatcher as LabelMatcher,
    error::Result,
    store::{
        ExemplarScan, LabelNameCardinality, LabelValueCardinality, MetadataScan, ScanResult,
        TsdbBlock, TsdbStats,
    },
};

fn counting_engine(opts: EngineOpts) -> (PromqlEngine<CountingStore>, Arc<AtomicUsize>) {
    let mut inner = InMemoryMetricStore::new();
    for (job, timestamp) in [("early", 3_600_000), ("late", 4_200_000)] {
        inner.push_float(
            "t",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "sparse"),
                crate::test_support::TestLabelPair::new("job", job),
            ]),
            timestamp,
            1.0,
        );
    }
    for i in 0..=600 {
        inner.push_float(
            "t",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
                crate::test_support::TestLabelPair::new("__type__", "counter"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]),
            i * 15_000,
            f64::from(i32::try_from(i).unwrap()),
        );
        inner.push_float(
            "t",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "gauge")]),
            i * 15_000,
            if i == 270 {
                stale_nan()
            } else {
                f64::from(i32::try_from(i % 11).unwrap())
            },
        );
    }
    let scans = Arc::new(AtomicUsize::new(0));
    let engine = PromqlEngine::new(
        Arc::new(CountingStore {
            inner,
            scans: Arc::clone(&scans),
            series_calls: Arc::new(AtomicUsize::new(0)),
        }),
        opts,
    );
    (engine, scans)
}

#[tokio::test]
async fn overlapping_subquery_grids_scan_once_as_steps_and_windows_vary() {
    for steps in [2, 10, 30] {
        for window in ["5m", "1h"] {
            let (engine, scans) = counting_engine(EngineOpts::default());
            let query = format!("max_over_time(rate(http_requests_total[5m])[{window}:15s])");
            let result = engine
                .query_range(
                    &tenant_id("t"),
                    &query,
                    3_600_000,
                    3_600_000 + (steps - 1) * 15_000,
                    secs(15),
                )
                .await
                .unwrap();
            let QueryResult::RangeMatrix(series) = result else {
                panic!("expected matrix")
            };
            assert2::assert!(series.len() == 1);
            assert2::assert!(series[0].samples.len() == usize::try_from(steps).unwrap());
            assert2::assert!(scans.load(Ordering::SeqCst) == 1);
        }
    }
}

#[tokio::test]
async fn subquery_union_reuses_grids_with_stable_annotations() {
    let (engine, scans) = counting_engine(EngineOpts::default());
    let result = engine
        .query_range_with_annotations(
            &tenant_id("t"),
            "max_over_time(rate(gauge[5m])[5m:15s])",
            3_600_000,
            4_035_000,
            secs(15),
        )
        .await
        .unwrap();
    assert2::assert!(result.annotations.infos.len() == 1);
    assert2::assert!(scans.load(Ordering::SeqCst) == 2);
}

#[tokio::test]
async fn subquery_union_matches_uncached_windows_and_annotations() {
    let (engine, _) = counting_engine(EngineOpts::default());
    for query in [
        "max_over_time(rate(http_requests_total[5m])[1h:15s])",
        "sum_over_time(gauge[5m:15s])",
        "last_over_time(sparse[1m:15s])",
        "sum_over_time(gauge[5m:])",
        "sum_over_time(gauge[5m:15s] offset 3m)",
        "sum_over_time(gauge[5m:15s] offset -3m)",
        "sum_over_time(gauge[5m:15s] @ start())",
        "sum_over_time(gauge[5m:15s] @ end())",
        "sum_over_time((gauge @ start())[5m:15s])",
        "sum_over_time((gauge @ end())[5m:15s])",
        "sum_over_time((sum(gauge))[5m:15s])",
        "sum_over_time((gauge + 2)[5m:15s])",
        "sum_over_time((-gauge)[5m:15s])",
        "quantile_over_time(time() / 10000, gauge[5m:15s])",
        "sum_over_time((vector(time()))[5m:15s])",
        #[cfg(feature = "experimental-functions")]
        "sum_over_time((vector(step() + range() + start() + end()))[5m:15s])",
        "max_over_time(sum_over_time(gauge[1m:15s])[5m:30s])",
        "sum_over_time(gauge[5m:15s]) + sum_over_time(gauge[10m:30s])",
        "max_over_time(rate(gauge[1m])[5m:15s])",
        "sum_over_time((quantile_over_time(2, gauge[1m]))[5m:15s])",
        "sum_over_time((quantile_over_time(time() / 1000, gauge[1m]))[5m:15s])",
    ] {
        let cached = engine
            .query_range_with_annotations(&tenant_id("t"), query, 3_907_000, 4_253_000, secs(37))
            .await
            .unwrap();
        let uncached = SUBQUERY_GRID_DISABLED
            .scope(
                (),
                engine.query_range_with_annotations(
                    &tenant_id("t"),
                    query,
                    3_907_000,
                    4_253_000,
                    secs(37),
                ),
            )
            .await
            .unwrap();
        assert2::assert!(cached == uncached, "{query}");
    }
}

#[tokio::test]
async fn union_limit_failure_falls_back_to_valid_windows() {
    for opts in [
        EngineOpts {
            max_samples: 25,
            ..EngineOpts::default()
        },
        EngineOpts {
            max_fetched_series: 1,
            ..EngineOpts::default()
        },
    ] {
        let (engine, _) = counting_engine(opts);
        let query = "sum_over_time(gauge[1m:15s])";
        let cached = engine
            .query_range(&tenant_id("t"), query, 3_600_000, 4_800_000, secs(60))
            .await;
        let uncached = SUBQUERY_GRID_DISABLED
            .scope(
                (),
                engine.query_range(&tenant_id("t"), query, 3_600_000, 4_800_000, secs(60)),
            )
            .await;
        assert2::assert!(cached.unwrap() == uncached.unwrap());
    }
    let (engine, _) = counting_engine(EngineOpts {
        max_samples: 1,
        ..EngineOpts::default()
    });
    assert2::assert!(
        engine
            .query_range(
                &tenant_id("t"),
                "sum_over_time(gauge[1m:15s])",
                3_600_000,
                3_660_000,
                secs(60)
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn union_series_limit_does_not_reject_disjoint_series_in_valid_windows() {
    let mut store = InMemoryMetricStore::new();
    for (job, timestamp) in [("early", 1_000_000), ("late", 2_000_000)] {
        store.push_float(
            "t",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "g"),
                crate::test_support::TestLabelPair::new("job", job),
            ]),
            timestamp,
            1.0,
        );
    }
    let engine = PromqlEngine::new(
        Arc::new(store),
        EngineOpts {
            max_fetched_series: 1,
            ..EngineOpts::default()
        },
    );
    let query = "last_over_time(g[1m:15s])";
    let cached = engine
        .query_range(&tenant_id("t"), query, 1_000_000, 2_300_000, secs(60))
        .await
        .unwrap();
    let uncached = SUBQUERY_GRID_DISABLED
        .scope(
            (),
            engine.query_range(&tenant_id("t"), query, 1_000_000, 2_300_000, secs(60)),
        )
        .await
        .unwrap();
    assert2::assert!(cached == uncached);
    let QueryResult::RangeMatrix(series) = cached else {
        panic!("expected matrix")
    };
    assert2::assert!(series.len() == 2);
}

#[tokio::test]
async fn subquery_union_resolution_cap_does_not_cap_individual_windows() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_range(
            &tenant_id("t"),
            "sum_over_time((vector(time()))[10s:1ms])",
            0,
            2_000,
            secs(1),
        )
        .await
        .unwrap();
    let QueryResult::RangeMatrix(series) = result else {
        panic!("expected matrix")
    };
    assert2::assert!(series[0].samples.len() == 3);
    assert2::assert!(
        engine
            .query_range(
                &tenant_id("t"),
                "sum_over_time((vector(time()))[12s:1ms])",
                0,
                2_000,
                secs(1)
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn subquery_sample_statistics_preserve_logical_evaluations() {
    let (engine, scans) = counting_engine(EngineOpts::default());
    let query = "max_over_time(rate(http_requests_total[5m])[5m:15s])";
    let tenant = tenant_id("t");
    let run = || {
        collect_query_sample_stats(
            PerStepSampleStats::Include(crate::planner::StepGrid {
                start: 3_600_000,
                end: 3_660_000,
                step: 15_000,
            }),
            engine.query_range(&tenant, query, 3_600_000, 3_660_000, secs(15)),
        )
    };
    let actual = run().await;
    let expected = SUBQUERY_GRID_DISABLED.scope((), run()).await;
    assert2::assert!(actual.output.unwrap() == expected.output.unwrap());
    assert2::assert!(actual.stats == expected.stats);
    assert2::assert!(actual.stats.total_queryable_samples > 0);
    assert2::assert!(scans.load(Ordering::SeqCst) > 2);
}

#[tokio::test]
async fn bounded_subquery_materialization_declines_before_retaining_too_many_points() {
    let (engine, _) = counting_engine(EngineOpts::default());
    let expr = crate::parse_promql("gauge").unwrap();
    let grid = crate::planner::StepGrid {
        start: 3_600_000,
        end: 3_660_000,
        step: 15_000,
    };
    assert2::assert!(
        engine
            .eval_range_via_planner_bounded("t", &expr, grid, 4)
            .await
            .unwrap()
            .is_none()
    );
    let series = engine
        .eval_range_via_planner_bounded("t", &expr, grid, 5)
        .await
        .unwrap()
        .unwrap();
    assert2::assert!(series[0].samples.len() == 5);
}

#[tokio::test]
async fn subquery_cache_does_not_cross_query_or_tenant_boundaries() {
    let (engine, _) = counting_engine(EngineOpts::default());
    let query = "sum_over_time(gauge[5m:15s])";
    let populated = engine
        .query_range(&tenant_id("t"), query, 3_600_000, 3_660_000, secs(15))
        .await
        .unwrap();
    let empty = engine
        .query_range(&tenant_id("other"), query, 3_600_000, 3_660_000, secs(15))
        .await
        .unwrap();
    assert2::assert!(empty == QueryResult::RangeMatrix(Vec::new()));
    assert2::assert!(populated != empty);
}

#[tokio::test]
async fn subquery_union_preserves_histograms() {
    let engine = PromqlEngine::new(Arc::new(native_histogram_store()), EngineOpts::default());
    for query in [
        "last_over_time(request_duration_seconds[1m:15s])",
        "sum_over_time(request_duration_seconds[1m:15s])",
        "max_over_time(histogram_sum(request_duration_seconds)[1m:15s])",
    ] {
        let cached = engine
            .query_range_with_annotations(&tenant_id("tenant-a"), query, 15_000, 180_000, secs(15))
            .await
            .unwrap();
        let uncached = SUBQUERY_GRID_DISABLED
            .scope(
                (),
                engine.query_range_with_annotations(
                    &tenant_id("tenant-a"),
                    query,
                    15_000,
                    180_000,
                    secs(15),
                ),
            )
            .await
            .unwrap();
        assert2::assert!(cached == uncached, "{query}");
    }
}

#[tokio::test]
async fn overlapping_subquery_range_matches_interpreter_at_every_outer_step() {
    let (engine, _) = counting_engine(EngineOpts::default());
    let start_ms = 3_907_000;
    let end_ms = 4_253_000;
    for query in [
        "max_over_time(rate(http_requests_total[5m])[1h:15s])",
        "sum_over_time(gauge[5m:])",
        "sum_over_time(gauge[5m:15s] offset 3m)",
        "sum_over_time(gauge[5m:15s] @ start())",
        "sum_over_time((gauge @ end())[5m:15s])",
        "last_over_time(sparse[1m:15s])",
    ] {
        let planned = engine
            .query_range(&tenant_id("t"), query, start_ms, end_ms, secs(37))
            .await
            .unwrap();
        let QueryResult::RangeMatrix(series) = planned else {
            panic!("expected matrix")
        };
        let expr = crate::parse_promql(query).unwrap();
        for timestamp in (start_ms..=end_ms).step_by(37_000) {
            let mut interpreted = super::super::AT_MODIFIER_BOUNDS
                .scope(
                    super::super::AtModifierBounds { start_ms, end_ms },
                    engine.eval_instant_expr("t", &expr, timestamp),
                )
                .await
                .unwrap();
            super::super::result_utils::finalize_metric_names(&mut interpreted).unwrap();
            let QueryResult::InstantVector(mut expected) = interpreted else {
                panic!("expected vector")
            };
            let mut actual = series
                .iter()
                .flat_map(|series| {
                    series
                        .samples
                        .iter()
                        .filter(move |sample| sample.timestamp_ms == timestamp)
                        .map(|sample| crate::InstantSample {
                            labels: series.labels.clone(),
                            ts_ms: timestamp,
                            value: sample.value.clone(),
                            drop_name: false,
                        })
                })
                .collect::<Vec<_>>();
            actual.sort_by_key(|sample| sample.labels.fingerprint());
            expected.sort_by_key(|sample| sample.labels.fingerprint());
            assert2::assert!(
                nan_equal_samples(&actual, &expected),
                "{query} at {timestamp}"
            );
        }
    }
}

#[tokio::test]
#[ignore = "manual CPU and peak RSS measurement with /usr/bin/time -v"]
async fn subquery_grid_profile() {
    let steps = std::env::var("SUBQUERY_OUTER_STEPS")
        .unwrap_or_else(|_| "30".to_string())
        .parse::<i64>()
        .unwrap();
    let window = std::env::var("SUBQUERY_WINDOW_MINUTES").unwrap_or_else(|_| "60".to_string());
    let uncached = std::env::var_os("SUBQUERY_UNCACHED").is_some();
    let (engine, scans) = counting_engine(EngineOpts::default());
    let query = format!("max_over_time(rate(http_requests_total[5m])[{window}m:15s])");
    let tenant = tenant_id("t");
    let future = engine.query_range(
        &tenant,
        &query,
        3_600_000,
        3_600_000 + (steps - 1) * 15_000,
        secs(15),
    );
    let start = std::time::Instant::now();
    let result = if uncached {
        SUBQUERY_GRID_DISABLED.scope((), future).await
    } else {
        future.await
    };
    result.unwrap();
    eprintln!(
        "steps={steps} window_minutes={window} uncached={uncached} scans={} elapsed={:?}",
        scans.load(Ordering::SeqCst),
        start.elapsed()
    );
}

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
    async fn exemplars(&self, t: &str, m: &[LabelMatcher], s: i64, e: i64) -> Result<ExemplarScan> {
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
