use super::*;

#[derive(bon::Builder)]
struct RangeFixtureSample {
    timestamp_ms: i64,
    value: f64,
}

struct RangeFixtureSeries {
    labels: Labels,
    samples: Vec<RangeFixtureSample>,
}

fn range_labels(metric: &str, job: &str, group: &str) -> Labels {
    labels(&[
        crate::test_support::TestLabelPair::new("__name__", metric),
        crate::test_support::TestLabelPair::new("job", job),
        crate::test_support::TestLabelPair::new("group", group),
    ])
}

fn minute_samples(values: &[f64]) -> Vec<RangeFixtureSample> {
    values
        .iter()
        .enumerate()
        .map(|(minute, &value)| {
            RangeFixtureSample::builder()
                .timestamp_ms(minute as i64 * 60_000)
                .value(value)
                .build()
        })
        .collect()
}

fn push_range_series(store: &mut InMemoryMetricStore, series: RangeFixtureSeries) {
    for sample in series.samples {
        store.push_float(
            "t",
            series.labels.clone(),
            sample.timestamp_ms,
            sample.value,
        );
    }
}

struct RangeQueryBounds {
    start_ms: i64,
    end_ms: i64,
    step_ms: krabka_units::prelude::Time,
}

#[tokio::test]
pub(crate) async fn range_planner_path_matches_interpreter() {
    use crate::DurationExprContext;

    let mut store = InMemoryMetricStore::new();
    let stale_bits = stale_nan();
    // Two counters (for rate and sum-by-rate), grouped by `group`.
    for series in [
        RangeFixtureSeries {
            labels: range_labels("http_requests_total", "api", "a"),
            samples: minute_samples(&[0.0, 1.0, 3.0, 6.0, 10.0, 15.0]),
        },
        RangeFixtureSeries {
            labels: range_labels("http_requests_total", "db", "a"),
            samples: minute_samples(&[0.0, 2.0, 4.0, 8.0, 16.0, 32.0]),
        },
        RangeFixtureSeries {
            // group b: a full history so `rate` has a value at every step
            // (no no-value NaN sentinel), keeping the operator aggregate
            // over this rate parity-exact in the forced comparison below.
            labels: range_labels("http_requests_total", "cache", "b"),
            samples: minute_samples(&[1.0, 5.0, 7.0, 9.0, 11.0, 20.0]),
        },
    ] {
        push_range_series(&mut store, series);
    }
    // A second counter family with a single-sample (no-value rate) series, to
    // exercise the SPARSE aggregate-over-rate parity (group b's only member is
    // no-value, so it is excluded from the group on both paths).
    for series in [
        RangeFixtureSeries {
            labels: range_labels("spotty_total", "api", "a"),
            samples: minute_samples(&[0.0, 1.0, 2.0, 3.0, 4.0, 5.0]),
        },
        RangeFixtureSeries {
            // Only one in-window sample at each step's 2m window: rate has no
            // value -> the operator rate emits NULL (not a NaN sentinel), the
            // aggregate planner drops it before grouping, and group b collapses
            // to no row at those steps — matching the interpreter, which omits
            // the no-value series. This drives the SPARSE aggregate-over-rate
            // parity proof below.
            labels: range_labels("spotty_total", "db", "b"),
            samples: vec![
                RangeFixtureSample::builder()
                    .timestamp_ms(180_000)
                    .value(100.0)
                    .build(),
            ],
        },
    ] {
        push_range_series(&mut store, series);
    }
    // A plain gauge for a bare-selector range and a binary op.
    for sample in [
        RangeFixtureSample::builder()
            .timestamp_ms(0)
            .value(2.0)
            .build(),
        RangeFixtureSample::builder()
            .timestamp_ms(60_000)
            .value(4.0)
            .build(),
        RangeFixtureSample::builder()
            .timestamp_ms(120_000)
            .value(8.0)
            .build(),
        RangeFixtureSample::builder()
            .timestamp_ms(180_000)
            .value(16.0)
            .build(),
        RangeFixtureSample::builder()
            .timestamp_ms(240_000)
            .value(32.0)
            .build(),
        RangeFixtureSample::builder()
            .timestamp_ms(300_000)
            .value(64.0)
            .build(),
    ] {
        store.push_float(
            "t",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "gauge"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]),
            sample.timestamp_ms,
            sample.value,
        );
    }
    // A series whose mid-range latest in-window sample is a stale-NaN marker
    // (the series must vanish for the steps that select it) and whose later
    // sample is a genuine NaN (kept as a NaN value).
    for sample in [
        RangeFixtureSample::builder()
            .timestamp_ms(0)
            .value(1.0)
            .build(),
        RangeFixtureSample::builder()
            .timestamp_ms(60_000)
            .value(stale_bits)
            .build(),
        RangeFixtureSample::builder()
            .timestamp_ms(120_000)
            .value(3.0)
            .build(),
        RangeFixtureSample::builder()
            .timestamp_ms(180_000)
            .value(f64::NAN)
            .build(),
        RangeFixtureSample::builder()
            .timestamp_ms(240_000)
            .value(5.0)
            .build(),
    ] {
        store.push_float(
            "t",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "spotty"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]),
            sample.timestamp_ms,
            sample.value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let bounds = RangeQueryBounds {
        start_ms: 0,
        end_ms: 300_000,
        step_ms: millis(60_000),
    };

    // Queries the production gate routes through the per-step operator
    // planner. For these, the gate must accept them and the planner-routed
    // public `query_range` must evaluate successfully; the byte-exact value
    // checks are pinned below (and across the conformance corpus).
    let planner_routed = [
        // A rate over a counter (per-step rate projection).
        "rate(http_requests_total[2m])",
        // A vector-scalar binary op.
        "gauge * 2",
        // A vector-vector binary op (one-to-one on job).
        "gauge + on(job) http_requests_total{job=\"api\"}",
        // A scalar-math call over a selector (preserves genuine NaN, gaps
        // the stale-marker steps).
        "abs(spotty - 10)",
        // A simple aggregate over a bare selector (no rate sentinel).
        "sum by(job)(gauge)",
        // Aggregation over a rate: the marquee fix. Every series is dense, so
        // no no-value NULL arises — pure parity with the interpreter.
        "sum by(group)(rate(http_requests_total[2m]))",
        // Aggregation over a rate where one group member is SPARSE (the
        // single-sample `spotty_total` series at job=db,group=b yields no rate
        // value across the early steps). The UDF emits NULL for those steps,
        // the aggregate planner drops them before grouping, and group b
        // collapses to no result row at the steps where its only member is
        // no-value — exactly as the interpreter omits the no-value series.
        // group a (dense) is unaffected. This is the headline divergence the
        // fix closes, proven byte-exact through the public range path.
        "sum by(group)(rate(spotty_total[2m]))",
        // Parameterized aggregations over a plannable inner now route through
        // the planner per step. `topk` selects original series each step (a
        // series can appear/disappear between steps, stitched by fingerprint);
        // `quantile`/`stddev` reduce per group per step. All must equal the
        // interpreter byte-for-byte across the step grid.
        "topk(2, rate(http_requests_total[2m]))",
        "quantile(0.5, gauge)",
        "stddev by(group)(rate(http_requests_total[2m]))",
        // A BARE top-level instant-vector selector now routes through the
        // planner: the interpreter range path is fixed to the left-OPEN
        // lookback, so the operator selector chain matches it (and Prometheus)
        // byte-for-byte, including the stale-marker gaps and genuine-NaN keep.
        "gauge",
        "spotty",
        // A top-level SCALAR-typed expression now routes too: both range paths
        // fold an identical no-label scalar series per step.
        "42",
        "time()",
        "1 + 2",
    ];
    for query in planner_routed {
        assert2::assert!(range_query_routes_through_planner(
            query,
            DurationExprContext::range(bounds.start_ms, bounds.end_ms, bounds.step_ms)
        ));
        // The public range path now routes these through the planner (the
        // only evaluation engine); it must evaluate without falling back.
        let planner = engine
            .query_range(
                &tenant_id("t"),
                query,
                bounds.start_ms,
                bounds.end_ms,
                bounds.step_ms,
            )
            .await
            .unwrap_or_else(|error| panic!("planner `{query}`: {error}"));
        assert2::assert!(matches!(planner, QueryResult::RangeMatrix(_)));
    }

    // The only top-level range shape the gate keeps on the interpreter is a
    // raw matrix selector / subquery (a range-vector shape owned by the
    // dedicated matrix/subquery range path, not the per-step instant
    // planner). Assert the gate excludes it.
    assert2::assert!(!range_query_routes_through_planner(
        "http_requests_total[2m]",
        DurationExprContext::range(bounds.start_ms, bounds.end_ms, bounds.step_ms)
    ));

    // The headline fix, proven directly on the SPARSE aggregate-over-rate.
    // `sum by(group)(rate(spotty_total[2m]))` over the full `[0, 300000]`
    // grid: group b's only member is a single-sample series, so its rate is
    // no-value (NULL) at every step. The rate UDF emits NULL for those steps,
    // the aggregate planner drops them before grouping, and group b collapses
    // to NO result row at all — only group a (dense) survives. (Before the
    // fix the operator path leaked a spurious NaN group-b row here.)
    let QueryResult::RangeMatrix(sparse) = engine
        .eval_range_via_planner_forced(
            "t",
            "sum by(group)(rate(spotty_total[2m]))",
            bounds.start_ms,
            bounds.end_ms,
            bounds.step_ms,
        )
        .await
        .unwrap()
    else {
        panic!("expected matrix for the sparse aggregate-over-rate");
    };
    let sparse_groups: Vec<Option<&str>> = sparse
        .iter()
        .map(|series| series.labels.get("group"))
        .collect();
    assert2::assert!(sparse_groups == vec![Some("a")]);

    // Pin the stale-vs-genuine-NaN semantics the scalar-math `spotty` parity
    // relies on, via the forced planner path on `abs(spotty - 10)`.
    let QueryResult::RangeMatrix(series) = engine
        .eval_range_via_planner_forced(
            "t",
            "spotty",
            bounds.start_ms,
            bounds.end_ms,
            bounds.step_ms,
        )
        .await
        .unwrap()
    else {
        panic!("expected matrix for `spotty`");
    };
    assert2::assert!(series.len() == 1);
    // Steps (ms) -> selected latest-in-window value, lookback 5m:
    //   0 -> 1.0; 60k -> stale (DROPPED, no point); 120k -> 3.0;
    //   180k -> NaN (kept); 240k -> 5.0; 300k -> 5.0 (240k still in window).
    let points = &series[0].samples;
    let times: Vec<i64> = points.iter().map(|sample| sample.timestamp_ms).collect();
    assert2::assert!(times == vec![0, 120_000, 180_000, 240_000, 300_000]);
    let nan_point = points
        .iter()
        .find(|sample| sample.timestamp_ms == 180_000)
        .expect("180k point");
    let SampleValue::Float(nan_value) = nan_point.value else {
        panic!("expected float at 180k");
    };
    assert2::assert!(nan_value.is_nan());
    assert2::assert!(!super::super::is_stale_nan(nan_value));
}
