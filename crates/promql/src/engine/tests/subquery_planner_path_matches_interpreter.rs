use super::*;

#[derive(bon::Builder)]
struct CounterSeriesFixture {
    label: &'static str,
    factor: f64,
}

#[derive(bon::Builder)]
struct GaugeSeriesFixture {
    label: &'static str,
    group: &'static str,
    base: f64,
}

struct SubqueryCase {
    query: &'static str,
    timestamp_ms: i64,
}

/// Differential parity for subqueries routed through the recursive planner.
///
/// The query is a range call or a `*_over_time` call whose argument is
/// `inner[range:res]`. The planner builds the subquery range vector per
/// aligned sub-step and then applies the shared outer fold. The result must
/// equal the interpreter's `eval_subquery` plus outer fold byte-for-byte, with
/// a NaN-aware comparison.
#[tokio::test]
pub(crate) async fn subquery_planner_path_matches_interpreter() {
    // A float-only store exercising the subquery sub-grid:
    //  - `reqs_total{l}`: two counters (l=a, l=b) for rate/over_time-of-rate.
    //  - `gauge{l}`: a plain gauge in two label groups for the aggregating
    //    inner (`sum by(l)`).
    //  - `sparse{l}`: a series with a single early sample so a tight subquery
    //    window strands it (no-value sub-grid -> dropped series), plus a dense
    //    member so the surviving series is observable.
    let mut store = InMemoryMetricStore::new();
    // Counters: slope = factor over 60s, sampled every 30s out to 20m.
    for fixture in [
        CounterSeriesFixture::builder()
            .label("a")
            .factor(1.0)
            .build(),
        CounterSeriesFixture::builder()
            .label("b")
            .factor(2.0)
            .build(),
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "reqs_total"),
            crate::test_support::TestLabelPair::new("l", fixture.label),
        ]);
        for step in 0..=40_i32 {
            store.push_float(
                "t",
                lbls.clone(),
                i64::from(step) * 30_000,
                f64::from(step) * fixture.factor,
            );
        }
    }
    // Gauges in two groups (`sum by(l)` collapses the `g` member dimension).
    for fixture in [
        GaugeSeriesFixture::builder()
            .label("a")
            .group("0")
            .base(3.0)
            .build(),
        GaugeSeriesFixture::builder()
            .label("a")
            .group("1")
            .base(5.0)
            .build(),
        GaugeSeriesFixture::builder()
            .label("b")
            .group("0")
            .base(7.0)
            .build(),
        GaugeSeriesFixture::builder()
            .label("b")
            .group("1")
            .base(11.0)
            .build(),
    ] {
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "gauge"),
            crate::test_support::TestLabelPair::new("l", fixture.label),
            crate::test_support::TestLabelPair::new("g", fixture.group),
        ]);
        for step in 0..=40_i32 {
            store.push_float(
                "t",
                lbls.clone(),
                i64::from(step) * 30_000,
                fixture.base + f64::from(step),
            );
        }
    }
    // Sparse: l=dense has a full history; l=stranded has only one early
    // sample, so a tight late subquery window yields it no sub-grid points.
    {
        let dense = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "sparse"),
            crate::test_support::TestLabelPair::new("l", "dense"),
        ]);
        for step in 0..=40_i32 {
            store.push_float(
                "t",
                dense.clone(),
                i64::from(step) * 30_000,
                f64::from(step),
            );
        }
        let stranded = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "sparse"),
            crate::test_support::TestLabelPair::new("l", "stranded"),
        ]);
        store.push_float("t", stranded, 0, 1.0);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Each query must route through the operator path and match the
    // interpreter byte-for-byte. `EngineOpts::default().eval_interval` is
    // 60s, so a subquery written `[range:]` (no resolution) uses a 60s stride
    // on BOTH paths.
    let queries = [
        // Selector inner, explicit resolution.
        SubqueryCase {
            query: "rate(reqs_total[5m:1m])",
            timestamp_ms: 1_200_000_i64,
        },
        // Nested: `*_over_time` over a `rate(...)` subquery — the inner rate is
        // itself planned per sub-step.
        SubqueryCase {
            query: "max_over_time(rate(reqs_total[1m])[10m:2m])",
            timestamp_ms: 1_200_000,
        },
        // Aggregating inner with DEFAULT resolution (`[5m:]` -> 60s stride).
        SubqueryCase {
            query: "sum_over_time((sum by(l)(gauge))[5m:])",
            timestamp_ms: 1_200_000,
        },
        SubqueryCase {
            query: "avg_over_time((sum by(l)(gauge))[5m:])",
            timestamp_ms: 1_200_000,
        },
        // `@` and offset on the subquery shift the evaluated end (and the
        // step-aligned start) identically on both paths.
        SubqueryCase {
            query: "sum_over_time(gauge[5m:1m] @ 600)",
            timestamp_ms: 1_200_000,
        },
        SubqueryCase {
            query: "sum_over_time(gauge[5m:1m] offset 5m)",
            timestamp_ms: 1_200_000,
        },
        // Sparse: the stranded member yields an empty sub-grid window and is
        // dropped from the result; the dense member survives. A tight window
        // at a late time.
        SubqueryCase {
            query: "sum_over_time(sparse[1m:30s])",
            timestamp_ms: 1_200_000,
        },
        SubqueryCase {
            query: "last_over_time(sparse[1m:30s])",
            timestamp_ms: 1_200_000,
        },
        // Binary inner.
        SubqueryCase {
            query: "rate((reqs_total + reqs_total)[5m:1m])",
            timestamp_ms: 1_200_000,
        },
        // Unary-negation inner: `Expr::Unary` now routes through the planner,
        // so the subquery's structural gate accepts it and the inner negation
        // is planned per sub-step.
        SubqueryCase {
            query: "sum_over_time((-gauge)[5m:1m])",
            timestamp_ms: 1_200_000,
        },
        SubqueryCase {
            query: "max_over_time((-gauge)[5m:1m])",
            timestamp_ms: 1_200_000,
        },
    ];

    for case in queries {
        let query = case.query;
        let SortedParity {
            via_operators,
            via_interpreter,
        } = sorted_planned_and_interpreted(&engine, query, case.timestamp_ms).await;
        assert2::assert!(instant_samples_match(&via_interpreter, &via_operators));

        // Pin the sparse-window rule: the stranded member is dropped (no
        // sub-grid points), so only the dense series survives.
        if query == "sum_over_time(sparse[1m:30s])" {
            assert2::assert!(via_operators.len() == 1);
            assert2::assert!(via_operators[0].labels.get("l") == Some("dense"));
        }
    }
}
