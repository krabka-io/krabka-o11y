use super::*;

#[tokio::test]
pub(crate) async fn range_planner_path_matches_interpreter() {
    use promql_parser::parser::Expr;

    use crate::{DurationExprContext, parse_promql_with_duration_context};

    let mut store = InMemoryMetricStore::new();
    let stale_bits = stale_nan();
    // Two counters (for rate and sum-by-rate), grouped by `group`.
    for (lbls, samples) in [
        (
            labels(&[
                ("__name__", "http_requests_total"),
                ("job", "api"),
                ("group", "a"),
            ]),
            vec![
                (0_i64, 0.0),
                (60_000, 1.0),
                (120_000, 3.0),
                (180_000, 6.0),
                (240_000, 10.0),
                (300_000, 15.0),
            ],
        ),
        (
            labels(&[
                ("__name__", "http_requests_total"),
                ("job", "db"),
                ("group", "a"),
            ]),
            vec![
                (0, 0.0),
                (60_000, 2.0),
                (120_000, 4.0),
                (180_000, 8.0),
                (240_000, 16.0),
                (300_000, 32.0),
            ],
        ),
        (
            // group b: a full history so `rate` has a value at every step
            // (no no-value NaN sentinel), keeping the operator aggregate
            // over this rate parity-exact in the forced comparison below.
            labels(&[
                ("__name__", "http_requests_total"),
                ("job", "cache"),
                ("group", "b"),
            ]),
            vec![
                (0, 1.0),
                (60_000, 5.0),
                (120_000, 7.0),
                (180_000, 9.0),
                (240_000, 11.0),
                (300_000, 20.0),
            ],
        ),
    ] {
        for (ts, value) in samples {
            store.push_float("t", lbls.clone(), ts, value);
        }
    }
    // A second counter family with a single-sample (no-value rate) series, to
    // exercise the SPARSE aggregate-over-rate parity (group b's only member is
    // no-value, so it is excluded from the group on both paths).
    for (lbls, samples) in [
        (
            labels(&[("__name__", "spotty_total"), ("job", "api"), ("group", "a")]),
            vec![
                (0_i64, 0.0),
                (60_000, 1.0),
                (120_000, 2.0),
                (180_000, 3.0),
                (240_000, 4.0),
                (300_000, 5.0),
            ],
        ),
        (
            // Only one in-window sample at each step's 2m window: rate has no
            // value -> the operator rate emits NULL (not a NaN sentinel), the
            // aggregate planner drops it before grouping, and group b collapses
            // to no row at those steps — matching the interpreter, which omits
            // the no-value series. This drives the SPARSE aggregate-over-rate
            // parity proof below.
            labels(&[("__name__", "spotty_total"), ("job", "db"), ("group", "b")]),
            vec![(180_000, 100.0)],
        ),
    ] {
        for (ts, value) in samples {
            store.push_float("t", lbls.clone(), ts, value);
        }
    }
    // A plain gauge for a bare-selector range and a binary op.
    for (ts, value) in [
        (0_i64, 2.0),
        (60_000, 4.0),
        (120_000, 8.0),
        (180_000, 16.0),
        (240_000, 32.0),
        (300_000, 64.0),
    ] {
        store.push_float(
            "t",
            labels(&[("__name__", "gauge"), ("job", "api")]),
            ts,
            value,
        );
    }
    // A series whose mid-range latest in-window sample is a stale-NaN marker
    // (the series must vanish for the steps that select it) and whose later
    // sample is a genuine NaN (kept as a NaN value).
    for (ts, value) in [
        (0_i64, 1.0),
        (60_000, stale_bits),
        (120_000, 3.0),
        (180_000, f64::NAN),
        (240_000, 5.0),
    ] {
        store.push_float(
            "t",
            labels(&[("__name__", "spotty"), ("job", "api")]),
            ts,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let (start, end, step) = (0_i64, 300_000_i64, millis(60_000));

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
        let expr =
            parse_promql_with_duration_context(query, DurationExprContext::range(start, end, step))
                .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let mut probe = &expr;
        while let Expr::Paren(paren) = probe {
            probe = &paren.expr;
        }
        assert2::assert!(super::range_expr_routes_through_planner(probe));
        // The public range path now routes these through the planner (the
        // only evaluation engine); it must evaluate without falling back.
        let planner = engine
            .query_range("t", query, start, end, step)
            .await
            .unwrap_or_else(|error| panic!("planner `{query}`: {error}"));
        assert2::assert!(matches!(planner, QueryResult::RangeMatrix(_)));
    }

    // The only top-level range shape the gate keeps on the interpreter is a
    // raw matrix selector / subquery (a range-vector shape owned by the
    // dedicated matrix/subquery range path, not the per-step instant
    // planner). Assert the gate excludes it.
    for query in ["http_requests_total[2m]"] {
        let expr =
            parse_promql_with_duration_context(query, DurationExprContext::range(start, end, step))
                .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let mut probe = &expr;
        while let Expr::Paren(paren) = probe {
            probe = &paren.expr;
        }
        assert2::assert!(!super::range_expr_routes_through_planner(probe));
    }

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
            start,
            end,
            step,
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
        .eval_range_via_planner_forced("t", "spotty", start, end, step)
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
    let times: Vec<i64> = points.iter().map(|(t, _)| *t).collect();
    assert2::assert!(times == vec![0, 120_000, 180_000, 240_000, 300_000]);
    let nan_point = points
        .iter()
        .find(|(t, _)| *t == 180_000)
        .expect("180k point");
    let SampleValue::Float(nan_value) = nan_point.1 else {
        panic!("expected float at 180k");
    };
    assert2::assert!(nan_value.is_nan());
    assert2::assert!(!super::is_stale_nan(nan_value));
}
