use super::*;

#[tokio::test]
pub(crate) async fn instant_selector_planner_path_matches_interpreter() {
    use promql_parser::parser::Expr;

    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A small float-only store with multiple series, an empty-string-ish
    // label set, an offset-relevant history, a stale marker (job=db: its
    // latest in-window sample is a stale-NaN marker, so it must be DROPPED
    // on both paths), and a genuine-NaN series (job=nan: its latest
    // in-window sample is a genuine NaN, so it must be KEPT as a NaN value
    // on both paths).
    let mut store = InMemoryMetricStore::new();
    let stale_bits = stale_nan();
    for (lbls, ts, value) in [
        (labels(&[("__name__", "up"), ("job", "api")]), 0_i64, 1.0),
        (labels(&[("__name__", "up"), ("job", "api")]), 60_000, 2.0),
        (labels(&[("__name__", "up"), ("job", "api")]), 120_000, 3.0),
        (labels(&[("__name__", "up"), ("job", "db")]), 60_000, 9.0),
        (
            labels(&[("__name__", "up"), ("job", "db")]),
            120_000,
            stale_bits,
        ),
        // Genuine NaN as the latest in-window sample: kept as a NaN value.
        (labels(&[("__name__", "up"), ("job", "nan")]), 60_000, 5.0),
        (
            labels(&[("__name__", "up"), ("job", "nan")]),
            120_000,
            f64::NAN,
        ),
        (
            labels(&[("__name__", "down"), ("job", "api")]),
            120_000,
            7.0,
        ),
        (labels(&[("__name__", "lonely")]), 120_000, 42.0),
    ] {
        store.push_float("t", lbls, ts, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let selectors = [
        ("up", 120_000_i64),
        ("up{job=\"api\"}", 120_000),
        ("up{job=~\"a.*\"}", 120_000),
        ("up{job!=\"api\"}", 120_000),
        ("{__name__=~\".+\"}", 120_000),
        ("up offset 1m", 120_000),
        ("up @ 60", 120_000),
        ("up{job=\"missing\"}", 120_000),
        ("lonely", 120_000),
        // Genuine NaN must be kept (NaN value) on both paths.
        ("up{job=\"nan\"}", 120_000),
        // Stale-NaN marker must be dropped (empty result) on both paths.
        ("up{job=\"db\"}", 120_000),
    ];

    for (query, time_ms) in selectors {
        let expr = parse_promql_with_duration_context(query, DurationExprContext::instant(time_ms))
            .unwrap_or_else(|error| panic!("parse `{query}`: {error}"));
        let Expr::VectorSelector(selector) = expr else {
            panic!("`{query}` did not parse to a bare vector selector");
        };

        let interpreter = engine
            .eval_instant_selector("t", &selector, time_ms)
            .await
            .unwrap_or_else(|error| panic!("interpreter `{query}`: {error}"));
        let planner = engine
            .eval_instant_selector_via_planner("t", &selector, time_ms)
            .await
            .unwrap_or_else(|error| panic!("planner `{query}`: {error}"));

        let normalize = |result: QueryResult| -> Vec<crate::InstantSample> {
            let QueryResult::InstantVector(mut samples) = result else {
                panic!("expected vector for `{query}`");
            };
            samples.sort_by_key(|sample| sample.labels.fingerprint());
            samples
        };

        let interpreter = normalize(interpreter);
        let planner = normalize(planner);
        assert2::assert!(instant_samples_match(&interpreter, &planner));

        // Pin the staleness semantics the parity above relies on.
        if query == "up{job=\"nan\"}" {
            // Genuine NaN is kept as a NaN value (and is not a stale marker).
            assert2::assert!(planner.len() == 1);
            let value = float_value(&planner[0].value);
            assert2::assert!(value.is_nan());
            assert2::assert!(!super::is_stale_nan(value));
        }
        if query == "up{job=\"db\"}" {
            // Stale-NaN marker terminates the series: empty result.
            assert2::assert!(planner.is_empty());
        }
    }
}
