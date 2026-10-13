use super::*;

#[derive(bon::Builder)]
struct SelectorSample {
    labels: Labels,
    timestamp_ms: i64,
    value: f64,
}

struct SelectorCase {
    query: &'static str,
    timestamp_ms: i64,
}

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
    for sample in [
        SelectorSample::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]))
            .timestamp_ms(0)
            .value(1.0)
            .build(),
        SelectorSample::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]))
            .timestamp_ms(60_000)
            .value(2.0)
            .build(),
        SelectorSample::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]))
            .timestamp_ms(120_000)
            .value(3.0)
            .build(),
        SelectorSample::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "db"),
            ]))
            .timestamp_ms(60_000)
            .value(9.0)
            .build(),
        SelectorSample::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "db"),
            ]))
            .timestamp_ms(120_000)
            .value(stale_bits)
            .build(),
        // Genuine NaN as the latest in-window sample: kept as a NaN value.
        SelectorSample::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "nan"),
            ]))
            .timestamp_ms(60_000)
            .value(5.0)
            .build(),
        SelectorSample::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "nan"),
            ]))
            .timestamp_ms(120_000)
            .value(f64::NAN)
            .build(),
        SelectorSample::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "down"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]))
            .timestamp_ms(120_000)
            .value(7.0)
            .build(),
        SelectorSample::builder()
            .labels(labels(&[crate::test_support::TestLabelPair::new(
                "__name__", "lonely",
            )]))
            .timestamp_ms(120_000)
            .value(42.0)
            .build(),
    ] {
        store.push_float("t", sample.labels, sample.timestamp_ms, sample.value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let selectors = [
        SelectorCase {
            query: "up",
            timestamp_ms: 120_000,
        },
        SelectorCase {
            query: "up{job=\"api\"}",
            timestamp_ms: 120_000,
        },
        SelectorCase {
            query: "up{job=~\"a.*\"}",
            timestamp_ms: 120_000,
        },
        SelectorCase {
            query: "up{job!=\"api\"}",
            timestamp_ms: 120_000,
        },
        SelectorCase {
            query: "{__name__=~\".+\"}",
            timestamp_ms: 120_000,
        },
        SelectorCase {
            query: "up offset 1m",
            timestamp_ms: 120_000,
        },
        SelectorCase {
            query: "up @ 60",
            timestamp_ms: 120_000,
        },
        SelectorCase {
            query: "up{job=\"missing\"}",
            timestamp_ms: 120_000,
        },
        SelectorCase {
            query: "lonely",
            timestamp_ms: 120_000,
        },
        // Genuine NaN must be kept (NaN value) on both paths.
        SelectorCase {
            query: "up{job=\"nan\"}",
            timestamp_ms: 120_000,
        },
        // Stale-NaN marker must be dropped (empty result) on both paths.
        SelectorCase {
            query: "up{job=\"db\"}",
            timestamp_ms: 120_000,
        },
    ];

    for case in selectors {
        let query = case.query;
        let time_ms = case.timestamp_ms;
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

        let normalize = |result: QueryResult| fingerprint_sorted(result, query);

        let interpreter = normalize(interpreter);
        let planner = normalize(planner);
        assert2::assert!(instant_samples_match(&interpreter, &planner));

        // Pin the staleness semantics the parity above relies on.
        if query == "up{job=\"nan\"}" {
            // Genuine NaN is kept as a NaN value (and is not a stale marker).
            assert2::assert!(planner.len() == 1);
            let value = float_value(&planner[0].value);
            assert2::assert!(value.is_nan());
            assert2::assert!(!super::super::is_stale_nan(value));
        }
        if query == "up{job=\"db\"}" {
            // Stale-NaN marker terminates the series: empty result.
            assert2::assert!(planner.is_empty());
        }
    }
}
