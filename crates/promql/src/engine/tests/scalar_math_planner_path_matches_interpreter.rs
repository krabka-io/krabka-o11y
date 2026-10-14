use super::*;

#[derive(bon::Builder)]
struct ScalarMathFloatSample {
    labels: Labels,
    timestamp_ms: i64,
    value: f64,
}

struct ScalarMathQueryCase {
    query: &'static str,
    time_ms: i64,
}

#[tokio::test]
pub(crate) async fn scalar_math_planner_path_matches_interpreter() {
    // A float-only store: a multi-label gauge with negatives (for
    // `sqrt`/`ln` NaN/-inf edges), a genuine-NaN series (must survive the
    // operator path), an up-like series for the nested aggregate case, and a
    // counter for the nested rate case.
    let mut store = InMemoryMetricStore::new();
    for sample in [
        ScalarMathFloatSample::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "g"),
                crate::test_support::TestLabelPair::new("l", "x"),
            ]))
            .timestamp_ms(60_000)
            .value(-3.0)
            .build(),
        ScalarMathFloatSample::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "g"),
                crate::test_support::TestLabelPair::new("l", "y"),
            ]))
            .timestamp_ms(60_000)
            .value(20.0)
            .build(),
        ScalarMathFloatSample::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "g"),
                crate::test_support::TestLabelPair::new("l", "z"),
            ]))
            .timestamp_ms(60_000)
            .value(f64::NAN)
            .build(),
        ScalarMathFloatSample::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]))
            .timestamp_ms(60_000)
            .value(1.0)
            .build(),
        ScalarMathFloatSample::builder()
            .labels(labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "db"),
            ]))
            .timestamp_ms(60_000)
            .value(1.0)
            .build(),
    ] {
        store.push_float("t", sample.labels, sample.timestamp_ms, sample.value);
    }
    // A counter with a few samples for `abs(rate(...))`.
    let ctr = labels(&[
        crate::test_support::TestLabelPair::new("__name__", "c"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]);
    for sample in [
        ScalarMathFloatSample::builder()
            .labels(ctr.clone())
            .timestamp_ms(0)
            .value(0.0)
            .build(),
        ScalarMathFloatSample::builder()
            .labels(ctr.clone())
            .timestamp_ms(60_000)
            .value(30.0)
            .build(),
        ScalarMathFloatSample::builder()
            .labels(ctr.clone())
            .timestamp_ms(120_000)
            .value(90.0)
            .build(),
    ] {
        store.push_float("t", sample.labels, sample.timestamp_ms, sample.value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Representative scalar-math queries over a plannable inner vector. Each
    // must route through the operator path and match the interpreter exactly,
    // including the genuine-NaN row and `sqrt(neg)`/`ln(neg)` -> NaN.
    let queries = [
        ScalarMathQueryCase {
            query: "abs(g)",
            time_ms: 60_000,
        },
        ScalarMathQueryCase {
            query: "sqrt(g)",
            time_ms: 60_000,
        },
        ScalarMathQueryCase {
            query: "ln(g)",
            time_ms: 60_000,
        },
        ScalarMathQueryCase {
            query: "log2(g)",
            time_ms: 60_000,
        },
        ScalarMathQueryCase {
            query: "sgn(g)",
            time_ms: 60_000,
        },
        ScalarMathQueryCase {
            query: "ceil(g)",
            time_ms: 60_000,
        },
        ScalarMathQueryCase {
            query: "floor(g)",
            time_ms: 60_000,
        },
        ScalarMathQueryCase {
            query: "exp(g)",
            time_ms: 60_000,
        },
        ScalarMathQueryCase {
            query: "sin(g)",
            time_ms: 60_000,
        },
        ScalarMathQueryCase {
            query: "cos(g)",
            time_ms: 60_000,
        },
        ScalarMathQueryCase {
            query: "atan(g)",
            time_ms: 60_000,
        },
        ScalarMathQueryCase {
            query: "deg(g)",
            time_ms: 60_000,
        },
        ScalarMathQueryCase {
            query: "rad(g)",
            time_ms: 60_000,
        },
        ScalarMathQueryCase {
            query: "round(g)",
            time_ms: 60_000,
        },
        ScalarMathQueryCase {
            query: "round(g, 5)",
            time_ms: 60_000,
        },
        ScalarMathQueryCase {
            query: "clamp_min(g, 0)",
            time_ms: 60_000,
        },
        ScalarMathQueryCase {
            query: "clamp_max(g, 10)",
            time_ms: 60_000,
        },
        ScalarMathQueryCase {
            query: "clamp(g, 0, 10)",
            time_ms: 60_000,
        },
        // `min > max` yields the empty vector.
        ScalarMathQueryCase {
            query: "clamp(g, 10, 0)",
            time_ms: 60_000,
        },
        // Nested compositional cases: scalar math over rate and over an
        // aggregate, both already on the operator path.
        ScalarMathQueryCase {
            query: "abs(rate(c[5m]))",
            time_ms: 120_000,
        },
        ScalarMathQueryCase {
            query: "ceil(sum by (job) (up))",
            time_ms: 60_000,
        },
        // Binary operands are now planner-supported, so scalar math over a
        // binary inner expression also routes through operators and must
        // match the interpreter (incl. the genuine-NaN row in `g`).
        ScalarMathQueryCase {
            query: "abs(g + 1)",
            time_ms: 60_000,
        },
        // `atan2` is a binary operator returning a vector; it routes through
        // the binary planner path and must match the interpreter.
        ScalarMathQueryCase {
            query: "g atan2 g",
            time_ms: 60_000,
        },
    ];

    for case in queries {
        let SortedParity {
            via_operators: operators,
            via_interpreter: interpreter,
        } = sorted_planned_and_interpreted(&engine, case.query, case.time_ms).await;
        assert2::assert!(nan_equal_samples(&interpreter, &operators));
    }

    // A bare matrix selector now routes through the planner as a
    // `RangeMatrix` result (covered by `matrix_subquery_planner_path_matches_
    // interpreter`), so it is no longer asserted as a scalar-math fall-back
    // here.
}
