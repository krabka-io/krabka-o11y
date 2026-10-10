use super::*;

#[tokio::test]
pub(crate) async fn scalar_math_planner_path_matches_interpreter() {
    // A float-only store: a multi-label gauge with negatives (for
    // `sqrt`/`ln` NaN/-inf edges), a genuine-NaN series (must survive the
    // operator path), an up-like series for the nested aggregate case, and a
    // counter for the nested rate case.
    let mut store = InMemoryMetricStore::new();
    for (lbls, ts, value) in [
        (labels(&[("__name__", "g"), ("l", "x")]), 60_000_i64, -3.0),
        (labels(&[("__name__", "g"), ("l", "y")]), 60_000, 20.0),
        (labels(&[("__name__", "g"), ("l", "z")]), 60_000, f64::NAN),
        (labels(&[("__name__", "up"), ("job", "api")]), 60_000, 1.0),
        (labels(&[("__name__", "up"), ("job", "db")]), 60_000, 1.0),
    ] {
        store.push_float("t", lbls, ts, value);
    }
    // A counter with a few samples for `abs(rate(...))`.
    let ctr = labels(&[("__name__", "c"), ("job", "api")]);
    for (ts, value) in [(0_i64, 0.0), (60_000, 30.0), (120_000, 90.0)] {
        store.push_float("t", ctr.clone(), ts, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    // Representative scalar-math queries over a plannable inner vector. Each
    // must route through the operator path and match the interpreter exactly,
    // including the genuine-NaN row and `sqrt(neg)`/`ln(neg)` -> NaN.
    let queries = [
        ("abs(g)", 60_000_i64),
        ("sqrt(g)", 60_000),
        ("ln(g)", 60_000),
        ("log2(g)", 60_000),
        ("sgn(g)", 60_000),
        ("ceil(g)", 60_000),
        ("floor(g)", 60_000),
        ("exp(g)", 60_000),
        ("sin(g)", 60_000),
        ("cos(g)", 60_000),
        ("atan(g)", 60_000),
        ("deg(g)", 60_000),
        ("rad(g)", 60_000),
        ("round(g)", 60_000),
        ("round(g, 5)", 60_000),
        ("clamp_min(g, 0)", 60_000),
        ("clamp_max(g, 10)", 60_000),
        ("clamp(g, 0, 10)", 60_000),
        // `min > max` yields the empty vector.
        ("clamp(g, 10, 0)", 60_000),
        // Nested compositional cases: scalar math over rate and over an
        // aggregate, both already on the operator path.
        ("abs(rate(c[5m]))", 120_000),
        ("ceil(sum by (job) (up))", 60_000),
        // Binary operands are now planner-supported, so scalar math over a
        // binary inner expression also routes through operators and must
        // match the interpreter (incl. the genuine-NaN row in `g`).
        ("abs(g + 1)", 60_000),
        // `atan2` is a binary operator returning a vector; it routes through
        // the binary planner path and must match the interpreter.
        ("g atan2 g", 60_000),
    ];

    for (query, time_ms) in queries {
        let (via_operators, via_interpreter) =
            planned_and_interpreted(&engine, query, time_ms).await;

        let normalize = |result: QueryResult| fingerprint_sorted(result, query);

        let interpreter = normalize(via_interpreter);
        let operators = normalize(via_operators);
        assert2::assert!(nan_equal_samples(&interpreter, &operators));
    }

    // A bare matrix selector now routes through the planner as a
    // `RangeMatrix` result (covered by `matrix_subquery_planner_path_matches_
    // interpreter`), so it is no longer asserted as a scalar-math fall-back
    // here.
}
