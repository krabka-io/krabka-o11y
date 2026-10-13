use super::*;

#[derive(bon::Builder)]
struct VectorSampleCase {
    metric_name: &'static str,
    job: &'static str,
    code: &'static str,
    instance: &'static str,
    value: f64,
}

struct ScalarResultCase {
    query: &'static str,
    expected: f64,
}

#[tokio::test]
pub(crate) async fn binary_planner_path_matches_interpreter() {
    use crate::{DurationExprContext, parse_promql_with_duration_context};

    // A float-only store with overlapping label dimensions for vector
    // matching. `left`/`right` share `{job}` for one-to-one and group_x
    // matching; `code` differentiates the many side. A NaN row and a series
    // present only on one side exercise NaN preservation and no-match drops.
    let mut store = InMemoryMetricStore::new();
    for case in [
        // `left`: one per job (the "one" side for group_left).
        VectorSampleCase::builder()
            .metric_name("left")
            .job("api")
            .code("")
            .instance("")
            .value(10.0)
            .build(),
        VectorSampleCase::builder()
            .metric_name("left")
            .job("db")
            .code("")
            .instance("")
            .value(20.0)
            .build(),
        // `right`: many per job (`code` dimension), the "many" side.
        VectorSampleCase::builder()
            .metric_name("right")
            .job("api")
            .code("200")
            .instance("")
            .value(1.0)
            .build(),
        VectorSampleCase::builder()
            .metric_name("right")
            .job("api")
            .code("500")
            .instance("")
            .value(2.0)
            .build(),
        VectorSampleCase::builder()
            .metric_name("right")
            .job("db")
            .code("200")
            .instance("")
            .value(4.0)
            .build(),
        // A `right` series whose job has no `left` match (no-match drop).
        VectorSampleCase::builder()
            .metric_name("right")
            .job("web")
            .code("200")
            .instance("")
            .value(8.0)
            .build(),
        // `m1`/`m2` for one-to-one on/ignoring matching.
        VectorSampleCase::builder()
            .metric_name("m1")
            .job("api")
            .code("")
            .instance("0")
            .value(3.0)
            .build(),
        VectorSampleCase::builder()
            .metric_name("m1")
            .job("api")
            .code("")
            .instance("1")
            .value(5.0)
            .build(),
        VectorSampleCase::builder()
            .metric_name("m2")
            .job("api")
            .code("")
            .instance("0")
            .value(7.0)
            .build(),
        VectorSampleCase::builder()
            .metric_name("m2")
            .job("api")
            .code("")
            .instance("1")
            .value(11.0)
            .build(),
        // A genuine-NaN row that must survive vector∘scalar arithmetic.
        VectorSampleCase::builder()
            .metric_name("nanm")
            .job("api")
            .code("")
            .instance("0")
            .value(f64::NAN)
            .build(),
        VectorSampleCase::builder()
            .metric_name("nanm")
            .job("api")
            .code("")
            .instance("1")
            .value(13.0)
            .build(),
    ] {
        let mut pairs = vec![
            krabka_metrics::MetricLabelPair::new("__name__", case.metric_name),
            krabka_metrics::MetricLabelPair::new("job", case.job),
        ];
        if !case.code.is_empty() {
            pairs.push(krabka_metrics::MetricLabelPair::new("code", case.code));
        }
        if !case.instance.is_empty() {
            pairs.push(krabka_metrics::MetricLabelPair::new(
                "instance",
                case.instance,
            ));
        }
        store.push_float(
            "t",
            crate::PromqlLabels::from_pairs(pairs),
            60_000,
            case.value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let time_ms = 60_000_i64;

    // Each query must route through the operator path and match the
    // interpreter byte-for-byte (NaN-aware).
    let queries = [
        // vector ∘ scalar (arithmetic, drops __name__).
        "m1 + 100",
        "m1 * 2",
        // scalar ∘ vector.
        "100 - m1",
        "2 ^ m1",
        // vector ∘ scalar comparison without bool (filters, keeps labelset).
        "m1 > 4",
        // vector ∘ scalar comparison with bool (keeps all, drops __name__).
        "m1 > bool 4",
        // genuine NaN must survive vector∘scalar arithmetic.
        "nanm + 1",
        // vector ∘ vector one-to-one, default matching (drops __name__).
        "m1 + m2",
        "m2 - m1",
        "m1 / m2",
        "m1 % m2",
        "m1 ^ m2",
        "m1 atan2 m2",
        // one-to-one with on / ignoring.
        "m1 + on(job, instance) m2",
        "m1 + ignoring(__name__) m2",
        // one-to-one comparison without bool (keeps LHS labelset incl. name).
        "m1 > m2",
        "m2 >= m1",
        // one-to-one comparison with bool (drops __name__).
        "m1 == bool m2",
        "m1 != bool m2",
        // group_left (many-to-one): the `right` many side copies a label
        // from the `left` one side.
        "right * on(job) group_left left",
        "right + on(job) group_left() left",
        // group_right (one-to-many): the `left` one side, many `right`.
        "left * on(job) group_right right",
        // set ops: and / or / unless, with and without on/ignoring.
        "m1 and m2",
        "m1 or m2",
        "m1 unless m2",
        "right and on(job) left",
        "right unless on(job) left",
        "left or on(job) right",
        // a no-match set op (web has no left): or keeps it, and/unless drop.
        "right and on(job) left",
    ];

    for query in queries {
        let SortedParity {
            via_operators: operators,
            via_interpreter: interpreter,
        } = sorted_planned_and_interpreted(&engine, query, time_ms).await;
        assert2::assert!(nan_equal_samples(&interpreter, &operators));
    }

    // Pin specific behaviors the parity above relies on.
    // 1. `__name__` is dropped for arithmetic.
    let arith = engine
        .query_instant(&tenant_id("t"), "m1 + m2", time_ms)
        .await
        .unwrap();
    let QueryResult::InstantVector(arith) = arith else {
        panic!("expected vector");
    };
    assert2::assert!(arith.iter().all(|s| s.labels.get("__name__").is_none()));
    // 2. A comparison without `bool` keeps the LHS labelset (incl. __name__).
    let cmp = engine
        .query_instant(&tenant_id("t"), "m1 > m2", time_ms)
        .await
        .unwrap();
    let QueryResult::InstantVector(cmp) = cmp else {
        panic!("expected vector");
    };
    assert2::assert!(cmp.iter().all(|s| s.labels.get("__name__") == Some("m1")));
    // 3. A no-match set op: `right and on(job) left` drops `web` (no left).
    let setop = engine
        .query_instant(&tenant_id("t"), "right and on(job) left", time_ms)
        .await
        .unwrap();
    let QueryResult::InstantVector(setop) = setop else {
        panic!("expected vector");
    };
    assert2::assert!(setop.iter().all(|s| s.labels.get("job") != Some("web")));

    // Scalar ∘ scalar now folds through the planner into a scalar planned
    // result; it must route AND match the interpreter's scalar value+ts.
    for case in [
        ScalarResultCase {
            query: "1 + 2",
            expected: 3.0_f64,
        },
        ScalarResultCase {
            query: "3 * 4 - 1",
            expected: 11.0_f64,
        },
        ScalarResultCase {
            query: "2 > bool 1",
            expected: 1.0_f64,
        },
    ] {
        let expr =
            parse_promql_with_duration_context(case.query, DurationExprContext::instant(time_ms))
                .unwrap();
        let planned = engine
            .plan_instant_expr("t", &expr, time_ms)
            .await
            .unwrap()
            .unwrap_or_else(|| {
                panic!(
                    "scalar∘scalar `{}` must route through the planner",
                    case.query
                )
            });
        let via_operators = engine
            .assemble_planned_instant(planned, time_ms)
            .await
            .unwrap();
        let QueryResult::Scalar { ts_ms, value } = via_operators else {
            panic!("expected scalar for `{}`", case.query);
        };
        assert2::assert!(ts_ms == time_ms);
        assert2::assert!(value.to_bits() == case.expected.to_bits());
    }
}
