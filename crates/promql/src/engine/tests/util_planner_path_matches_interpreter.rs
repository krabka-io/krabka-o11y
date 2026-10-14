use super::*;

#[derive(bon::Builder)]
struct UtilitySampleCase {
    name: &'static str,
    job: &'static str,
    timestamp_ms: i64,
    value: f64,
}

struct TimestampExpectation {
    job: &'static str,
    seconds: f64,
}

#[tokio::test]
pub(crate) async fn util_planner_path_matches_interpreter() {
    // NaN-aware whole-result comparison covering both scalar and vector
    // results, sorting vector samples by fingerprint first.
    fn results_match(left: QueryResult, right: QueryResult) -> bool {
        match left {
            QueryResult::Scalar {
                ts_ms: left_ts,
                value: left_value,
            } => match right {
                QueryResult::Scalar {
                    ts_ms: right_ts,
                    value: right_value,
                } => {
                    left_ts == right_ts
                        && (left_value.to_bits() == right_value.to_bits()
                            || (left_value.is_nan() && right_value.is_nan()))
                }
                _ => false,
            },
            QueryResult::InstantVector(mut left_samples) => match right {
                QueryResult::InstantVector(mut right_samples) => {
                    left_samples.sort_by_key(|sample| sample.labels.fingerprint());
                    right_samples.sort_by_key(|sample| sample.labels.fingerprint());
                    nan_equal_samples(&left_samples, &right_samples)
                }
                _ => false,
            },
            _ => false,
        }
    }

    // A float-only store. `m{job}` carries distinct timestamps per series so
    // `timestamp(m)` differs per row; a single-series metric `solo` exercises
    // `scalar(single)`; `dup` (two series) exercises `scalar(multi)->NaN`. A
    // genuine-NaN row survives `timestamp`/calendar drops. `present` exists,
    // `gone` does not (for absent / absent_over_time).
    let mut store = InMemoryMetricStore::new();
    for case in [
        // Two `m` series at different timestamps within the lookback window.
        UtilitySampleCase::builder()
            .name("m")
            .job("api")
            .timestamp_ms(30_000)
            .value(100.0)
            .build(),
        UtilitySampleCase::builder()
            .name("m")
            .job("db")
            .timestamp_ms(60_000)
            .value(1_700_000_000.0)
            .build(),
        // A genuine-NaN `m` row (must survive timestamp/calendar, value-> ts).
        UtilitySampleCase::builder()
            .name("m")
            .job("nan")
            .timestamp_ms(45_000)
            .value(f64::NAN)
            .build(),
        // A single-series metric for scalar(single).
        UtilitySampleCase::builder()
            .name("solo")
            .job("x")
            .timestamp_ms(60_000)
            .value(42.5)
            .build(),
        // Two series sharing a name for scalar(multi)->NaN.
        UtilitySampleCase::builder()
            .name("dup")
            .job("a")
            .timestamp_ms(60_000)
            .value(1.0)
            .build(),
        UtilitySampleCase::builder()
            .name("dup")
            .job("b")
            .timestamp_ms(60_000)
            .value(2.0)
            .build(),
        // A present series for absent(present)->empty / absent_over_time.
        UtilitySampleCase::builder()
            .name("present")
            .job("p")
            .timestamp_ms(55_000)
            .value(7.0)
            .build(),
    ] {
        store.push_float(
            "t",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", case.name),
                crate::test_support::TestLabelPair::new("job", case.job),
            ]),
            case.timestamp_ms,
            case.value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let time_ms = 60_000_i64;

    // Each query must route through the operator path AND match the
    // interpreter (NaN-aware), covering both vector and scalar results.
    let queries = [
        // Vector-returning utilities over a plannable inner.
        "timestamp(m)",
        "timestamp(solo)",
        "day_of_week(m)",
        "day_of_month(m)",
        "day_of_year(m)",
        "days_in_month(m)",
        "hour(m)",
        "minute(m)",
        "month(m)",
        "year(m)",
        // vector(scalar) yields a single no-label series.
        "vector(42)",
        "vector(time())",
        // absent / absent_over_time, present and missing.
        "absent(present)",
        "absent(gone)",
        "absent(gone{job=\"z\"})",
        "absent_over_time(present[5m])",
        "absent_over_time(gone[5m])",
        "absent_over_time(gone{job=\"z\"}[5m])",
        // Scalar-returning utilities.
        "time()",
        "pi()",
        "scalar(solo)",
        "scalar(dup)",
        // Argless calendar forms operate on time().
        "hour()",
        "year()",
        // scalar∘scalar arithmetic folds to a scalar.
        "2 + 3 * 4",
        // calendar over a scalar-arg utility (vector arg).
        "timestamp(vector(1700000000))",
    ];

    for query in queries {
        let parity = planned_and_interpreted(&engine, query, time_ms).await;
        let via_operators = parity.via_operators;
        let via_interpreter = parity.via_interpreter;

        assert2::assert!(results_match(
            via_interpreter.clone(),
            via_operators.clone()
        ));
    }

    // Pin specific behaviors the parity above relies on.
    // 1. scalar(single) returns the lone value; scalar(multi) returns NaN.
    let QueryResult::Scalar { value: single, .. } = engine
        .query_instant(&tenant_id("t"), "scalar(solo)", time_ms)
        .await
        .unwrap()
    else {
        panic!("expected scalar");
    };
    assert2::assert!(single.to_bits() == 42.5_f64.to_bits());
    let QueryResult::Scalar { value: multi, .. } = engine
        .query_instant(&tenant_id("t"), "scalar(dup)", time_ms)
        .await
        .unwrap()
    else {
        panic!("expected scalar");
    };
    assert2::assert!(multi.is_nan());

    // 2. time() and pi() are the eval-time seconds and π.
    let QueryResult::Scalar {
        ts_ms: returned_ts,
        value: eval_seconds,
    } = engine
        .query_instant(&tenant_id("t"), "time()", time_ms)
        .await
        .unwrap()
    else {
        panic!("expected scalar");
    };
    assert2::assert!(returned_ts == time_ms);
    assert2::assert!(eval_seconds.to_bits() == 60.0_f64.to_bits());
    let QueryResult::Scalar { value: pi_v, .. } = engine
        .query_instant(&tenant_id("t"), "pi()", time_ms)
        .await
        .unwrap()
    else {
        panic!("expected scalar");
    };
    assert2::assert!(pi_v.to_bits() == std::f64::consts::PI.to_bits());

    // 3. absent(present) is empty; absent(gone{job="z"}) carries the matcher
    //    label and value 1.
    let QueryResult::InstantVector(present) = engine
        .query_instant(&tenant_id("t"), "absent(present)", time_ms)
        .await
        .unwrap()
    else {
        panic!("expected vector");
    };
    assert2::assert!(present.is_empty());
    let QueryResult::InstantVector(gone) = engine
        .query_instant(&tenant_id("t"), "absent(gone{job=\"z\"})", time_ms)
        .await
        .unwrap()
    else {
        panic!("expected vector");
    };
    assert2::assert!(gone.len() == 1);
    assert2::assert!(gone[0].labels.get("job") == Some("z"));
    assert2::assert!(gone[0].labels.get("__name__") == None);
    assert2::assert!(float_value(&gone[0].value).to_bits() == 1.0_f64.to_bits());

    // 4. timestamp(m) reports each sample's own timestamp in seconds, not the
    //    eval time, and drops __name__.
    let QueryResult::InstantVector(ts_samples) = engine
        .query_instant(&tenant_id("t"), "timestamp(m)", time_ms)
        .await
        .unwrap()
    else {
        panic!("expected vector");
    };
    assert2::assert!(
        ts_samples
            .iter()
            .all(|s| s.labels.get("__name__").is_none())
    );
    let mut by_job = std::collections::BTreeMap::new();
    for sample in &ts_samples {
        by_job.insert(
            sample.labels.get("job").unwrap(),
            float_value(&sample.value),
        );
    }
    for expected in [
        TimestampExpectation {
            job: "api",
            seconds: 30.0_f64,
        },
        TimestampExpectation {
            job: "db",
            seconds: 60.0,
        },
        TimestampExpectation {
            job: "nan",
            seconds: 45.0,
        },
    ] {
        check!(
            by_job[expected.job].to_bits() == expected.seconds.to_bits(),
            "timestamp {} row",
            expected.job
        );
    }
}
