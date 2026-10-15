use super::*;

mod instant_avg_over_time_holds_an_infinite_mean_until_it_is_contradicted {
use super::*;

#[derive(bon::Builder)]
struct AverageCase {
    name: &'static str,
    values: Vec<f64>,
    expected: f64,
}

/// Prometheus' mean keeps an infinite running value infinite, and only a
/// contradicting sample turns it into NaN. Nothing else in the suite drives
/// the kernel's infinity arms, which decide when a sample is skipped.
#[tokio::test]
pub(crate) async fn instant_avg_over_time_holds_an_infinite_mean_until_it_is_contradicted() {
    let cases = [
        AverageCase::builder()
            .name("two infinities of the same sign stay that infinity")
            .values(vec![f64::INFINITY, f64::INFINITY])
            .expected(f64::INFINITY)
            .build(),
        AverageCase::builder()
            .name("a finite sample cannot pull it back")
            .values(vec![f64::INFINITY, 5.0])
            .expected(f64::INFINITY)
            .build(),
        AverageCase::builder()
            .name("the other infinity contradicts it")
            .values(vec![f64::INFINITY, f64::NEG_INFINITY])
            .expected(f64::NAN)
            .build(),
        AverageCase::builder()
            .name("so does a NaN")
            .values(vec![f64::INFINITY, f64::NAN])
            .expected(f64::NAN)
            .build(),
        AverageCase::builder()
            .name("and it works negative too")
            .values(vec![f64::NEG_INFINITY, -5.0])
            .expected(f64::NEG_INFINITY)
            .build(),
        // `1.0 + 0.1 + 0.1` rounds up to `1.2000000000000002`, and only the
        // compensation carries the lost bit back. Adding it lands the mean on
        // `0.4`; subtracting it, or dropping it, lands one ulp either side.
        AverageCase::builder()
            .name("the compensation is added back, not subtracted")
            .values(vec![1.0, 0.1, 0.1])
            .expected(0.4)
            .build(),
    ];
    for case_index in 0..cases.len() {
        let case = &cases[case_index];
        let mut store = InMemoryMetricStore::new();
        for index in 0..case.values.len() {
            let value = case.values[index];
            store.push_float(
                "tenant-a",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "queue_depth"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                ]),
                i64::try_from(index).unwrap() * 1_000,
                value,
            );
        }
        // A native histogram under the same name routes the selector through
        // the interpreter's range kernel rather than the operator leaf.
        store.push_histogram(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "queue_depth"),
                crate::test_support::TestLabelPair::new("job", "cache"),
            ]),
            1_000,
            native_histogram(4.0, 1.0),
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(&engine, "avg_over_time(queue_depth[5m])", 60_000).await;
        let sample = samples
            .iter()
            .find(|sample| sample.labels.get("job") == Some("api"))
            .unwrap_or_else(|| panic!("{}: the float series is missing", case.name));
        let got = float_value(&sample.value);
        if case.expected.is_nan() {
            check!(got.is_nan(), "{}: {got}", case.name);
        } else {
            // Exact, not approximate: the compensation case differs from its
            // mutants by a single ulp.
            check!(
                got.to_bits() == case.expected.to_bits(),
                "{}: {got}",
                case.name
            );
        }
    }
}

}

mod instant_basic_over_time_functions_reduce_range_samples {
use super::*;

struct OverTimeCase {
    query: &'static str,
    expected: f64,
    preserves_name: bool,
}

#[tokio::test]
pub(crate) async fn instant_basic_over_time_functions_reduce_range_samples() {
    let store = three_point_gauge_store("queue_depth");
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for case in [
        OverTimeCase {
            query: "sum_over_time(queue_depth[2m])",
            expected: 8.0,
            preserves_name: false,
        },
        OverTimeCase {
            query: "avg_over_time(queue_depth[2m])",
            expected: 4.0,
            preserves_name: false,
        },
        OverTimeCase {
            query: "count_over_time(queue_depth[2m])",
            expected: 2.0,
            preserves_name: false,
        },
        OverTimeCase {
            query: "min_over_time(queue_depth[2m])",
            expected: 3.0,
            preserves_name: false,
        },
        OverTimeCase {
            query: "max_over_time(queue_depth[2m])",
            expected: 5.0,
            preserves_name: false,
        },
        OverTimeCase {
            query: "first_over_time(queue_depth[2m])",
            expected: 3.0,
            preserves_name: true,
        },
        OverTimeCase {
            query: "last_over_time(queue_depth[2m])",
            expected: 5.0,
            preserves_name: true,
        },
        OverTimeCase {
            query: "present_over_time(queue_depth[2m])",
            expected: 1.0,
            preserves_name: false,
        },
    ] {
        let samples = instant_vector(&engine, case.query, 120_000).await;
        assert2::assert!(samples.len() == 1);
        if case.preserves_name {
            assert2::assert!(samples[0].labels.get("__name__") == Some("queue_depth"));
        } else {
            assert2::assert!(samples[0].labels.get("__name__").is_none());
        }
        assert2::assert!(samples[0].labels.get("job") == Some("api"));
        assert2::assert!(approx_eq(float_value(&samples[0].value), case.expected));
    }
}

}

mod instant_calendar_functions_extract_utc_fields_from_sample_values {
use super::*;

struct CalendarQueryCase {
    query: &'static str,
    expected: f64,
}

#[tokio::test]
pub(crate) async fn instant_calendar_functions_extract_utc_fields_from_sample_values() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "event_timestamp_seconds"),
            crate::test_support::TestLabelPair::new("case", "leap"),
        ]),
        10_000,
        1_709_178_060.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for case in [
        CalendarQueryCase {
            query: "year(event_timestamp_seconds)",
            expected: 2024.0,
        },
        CalendarQueryCase {
            query: "month(event_timestamp_seconds)",
            expected: 2.0,
        },
        CalendarQueryCase {
            query: "day_of_month(event_timestamp_seconds)",
            expected: 29.0,
        },
        CalendarQueryCase {
            query: "day_of_week(event_timestamp_seconds)",
            expected: 4.0,
        },
        CalendarQueryCase {
            query: "day_of_year(event_timestamp_seconds)",
            expected: 60.0,
        },
        CalendarQueryCase {
            query: "days_in_month(event_timestamp_seconds)",
            expected: 29.0,
        },
        CalendarQueryCase {
            query: "hour(event_timestamp_seconds)",
            expected: 3.0,
        },
        CalendarQueryCase {
            query: "minute(event_timestamp_seconds)",
            expected: 41.0,
        },
    ] {
        let samples = instant_vector(&engine, case.query, 10_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "case",
                label_value: "leap",
            },
            case.expected,
        );
    }
}

}

mod instant_calendar_functions_without_args_use_eval_time {
use super::*;

#[tokio::test]
pub(crate) async fn instant_calendar_functions_without_args_use_eval_time() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_instant(&tenant_id("tenant-a"), "minute()", 3_660_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected instant vector");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(samples[0].labels.is_empty());
    assert2::assert!(samples[0].ts_ms == 3_660_000);
    assert2::assert!(approx_eq(float_value(&samples[0].value), 1.0));
}

}

mod instant_clamp_bounds_vector_values {
use super::*;

struct ClampedSample {
    instance: String,
    value: f64,
}

#[tokio::test]
pub(crate) async fn instant_clamp_bounds_vector_values() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("low", -5.0), ("mid", 7.0), ("high", 20.0)] {
        push_metric_instance_sample(&mut store, "temperature_celsius", instance, value);
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "clamp(temperature_celsius, 0, 10)", 10_000).await;
    assert2::assert!(samples.len() == 3);
    let values = samples
        .iter()
        .map(|sample| ClampedSample {
            instance: sample.labels.get("instance").unwrap().to_string(),
            value: float_value(&sample.value),
        })
        .collect::<Vec<_>>();
    assert!(
        values
            .iter()
            .any(|sample| sample.instance == "low" && sample.value == 0.0)
    );
    assert!(
        values
            .iter()
            .any(|sample| sample.instance == "mid" && sample.value == 7.0)
    );
    assert!(
        values
            .iter()
            .any(|sample| sample.instance == "high" && sample.value == 10.0)
    );
}

}

mod instant_clamp_min_and_max_apply_single_bound {
use super::*;

#[derive(bon::Builder)]
struct ClampSeries {
    name: &'static str,
    value: f64,
}

#[tokio::test]
pub(crate) async fn instant_clamp_min_and_max_apply_single_bound() {
    let mut store = InMemoryMetricStore::new();
    for series in [
        ClampSeries::builder().name("below").value(-5.0).build(),
        ClampSeries::builder().name("inside").value(7.0).build(),
        ClampSeries::builder().name("above").value(20.0).build(),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "temperature_celsius"),
                crate::test_support::TestLabelPair::new("case", series.name),
            ]),
            10_000,
            series.value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let min_result = engine
        .query_instant(
            &tenant_id("tenant-a"),
            "clamp_min(temperature_celsius, 0)",
            10_000,
        )
        .await
        .unwrap();
    let max_result = engine
        .query_instant(
            &tenant_id("tenant-a"),
            "clamp_max(temperature_celsius, 10)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(min_samples) = min_result else {
        panic!("expected vector");
    };
    let QueryResult::InstantVector(max_samples) = max_result else {
        panic!("expected vector");
    };
    check!(min_samples.len() == 3);
    check!(max_samples.len() == 3);
    check!(min_samples.iter().any(|sample| {
        sample.labels.get("case") == Some("below") && approx_eq(float_value(&sample.value), 0.0)
    }));
    check!(min_samples.iter().any(|sample| {
        sample.labels.get("case") == Some("above") && approx_eq(float_value(&sample.value), 20.0)
    }));
    check!(max_samples.iter().any(|sample| {
        sample.labels.get("case") == Some("below") && approx_eq(float_value(&sample.value), -5.0)
    }));
    check!(max_samples.iter().any(|sample| {
        sample.labels.get("case") == Some("above") && approx_eq(float_value(&sample.value), 10.0)
    }));
}

}

mod instant_clamp_with_reversed_bounds_returns_empty_vector {
use super::*;

#[tokio::test]
pub(crate) async fn instant_clamp_with_reversed_bounds_returns_empty_vector() {
    let mut store = InMemoryMetricStore::new();
    store.push_float(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "temperature_celsius"),
            crate::test_support::TestLabelPair::new("instance", "api"),
        ]),
        10_000,
        7.0,
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "clamp(temperature_celsius, 10, 0)", 10_000).await;
    assert2::assert!(samples.is_empty());
}

}

mod instant_delta_is_gauge_delta_without_reset_correction {
use super::*;

#[tokio::test]
pub(crate) async fn instant_delta_is_gauge_delta_without_reset_correction() {
    let store = SeriesFixture::new(labels(&[
        crate::test_support::TestLabelPair::new("__name__", "temperature_celsius"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]))
    .at(30_000_i64, 4.0)
    .at(60_000, 3.0)
    .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "delta(temperature_celsius[1m])", 60_000).await;
    assert2::assert!(samples.len() == 1);
    assert2::assert!(approx_eq(float_value(&samples[0].value), -2.0));
}

}

mod instant_deriv_returns_gauge_slope_per_second {
use super::*;

#[tokio::test]
pub(crate) async fn instant_deriv_returns_gauge_slope_per_second() {
    let store = three_point_gauge_store("temperature_celsius");
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "deriv(temperature_celsius[2m])", 120_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        2.0 / 60.0,
    );
}

}

mod instant_double_exponential_smoothing_smooths_gauge_series {
#[cfg(feature = "experimental-functions")]
use super::*;

#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn instant_double_exponential_smoothing_smooths_gauge_series() {
    let store = queue_depth_store(&[(0, 3.0), (60_000, 6.0), (120_000, 12.0), (180_000, 21.0)]);

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(
        &engine,
        "double_exponential_smoothing(queue_depth[4m], 0.5, 0.5)",
        180_000,
    )
    .await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        17.625,
    );
}

}

mod instant_double_exponential_smoothing_validates_factors {
#[cfg(feature = "experimental-functions")]
use super::*;

#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn instant_double_exponential_smoothing_validates_factors() {
    let store = queue_depth_store(&[(0, 3.0), (60_000, 6.0)]);

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let error = engine
        .query_instant(
            &tenant_id("tenant-a"),
            "double_exponential_smoothing(queue_depth[2m], 1, 0.5)",
            60_000,
        )
        .await
        .unwrap_err();

    assert2::assert!(matches!(error, PromqlError::Plan(_)));
    assert2::assert!(format!("{error}").contains("smoothing factor"));
}

}

mod instant_double_exponential_smoothing_weights_each_factor_distinctly {
#[cfg(feature = "experimental-functions")]
use super::*;

#[cfg(feature = "experimental-functions")]
#[derive(bon::Builder)]
struct SampleCase {
    timestamp_ms: i64,
    value: f64,
}

#[cfg(feature = "experimental-functions")]
struct QueryCase {
    query: &'static str,
    expected: f64,
}

/// The same smoothing with *distinct* factors. Every other test here passes
/// `0.5, 0.5`, where `factor` and `1.0 - factor` are the same number, so
/// swapping either one is invisible -- as is reversing which of the two the
/// value and the trend are weighted by. Four samples also run the trend update
/// twice, which a shorter series never reaches, and the pair case pins the
/// two-sample minimum.
#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn instant_double_exponential_smoothing_weights_each_factor_distinctly() {
    let mut store = InMemoryMetricStore::new();
    // The initial trend is 2.0, not 1.0: at 1.0 the trend carry `(1 - tf) *
    // trend` equals `(1 - tf) / trend`, so that multiply would be free to be a
    // divide.
    for sample in [
        SampleCase::builder().timestamp_ms(0).value(1.0).build(),
        SampleCase::builder()
            .timestamp_ms(60_000)
            .value(3.0)
            .build(),
        SampleCase::builder()
            .timestamp_ms(120_000)
            .value(4.0)
            .build(),
        SampleCase::builder()
            .timestamp_ms(180_000)
            .value(8.0)
            .build(),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "g")]),
            sample.timestamp_ms,
            sample.value,
        );
    }
    for sample in [
        SampleCase::builder()
            .timestamp_ms(120_000)
            .value(1.0)
            .build(),
        SampleCase::builder()
            .timestamp_ms(180_000)
            .value(2.0)
            .build(),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "pair")]),
            sample.timestamp_ms,
            sample.value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for case in [
        QueryCase {
            query: "double_exponential_smoothing(g[4m], 0.3, 0.4)",
            expected: 7.006,
        },
        // Two samples are the minimum the fold accepts.
        QueryCase {
            query: "double_exponential_smoothing(pair[2m], 0.3, 0.4)",
            expected: 2.0,
        },
    ] {
        assert_lone_value_at(
            &engine,
            LoneValueQuery {
                query: case.query,
                time_ms: 180_000,
                want: case.expected,
            },
        )
        .await;
    }
}

}

mod instant_duration_expression_helpers_require_experimental_feature {
#[cfg(not(feature = "experimental-functions"))]
use super::*;

#[cfg(not(feature = "experimental-functions"))]
#[tokio::test]
pub(crate) async fn instant_duration_expression_helpers_require_experimental_feature() {
    let store = InMemoryMetricStore::new();
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for query in ["range()", "step()", "start()", "end()"] {
        let error = engine
            .query_instant(&tenant_id("tenant-a"), query, 120_000)
            .await
            .unwrap_err();

        assert2::assert!(matches!(error, PromqlError::Unsupported(_)));
        assert2::assert!(format!("{error}").contains("experimental-functions"));
    }
}

}

mod instant_duration_expression_helpers_return_zero {
#[cfg(feature = "experimental-functions")]
use super::*;

#[cfg(feature = "experimental-functions")]
struct ScalarCase {
    query: &'static str,
    expected: f64,
}

#[cfg(feature = "experimental-functions")]
struct RangeCase {
    query: &'static str,
    expected: Vec<f64>,
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn instant_duration_expression_helpers_use_the_evaluation_time() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());

    for case in [
        ScalarCase {
            query: "range()",
            expected: 0.0,
        },
        ScalarCase {
            query: "step()",
            expected: 0.0,
        },
        ScalarCase {
            query: "start()",
            expected: 120.0,
        },
        ScalarCase {
            query: "end()",
            expected: 120.0,
        },
        ScalarCase {
            query: "(start() + end()) / 2",
            expected: 120.0,
        },
        ScalarCase {
            query: "start ( ) - end ( )",
            expected: 0.0,
        },
    ] {
        let result = engine
            .query_instant(&tenant_id("tenant-a"), case.query, 120_000)
            .await
            .unwrap();

        assert2::assert!(
            result
                == QueryResult::Scalar {
                    ts_ms: 120_000,
                    value: case.expected,
                }
        );
    }

    let result = engine
        .query_instant(&tenant_id("tenant-a"), "start() ^ 2", -1_000)
        .await
        .unwrap();
    assert2::assert!(
        result
            == QueryResult::Scalar {
                ts_ms: -1_000,
                value: 1.0,
            }
    );
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
async fn nested_query_bounds_fold_over_a_populated_range() {
    let store = SeriesFixture::new(labels(&[crate::test_support::TestLabelPair::new(
        "__name__", "m",
    )]))
    .at(60_000, 2.0)
    .at(120_000, 3.0)
    .at(180_000, 4.0)
    .store();
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for case in [
        RangeCase {
            query: "m * (end() - start())",
            expected: vec![240.0, 360.0, 480.0],
        },
        RangeCase {
            query: "m @ start() + vector(end())",
            expected: vec![182.0; 3],
        },
        RangeCase {
            query: "m @ end() + vector(start())",
            expected: vec![64.0; 3],
        },
    ] {
        let QueryResult::RangeMatrix(series) = engine
            .query_range(
                &tenant_id("tenant-a"),
                case.query,
                60_000,
                180_000,
                millis(60_000),
            )
            .await
            .unwrap()
        else {
            panic!("expected matrix for {}", case.query);
        };
        assert2::assert!(series.len() == 1);
        assert2::assert!(
            series[0]
                .samples
                .iter()
                .map(|sample| float_value(&sample.value))
                .collect::<Vec<_>>()
                == case.expected
        );
    }

    for query in ["start(1)", "end(m)", "restart()", "start_extra()"] {
        assert2::assert!(
            engine
                .query_instant(&tenant_id("tenant-a"), query, 120_000)
                .await
                .is_err()
        );
    }

    let result = engine
        .query_instant(
            &tenant_id("tenant-a"),
            r#"m{note="start() end()"}"#,
            120_000,
        )
        .await
        .unwrap();
    assert2::assert!(result == QueryResult::InstantVector(Vec::new()));
}

}

mod instant_hyperbolic_functions_transform_vector_values {
use super::*;

#[tokio::test]
pub(crate) async fn instant_hyperbolic_functions_transform_vector_values() {
    assert_signed_temperature_cases(&[
        CaseQuery {
            query: "sinh(temperature_celsius)",
            expected: [
                CaseValue::new("neg", (-1.2_f64).sinh()),
                CaseValue::new("zero", 0.0_f64.sinh()),
                CaseValue::new("pos", 1.2_f64.sinh()),
            ],
        },
        CaseQuery {
            query: "cosh(temperature_celsius)",
            expected: [
                CaseValue::new("neg", (-1.2_f64).cosh()),
                CaseValue::new("zero", 0.0_f64.cosh()),
                CaseValue::new("pos", 1.2_f64.cosh()),
            ],
        },
        CaseQuery {
            query: "tanh(temperature_celsius)",
            expected: [
                CaseValue::new("neg", (-1.2_f64).tanh()),
                CaseValue::new("zero", 0.0_f64.tanh()),
                CaseValue::new("pos", 1.2_f64.tanh()),
            ],
        },
        CaseQuery {
            query: "asinh(temperature_celsius)",
            expected: [
                CaseValue::new("neg", (-1.2_f64).asinh()),
                CaseValue::new("zero", 0.0_f64.asinh()),
                CaseValue::new("pos", 1.2_f64.asinh()),
            ],
        },
        CaseQuery {
            query: "acosh(abs(temperature_celsius) + 1)",
            expected: [
                CaseValue::new("neg", 2.2_f64.acosh()),
                CaseValue::new("zero", 1.0_f64.acosh()),
                CaseValue::new("pos", 2.2_f64.acosh()),
            ],
        },
        CaseQuery {
            query: "atanh(temperature_celsius / 2)",
            expected: [
                CaseValue::new("neg", (-0.6_f64).atanh()),
                CaseValue::new("zero", 0.0_f64.atanh()),
                CaseValue::new("pos", 0.6_f64.atanh()),
            ],
        },
    ])
    .await;
}

}

mod instant_idelta_uses_last_two_samples_without_per_second_division {
use super::*;

#[tokio::test]
pub(crate) async fn instant_idelta_uses_last_two_samples_without_per_second_division() {
    let engine = late_jump_engine("temperature_celsius");
    let samples = instant_vector(&engine, "idelta(temperature_celsius[2m])", 90_000).await;
    assert2::assert!(samples.len() == 1);
    assert2::assert!(approx_eq(float_value(&samples[0].value), 2.0));
}

}

mod instant_irate_reads_the_last_two_samples_and_recovers_from_a_reset {
use super::*;

#[derive(bon::Builder)]
struct CounterSample {
    timestamp_ms: i64,
    value: f64,
}

#[derive(bon::Builder)]
struct IrateCase {
    name: &'static str,
    samples: Vec<CounterSample>,
    expected: f64,
}

#[tokio::test]
pub(crate) async fn instant_irate_reads_the_last_two_samples_and_recovers_from_a_reset() {
    for case in [
        IrateCase::builder()
            .name("two samples are enough")
            .samples(vec![
                CounterSample::builder().timestamp_ms(0).value(10.0).build(),
                CounterSample::builder()
                    .timestamp_ms(60_000)
                    .value(70.0)
                    .build(),
            ])
            .expected(1.0)
            .build(),
        IrateCase::builder()
            .name("a flat counter rates to zero")
            .samples(vec![
                CounterSample::builder().timestamp_ms(0).value(10.0).build(),
                CounterSample::builder()
                    .timestamp_ms(60_000)
                    .value(10.0)
                    .build(),
            ])
            .expected(0.0)
            .build(),
        IrateCase::builder()
            .name("a reset takes the last value alone")
            .samples(vec![
                CounterSample::builder()
                    .timestamp_ms(0)
                    .value(100.0)
                    .build(),
                CounterSample::builder()
                    .timestamp_ms(60_000)
                    .value(10.0)
                    .build(),
            ])
            .expected(10.0 / 60.0)
            .build(),
    ] {
        let mut store = InMemoryMetricStore::new();
        for sample in case.samples {
            store.push_float(
                "tenant-a",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
                    crate::test_support::TestLabelPair::new("job", "api"),
                ]),
                sample.timestamp_ms,
                sample.value,
            );
        }
        // A native histogram under the same name routes the selector through
        // the interpreter's range kernel rather than the operator leaf. It
        // folds to nothing itself -- `irate` wants floats -- so the one series
        // that comes back is the counter.
        store.push_histogram(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "http_requests_total"),
                crate::test_support::TestLabelPair::new("job", "cache"),
            ]),
            60_000,
            native_histogram(4.0, 1.0),
        );

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let samples = instant_vector(&engine, "irate(http_requests_total[5m])", 60_000).await;
        check!(samples.len() == 1, "{}", case.name);
        check!(
            approx_eq(float_value(&samples[0].value), case.expected),
            "{}: {}",
            case.name,
            float_value(&samples[0].value)
        );
    }
}

}

mod instant_irate_uses_last_two_samples_per_second {
use super::*;

#[tokio::test]
pub(crate) async fn instant_irate_uses_last_two_samples_per_second() {
    let engine = late_jump_engine("http_requests_total");
    let samples = instant_vector(&engine, "irate(http_requests_total[2m])", 90_000).await;
    assert2::assert!(samples.len() == 1);
    assert2::assert!(approx_eq(float_value(&samples[0].value), 2.0 / 30.0));
}

}

mod instant_label_join_combines_source_labels {
use super::*;

#[tokio::test]
pub(crate) async fn instant_label_join_combines_source_labels() {
    let engine = FloatStore::default()
        .named_sample(
            "up",
            &[
                crate::test_support::TestLabelPair::new("job", "api"),
                crate::test_support::TestLabelPair::new("instance", "a"),
                crate::test_support::TestLabelPair::new("zone", "us-east-1a"),
            ],
            1.0,
        )
        .engine();
    assert_label_transform(
        &engine,
        r#"label_join(up, "target", "/", "job", "instance")"#,
        "target",
        "api/a",
        "zone",
        "us-east-1a",
    )
    .await;
}

}

mod instant_over_time_accepts_subquery_range_argument {
use super::*;

#[tokio::test]
pub(crate) async fn instant_over_time_accepts_subquery_range_argument() {
    let engine = queue_depth_engine();
    let samples = instant_vector(&engine, "avg_over_time((queue_depth * 2)[2m:1m])", 120_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        5.0,
    );
}

}

mod instant_over_time_windows_are_open_at_the_start_and_closed_at_the_end {
use super::*;

#[derive(bon::Builder)]
struct HistogramPoint {
    timestamp_ms: i64,
    sum: f64,
}

struct QueryExpectation {
    query: &'static str,
    expected: f64,
}

#[tokio::test]
pub(crate) async fn instant_over_time_windows_are_open_at_the_start_and_closed_at_the_end() {
    let mut store = InMemoryMetricStore::new();
    let points = [
        HistogramPoint::builder().timestamp_ms(0).sum(1.0).build(),
        HistogramPoint::builder()
            .timestamp_ms(10_000)
            .sum(2.0)
            .build(),
        HistogramPoint::builder()
            .timestamp_ms(60_000)
            .sum(3.0)
            .build(),
    ];
    for point_index in 0..points.len() {
        let point = &points[point_index];
        store.push_histogram(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "queue_depth"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]),
            point.timestamp_ms,
            native_histogram(4.0, point.sum),
        );
    }

    // A `[50s]` range ending at 60s starts at exactly 10s. Prometheus' window
    // is half-open, so the sample sitting on that start is out and only the
    // one at 60s is in. This pins that boundary as engine behaviour rather
    // than as a property of whichever layer happens to apply the trim.
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let queries = [
        QueryExpectation {
            query: "histogram_sum(first_over_time(queue_depth[50s]))",
            expected: 3.0,
        },
        QueryExpectation {
            query: "histogram_sum(last_over_time(queue_depth[50s]))",
            expected: 3.0,
        },
        QueryExpectation {
            query: "count_over_time(queue_depth[50s])",
            expected: 1.0,
        },
    ];
    for query_index in 0..queries.len() {
        let query = &queries[query_index];
        let samples = instant_vector(&engine, query.query, 60_000).await;
        check!(samples.len() == 1, "{}", query.query);
        check!(
            approx_eq(float_value(&samples[0].value), query.expected),
            "{}: {}",
            query.query,
            float_value(&samples[0].value)
        );
    }
}

}

mod instant_predict_linear_extrapolates_gauge_series {
use super::*;

#[tokio::test]
pub(crate) async fn instant_predict_linear_extrapolates_gauge_series() {
    let store = three_point_gauge_store("disk_free_bytes");
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "predict_linear(disk_free_bytes[2m], 60)", 120_000).await;
    assert_one_unnamed_float(
        &samples,
        ExpectedLabel {
            name: "job",
            label_value: "api",
        },
        7.0,
    );
    // The same series over a window that admits all three samples. `[2m]`
    // starts exactly at the first one, and the window excludes its own start,
    // so only two ever reached the regression -- leaving a guard that refused
    // three samples with nothing to refuse.
    let QueryResult::InstantVector(samples) = engine
        .query_instant(
            &tenant_id("tenant-a"),
            "predict_linear(disk_free_bytes[3m], 60)",
            120_000,
        )
        .await
        .expect("a prediction")
    else {
        panic!("expected a vector");
    };
    check!(samples.len() == 1, "three samples still predict");
    check!(approx_eq(float_value(&samples[0].value), 7.0));
}

}

mod instant_sort_functions_order_vector_by_sample_value {
use super::*;

struct SortOrderExpectation {
    query: &'static str,
    instances: [&'static str; 3],
}

#[tokio::test]
pub(crate) async fn instant_sort_functions_order_vector_by_sample_value() {
    let engine = zoned_queue_depth_engine(&standard_zoned_queue_depth_series());
    for expectation in [
        SortOrderExpectation {
            query: "sort(queue_depth)",
            instances: ["api-a", "api-c", "api-b"],
        },
        SortOrderExpectation {
            query: "sort_desc(queue_depth)",
            instances: ["api-b", "api-c", "api-a"],
        },
        SortOrderExpectation {
            query: r#"sort_by_label(queue_depth, "zone", "instance")"#,
            instances: ["api-a", "api-c", "api-b"],
        },
        SortOrderExpectation {
            query: r#"sort_by_label_desc(queue_depth, "zone", "instance")"#,
            instances: ["api-b", "api-c", "api-a"],
        },
    ] {
        let samples = instant_vector(&engine, expectation.query, 10_000).await;
        assert2::assert!(samples.len() == 3);
        let instances = samples
            .iter()
            .map(|sample| sample.labels.get("instance").unwrap().to_string())
            .collect::<Vec<_>>();
        assert2::assert!(instances == expectation.instances);
        assert2::assert!(
            samples
                .iter()
                .all(|sample| sample.labels.get("__name__") == Some("queue_depth"))
        );
    }
}

}

mod instant_sort_functions_place_nan_last {
use super::*;

struct SortExpectation {
    query: &'static str,
    instances: [&'static str; 4],
}

/// Prometheus sends a `NaN` to the bottom under `sort` and under `sort_desc`.
/// A `NaN` is neither the largest value nor the smallest one, so no direction
/// puts it first. The `sort_by_label` family orders by label and never reads
/// the value, so the `NaN` series takes the position its labels give it.
#[tokio::test]
pub(crate) async fn instant_sort_functions_place_nan_last() {
    let mut series = standard_zoned_queue_depth_series().to_vec();
    series.push(ZonedQueueDepth {
        instance: "api-n",
        zone: "us-east-1a",
        depth: f64::NAN,
    });
    let engine = zoned_queue_depth_engine(&series);
    for expectation in [
        SortExpectation {
            query: "sort(queue_depth)",
            instances: ["api-a", "api-c", "api-b", "api-n"],
        },
        SortExpectation {
            query: "sort_desc(queue_depth)",
            instances: ["api-b", "api-c", "api-a", "api-n"],
        },
        SortExpectation {
            query: r#"sort_by_label(queue_depth, "zone", "instance")"#,
            instances: ["api-a", "api-c", "api-n", "api-b"],
        },
        SortExpectation {
            query: r#"sort_by_label_desc(queue_depth, "zone", "instance")"#,
            instances: ["api-b", "api-n", "api-c", "api-a"],
        },
    ] {
        let samples = instant_vector(&engine, expectation.query, 10_000).await;
        let instances = samples
            .iter()
            .map(|sample| sample.labels.get("instance").unwrap())
            .collect::<Vec<_>>();
        assert2::assert!(instances == expectation.instances);
        let nan_position = instances
            .iter()
            .position(|instance| *instance == "api-n")
            .unwrap();
        assert2::assert!(
            matches!(samples[nan_position].value, SampleValue::Float(value) if value.is_nan())
        );
    }
}

}

mod instant_statistical_over_time_functions_reduce_range_samples {
use super::*;

struct StatisticalExpectation {
    query: &'static str,
    value: f64,
}

#[tokio::test]
pub(crate) async fn instant_statistical_over_time_functions_reduce_range_samples() {
    let store = SeriesFixture::new(labels(&[
        crate::test_support::TestLabelPair::new("__name__", "latency_seconds"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]))
    .at(0_i64, 2.0)
    .at(60_000, 4.0)
    .at(120_000, 4.0)
    .at(180_000, 4.0)
    .at(240_000, 5.0)
    .at(300_000, 5.0)
    .at(360_000, 7.0)
    .at(420_000, 9.0)
    .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for expectation in [
        StatisticalExpectation {
            query: "stdvar_over_time(latency_seconds[8m])",
            value: 4.0,
        },
        StatisticalExpectation {
            query: "stddev_over_time(latency_seconds[8m])",
            value: 2.0,
        },
        StatisticalExpectation {
            query: "quantile_over_time(0.5, latency_seconds[8m])",
            value: 4.5,
        },
        StatisticalExpectation {
            query: "mad_over_time(latency_seconds[8m])",
            value: 0.5,
        },
    ] {
        let samples = instant_vector(&engine, expectation.query, 420_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            expectation.value,
        );
    }
}

}

mod instant_time_returns_evaluation_timestamp_seconds {
use super::*;

#[tokio::test]
pub(crate) async fn instant_time_returns_evaluation_timestamp_seconds() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_instant(&tenant_id("tenant-a"), "time()", 123_456)
        .await
        .unwrap();

    assert2::assert!(
        result
            == QueryResult::Scalar {
                ts_ms: 123_456,
                value: 123.456
            }
    );
}

}

mod instant_timestamp_returns_sample_timestamp_seconds {
use super::*;

#[tokio::test]
pub(crate) async fn instant_timestamp_returns_sample_timestamp_seconds() {
    let store = crate::test_support::up_api_store("tenant-a", &[(60_000, 1.0)]);

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "timestamp(up)", 120_000).await;
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(samples[0].labels.get("job") == Some("api"));
    check!(samples[0].ts_ms == 120_000);
    check!(approx_eq(float_value(&samples[0].value), 60.0));
}

}

mod instant_trigonometric_functions_transform_vector_values {
use super::*;

struct TrigonometricQueryCase {
    query: &'static str,
    expected: [CaseValue; 3],
}

#[tokio::test]
pub(crate) async fn instant_trigonometric_functions_transform_vector_values() {
    let mut store = InMemoryMetricStore::new();
    push_cases(
        &mut store,
        "angle_radians",
        &[
            CaseValue::new("zero", 0.0),
            CaseValue::new("half_pi", std::f64::consts::FRAC_PI_2),
            CaseValue::new("pi", std::f64::consts::PI),
        ],
    );
    push_cases(
        &mut store,
        "unit_value",
        &[
            CaseValue::new("neg", -0.5),
            CaseValue::new("zero", 0.0),
            CaseValue::new("pos", 0.5),
        ],
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for case in [
        TrigonometricQueryCase {
            query: "sin(angle_radians)",
            expected: [
                CaseValue::new("zero", 0.0_f64.sin()),
                CaseValue::new("half_pi", std::f64::consts::FRAC_PI_2.sin()),
                CaseValue::new("pi", std::f64::consts::PI.sin()),
            ],
        },
        TrigonometricQueryCase {
            query: "cos(angle_radians)",
            expected: [
                CaseValue::new("zero", 0.0_f64.cos()),
                CaseValue::new("half_pi", std::f64::consts::FRAC_PI_2.cos()),
                CaseValue::new("pi", std::f64::consts::PI.cos()),
            ],
        },
        TrigonometricQueryCase {
            query: "tan(angle_radians)",
            expected: [
                CaseValue::new("zero", 0.0_f64.tan()),
                CaseValue::new("half_pi", std::f64::consts::FRAC_PI_2.tan()),
                CaseValue::new("pi", std::f64::consts::PI.tan()),
            ],
        },
        TrigonometricQueryCase {
            query: "deg(angle_radians)",
            expected: [
                CaseValue::new("zero", 0.0),
                CaseValue::new("half_pi", 90.0),
                CaseValue::new("pi", 180.0),
            ],
        },
        TrigonometricQueryCase {
            query: "rad(deg(angle_radians))",
            expected: [
                CaseValue::new("zero", 0.0),
                CaseValue::new("half_pi", std::f64::consts::FRAC_PI_2),
                CaseValue::new("pi", std::f64::consts::PI),
            ],
        },
        TrigonometricQueryCase {
            query: "asin(unit_value)",
            expected: [
                CaseValue::new("neg", (-0.5_f64).asin()),
                CaseValue::new("zero", 0.0_f64.asin()),
                CaseValue::new("pos", 0.5_f64.asin()),
            ],
        },
        TrigonometricQueryCase {
            query: "acos(unit_value)",
            expected: [
                CaseValue::new("neg", (-0.5_f64).acos()),
                CaseValue::new("zero", 0.0_f64.acos()),
                CaseValue::new("pos", 0.5_f64.acos()),
            ],
        },
        TrigonometricQueryCase {
            query: "atan(unit_value)",
            expected: [
                CaseValue::new("neg", (-0.5_f64).atan()),
                CaseValue::new("zero", 0.0_f64.atan()),
                CaseValue::new("pos", 0.5_f64.atan()),
            ],
        },
    ] {
        let samples = instant_vector(&engine, case.query, 10_000).await;
        assert_case_values(&samples, &case.expected);
    }
}

}

mod instant_ts_of_over_time_functions_return_sample_timestamps_seconds {
use super::*;

struct TimestampFunctionExpectation {
    query: &'static str,
    timestamp_seconds: f64,
}

#[tokio::test]
pub(crate) async fn instant_ts_of_over_time_functions_return_sample_timestamps_seconds() {
    let store = SeriesFixture::new(labels(&[
        crate::test_support::TestLabelPair::new("__name__", "queue_depth"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ]))
    .at(0_i64, 10.0)
    .at(60_000, 3.0)
    .at(120_000, 7.0)
    .at(180_000, 3.0)
    .at(240_000, 11.0)
    .store();

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for expectation in [
        TimestampFunctionExpectation {
            query: "ts_of_first_over_time(queue_depth[4m])",
            timestamp_seconds: 60.0,
        },
        TimestampFunctionExpectation {
            query: "ts_of_last_over_time(queue_depth[4m])",
            timestamp_seconds: 240.0,
        },
        TimestampFunctionExpectation {
            query: "ts_of_min_over_time(queue_depth[4m])",
            timestamp_seconds: 180.0,
        },
        TimestampFunctionExpectation {
            query: "ts_of_max_over_time(queue_depth[4m])",
            timestamp_seconds: 240.0,
        },
    ] {
        let samples = instant_vector(&engine, expectation.query, 240_000).await;
        assert_one_unnamed_float(
            &samples,
            ExpectedLabel {
                name: "job",
                label_value: "api",
            },
            expectation.timestamp_seconds,
        );
    }
}

}

mod instant_unary_numeric_functions_transform_vector_values {
use super::*;

#[tokio::test]
pub(crate) async fn instant_unary_numeric_functions_transform_vector_values() {
    assert_signed_temperature_cases(&[
        CaseQuery {
            query: "ceil(temperature_celsius)",
            expected: [
                CaseValue::new("neg", -1.0),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 2.0),
            ],
        },
        CaseQuery {
            query: "floor(temperature_celsius)",
            expected: [
                CaseValue::new("neg", -2.0),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 1.0),
            ],
        },
        CaseQuery {
            query: "sgn(temperature_celsius)",
            expected: [
                CaseValue::new("neg", -1.0),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 1.0),
            ],
        },
        CaseQuery {
            query: "abs(temperature_celsius)",
            expected: [
                CaseValue::new("neg", 1.2),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 1.2),
            ],
        },
        CaseQuery {
            query: "sqrt(abs(temperature_celsius))",
            expected: [
                CaseValue::new("neg", 1.2_f64.sqrt()),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 1.2_f64.sqrt()),
            ],
        },
        CaseQuery {
            query: "exp(abs(temperature_celsius))",
            expected: [
                CaseValue::new("neg", 1.2_f64.exp()),
                CaseValue::new("zero", 1.0),
                CaseValue::new("pos", 1.2_f64.exp()),
            ],
        },
        CaseQuery {
            query: "ln(abs(temperature_celsius))",
            expected: [
                CaseValue::new("neg", 1.2_f64.ln()),
                CaseValue::new("zero", f64::NEG_INFINITY),
                CaseValue::new("pos", 1.2_f64.ln()),
            ],
        },
        CaseQuery {
            query: "log2(abs(temperature_celsius))",
            expected: [
                CaseValue::new("neg", 1.2_f64.log2()),
                CaseValue::new("zero", f64::NEG_INFINITY),
                CaseValue::new("pos", 1.2_f64.log2()),
            ],
        },
        CaseQuery {
            query: "log10(abs(temperature_celsius))",
            expected: [
                CaseValue::new("neg", 1.2_f64.log10()),
                CaseValue::new("zero", f64::NEG_INFINITY),
                CaseValue::new("pos", 1.2_f64.log10()),
            ],
        },
        CaseQuery {
            query: "round(temperature_celsius)",
            expected: [
                CaseValue::new("neg", -1.0),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 1.0),
            ],
        },
        CaseQuery {
            query: "round(temperature_celsius, 0.5)",
            expected: [
                CaseValue::new("neg", -1.0),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 1.0),
            ],
        },
    ])
    .await;
}

}
