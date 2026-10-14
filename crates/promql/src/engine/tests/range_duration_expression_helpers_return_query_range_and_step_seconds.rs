#[cfg(feature = "experimental-functions")]
use super::*;

#[cfg(feature = "experimental-functions")]
struct DurationHelperCase {
    query: &'static str,
    expected: f64,
}

#[cfg(feature = "experimental-functions")]
#[derive(bon::Builder)]
struct DurationProbeSample {
    timestamp_ms: i64,
    value: f64,
}

#[cfg(feature = "experimental-functions")]
struct NestedDurationCase {
    query: &'static str,
    expected: Vec<f64>,
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn range_duration_expression_helpers_return_query_range_and_step_seconds() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());

    for case in [
        DurationHelperCase {
            query: "range()",
            expected: 120.0,
        },
        DurationHelperCase {
            query: "step()",
            expected: 30.0,
        },
        DurationHelperCase {
            query: "start()",
            expected: 60.0,
        },
        DurationHelperCase {
            query: "end()",
            expected: 180.0,
        },
    ] {
        let series = query_range_matrix(&engine, case.query, 30_000).await;
        assert2::assert!(series.len() == 1);
        assert2::assert!(series[0].labels.len() == 0);
        assert2::assert!(
            series[0]
                .samples
                .iter()
                .map(|sample| float_value(&sample.value))
                .collect::<Vec<_>>()
                == vec![case.expected; 5]
        );
    }
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
async fn nested_duration_extrema_select_the_requested_windows_and_offsets() {
    let mut store = InMemoryMetricStore::new();
    for sample in [
        DurationProbeSample::builder()
            .timestamp_ms(0)
            .value(1.0)
            .build(),
        DurationProbeSample::builder()
            .timestamp_ms(30_000)
            .value(2.0)
            .build(),
        DurationProbeSample::builder()
            .timestamp_ms(60_000)
            .value(3.0)
            .build(),
        DurationProbeSample::builder()
            .timestamp_ms(90_000)
            .value(4.0)
            .build(),
        DurationProbeSample::builder()
            .timestamp_ms(120_000)
            .value(5.0)
            .build(),
        DurationProbeSample::builder()
            .timestamp_ms(150_000)
            .value(6.0)
            .build(),
        DurationProbeSample::builder()
            .timestamp_ms(180_000)
            .value(7.0)
            .build(),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new(
                "__name__",
                "duration_probe",
            )]),
            sample.timestamp_ms,
            sample.value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for case in [
        NestedDurationCase {
            query: "sum_over_time(duration_probe[min_of(max_of(step()*2, 45s), range()/2)])",
            expected: vec![5.0, 7.0, 9.0, 11.0, 13.0],
        },
        NestedDurationCase {
            query: "sum_over_time(duration_probe[max_of(min_of(step(), range()), 90s)])",
            expected: vec![6.0, 9.0, 12.0, 15.0, 18.0],
        },
        NestedDurationCase {
            query: "sum_over_time(duration_probe[step()] offset (min_of(max_of(step(), 10s), range())))",
            expected: vec![2.0, 3.0, 4.0, 5.0, 6.0],
        },
    ] {
        let result = engine
            .query_range(
                &tenant_id("tenant-a"),
                case.query,
                60_000,
                180_000,
                millis(30_000),
            )
            .await
            .unwrap();
        let QueryResult::RangeMatrix(series) = result else {
            panic!("expected matrix for {}", case.query);
        };
        assert2::assert!(series.len() == 1, "{}", case.query);
        assert2::assert!(
            series[0]
                .samples
                .iter()
                .map(|sample| float_value(&sample.value))
                .collect::<Vec<_>>()
                == case.expected,
            "{}",
            case.query
        );
    }
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
async fn duration_extrema_reject_literal_division_by_zero_in_either_operand() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    for query in [
        "sum_over_time(duration_probe[min_of(1m, 0/0)])",
        "sum_over_time(duration_probe[min_of(0/0, 1m)])",
        "sum_over_time(duration_probe[max_of(1m, 0/0)])",
        "sum_over_time(duration_probe[max_of(0/0, 1m)])",
    ] {
        let error = engine
            .query_range(
                &tenant_id("tenant-a"),
                query,
                60_000,
                180_000,
                millis(30_000),
            )
            .await
            .unwrap_err();
        assert2::assert!(matches!(error, PromqlError::Parse(_)), "{query}: {error}");
        assert2::assert!(
            error.to_string().contains("division by zero"),
            "{query}: {error}"
        );
    }
}
