#[cfg(feature = "experimental-functions")]
use super::*;

#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn range_duration_expression_helpers_return_query_range_and_step_seconds() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());

    for (query, expected) in [
        ("range()", 120.0),
        ("step()", 30.0),
        ("start()", 60.0),
        ("end()", 180.0),
    ] {
        let result = engine
            .query_range(
                &tenant_id("tenant-a"),
                query,
                60_000,
                180_000,
                millis(30_000),
            )
            .await
            .unwrap();

        let QueryResult::RangeMatrix(series) = result else {
            panic!("expected matrix");
        };
        assert2::assert!(series.len() == 1);
        assert2::assert!(series[0].labels.len() == 0);
        assert2::assert!(
            series[0]
                .samples
                .iter()
                .map(|(_, value)| float_value(value))
                .collect::<Vec<_>>()
                == vec![expected; 5]
        );
    }
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
async fn nested_duration_extrema_select_the_requested_windows_and_offsets() {
    let mut store = InMemoryMetricStore::new();
    for (timestamp, value) in [
        (0, 1.0),
        (30_000, 2.0),
        (60_000, 3.0),
        (90_000, 4.0),
        (120_000, 5.0),
        (150_000, 6.0),
        (180_000, 7.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "duration_probe")]),
            timestamp,
            value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        (
            "sum_over_time(duration_probe[min_of(max_of(step()*2, 45s), range()/2)])",
            vec![5.0, 7.0, 9.0, 11.0, 13.0],
        ),
        (
            "sum_over_time(duration_probe[max_of(min_of(step(), range()), 90s)])",
            vec![6.0, 9.0, 12.0, 15.0, 18.0],
        ),
        (
            "sum_over_time(duration_probe[step()] offset (min_of(max_of(step(), 10s), range())))",
            vec![2.0, 3.0, 4.0, 5.0, 6.0],
        ),
    ] {
        let result = engine
            .query_range(
                &tenant_id("tenant-a"),
                query,
                60_000,
                180_000,
                millis(30_000),
            )
            .await
            .unwrap();
        let QueryResult::RangeMatrix(series) = result else {
            panic!("expected matrix for {query}");
        };
        assert2::assert!(series.len() == 1, "{query}");
        assert2::assert!(
            series[0]
                .samples
                .iter()
                .map(|(_, value)| float_value(value))
                .collect::<Vec<_>>()
                == expected,
            "{query}"
        );
    }
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
async fn duration_extrema_propagate_nan_from_either_operand() {
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
        assert2::assert!(error.to_string().contains("NaN"), "{query}: {error}");
    }
}
