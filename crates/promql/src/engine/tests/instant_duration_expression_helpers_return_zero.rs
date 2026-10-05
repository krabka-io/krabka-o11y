#[cfg(feature = "experimental-functions")]
use super::*;

#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn instant_duration_expression_helpers_use_the_evaluation_time() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());

    for (query, expected) in [
        ("range()", 0.0),
        ("step()", 0.0),
        ("start()", 120.0),
        ("end()", 120.0),
        ("(start() + end()) / 2", 120.0),
        ("start ( ) - end ( )", 0.0),
    ] {
        let result = engine
            .query_instant(&tenant_id("tenant-a"), query, 120_000)
            .await
            .unwrap();

        assert2::assert!(
            result
                == QueryResult::Scalar {
                    ts_ms: 120_000,
                    value: expected,
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
    let mut store = InMemoryMetricStore::new();
    for (ts_ms, value) in [(60_000, 2.0), (120_000, 3.0), (180_000, 4.0)] {
        store.push_float("tenant-a", labels(&[("__name__", "m")]), ts_ms, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        ("m * (end() - start())", vec![240.0, 360.0, 480.0]),
        ("m @ start() + vector(end())", vec![182.0; 3]),
        ("m @ end() + vector(start())", vec![64.0; 3]),
    ] {
        let QueryResult::RangeMatrix(series) = engine
            .query_range(
                &tenant_id("tenant-a"),
                query,
                60_000,
                180_000,
                millis(60_000),
            )
            .await
            .unwrap()
        else {
            panic!("expected matrix for {query}");
        };
        assert2::assert!(series.len() == 1);
        assert2::assert!(
            series[0]
                .samples
                .iter()
                .map(|(_, value)| float_value(value))
                .collect::<Vec<_>>()
                == expected
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
