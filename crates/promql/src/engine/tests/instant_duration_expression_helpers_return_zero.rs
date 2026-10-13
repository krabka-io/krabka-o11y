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
