#[cfg(feature = "experimental-functions")]
use super::*;

#[cfg(feature = "experimental-functions")]
#[derive(bon::Builder)]
struct ScalarExpectation {
    query: &'static str,
    value: f64,
}

#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn scalar_max_of_min_of_return_larger_and_smaller_scalar() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    for expected in [
        ScalarExpectation::builder()
            .query("max_of(1, 2)")
            .value(2.0)
            .build(),
        ScalarExpectation::builder()
            .query("min_of(1, 2)")
            .value(1.0)
            .build(),
    ] {
        let result = engine
            .query_instant(&tenant_id("tenant-a"), expected.query, 10_000)
            .await
            .unwrap();
        assert2::assert!(
            result
                == QueryResult::Scalar {
                    ts_ms: 10_000,
                    value: expected.value,
                }
        );
    }
    for query in [
        "max_of(NaN, 2)",
        "max_of(2, NaN)",
        "min_of(NaN, 2)",
        "min_of(2, NaN)",
    ] {
        let result = engine
            .query_instant(&tenant_id("tenant-a"), query, 10_000)
            .await
            .unwrap();
        let QueryResult::Scalar { ts_ms, value } = result else {
            panic!("expected scalar for {query}");
        };
        assert2::assert!(ts_ms == 10_000, "{query}");
        assert2::assert!(value.is_nan(), "{query}: {value}");
    }
}
