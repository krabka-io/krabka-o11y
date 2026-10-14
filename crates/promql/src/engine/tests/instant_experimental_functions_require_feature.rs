#[cfg(not(feature = "experimental-functions"))]
use super::*;

#[cfg(not(feature = "experimental-functions"))]
#[yare::parameterized(
    limitk = { "limitk(2, memory_bytes)", 10_000 },
    limit_ratio = { "limit_ratio(0.5, memory_bytes)", 10_000 },
    double_exponential_smoothing = {
        "double_exponential_smoothing(gauge[5m], 0.5, 0.5)",
        120_000
    },
)]
pub(crate) fn instant_functions_require_experimental_feature(query: &str, time_ms: i64) {
    let store = InMemoryMetricStore::new();
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("the async test runtime builds");
    let error = runtime.block_on(async {
        engine
            .query_instant(&tenant_id("tenant-a"), query, time_ms)
            .await
            .unwrap_err()
    });

    assert2::assert!(matches!(error, PromqlError::Unsupported(_)));
    assert2::assert!(format!("{error}").contains("experimental-functions"));
}
