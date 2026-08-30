use super::*;

pub(crate) async fn eval_instant_err(
    store: &InMemoryMetricStore,
    query: &str,
    ts_ms: i64,
) -> Result<QueryResult> {
    let engine = PromqlEngine::new(Arc::new(store.clone()), EngineOpts::default());
    engine.query_instant(TENANT, query, ts_ms).await
}
