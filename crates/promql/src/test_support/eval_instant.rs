use super::*;

pub(crate) async fn eval_instant(
    store: &InMemoryMetricStore,
    query: &str,
    ts_ms: i64,
) -> QueryResult {
    eval_instant_err(store, query, ts_ms).await.unwrap()
}
