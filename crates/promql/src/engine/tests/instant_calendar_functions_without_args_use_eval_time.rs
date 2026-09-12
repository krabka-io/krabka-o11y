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
    assert2::assert!(float_value(&samples[0].value) == 1.0);
}
