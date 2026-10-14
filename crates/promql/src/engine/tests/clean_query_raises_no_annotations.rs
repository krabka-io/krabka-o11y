use super::*;

#[tokio::test]
pub(crate) async fn clean_query_raises_no_annotations() {
    let engine = PromqlEngine::new(Arc::new(set_op_store()), EngineOpts::default());
    let annotated = engine
        .query_instant_with_annotations(&tenant_id("tenant-a"), "up", 10_000)
        .await
        .expect("query");
    assert2::assert!(annotated.annotations.is_empty());
}
