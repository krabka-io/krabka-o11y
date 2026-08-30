use super::*;

#[tokio::test]
pub(crate) async fn clean_query_raises_no_annotations() {
    let engine = PromqlEngine::new(Arc::new(set_op_store()), EngineOpts::default());
    let (_, annotations) = engine
        .query_instant_with_annotations("tenant-a", "up", 10_000)
        .await
        .expect("query");
    assert2::assert!(annotations.is_empty());
}
