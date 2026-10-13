use super::*;

#[tokio::test]
pub(crate) async fn histogram_float_comparison_emits_incompatible_types_info() {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "h"),
            crate::test_support::TestLabelPair::new("job", "app"),
        ]),
        0,
        native_histogram(4.0, 5.0),
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let annotated = engine
        .query_instant_with_annotations(&tenant_id("tenant-a"), "h > 80", 0)
        .await
        .expect("query");
    assert2::assert!(matches!(annotated.result, QueryResult::InstantVector(ref v) if v.is_empty()));
    assert2::assert!(annotated.annotations == crate::Annotations {
            infos: vec![
                "PromQL info: incompatible sample types encountered for binary operator \">\": histogram > float (1:1)"
                    .to_string()
            ],
            warnings: vec![],
            ..crate::Annotations::default()
        });
}
