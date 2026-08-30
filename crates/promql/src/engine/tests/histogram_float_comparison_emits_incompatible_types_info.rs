use super::*;

#[tokio::test]
pub(crate) async fn histogram_float_comparison_emits_incompatible_types_info() {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        labels(&[("__name__", "h"), ("job", "app")]),
        0,
        native_histogram(4.0, 5.0),
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let (result, annotations) = engine
        .query_instant_with_annotations("tenant-a", "h > 80", 0)
        .await
        .expect("query");
    assert2::assert!(matches!(result, QueryResult::InstantVector(ref v) if v.is_empty()));
    assert2::assert!(annotations == crate::Annotations {
            infos: vec![
                "PromQL info: incompatible sample types encountered for binary operator \">\": histogram > float"
                    .to_string()
            ],
            warnings: vec![],
        });
}
