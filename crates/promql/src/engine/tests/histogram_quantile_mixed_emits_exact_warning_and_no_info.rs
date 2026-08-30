use super::*;

#[tokio::test]
pub(crate) async fn histogram_quantile_mixed_emits_exact_warning_and_no_info() {
    let engine = PromqlEngine::new(Arc::new(mixed_histogram_store()), EngineOpts::default());
    let (_, annotations) = engine
        .query_instant_with_annotations("tenant-a", "histogram_quantile(0.8, series)", 0)
        .await
        .expect("query");
    assert2::assert!(annotations == crate::Annotations {
            warnings: vec![
                "PromQL warning: vector contains a mix of classic and native histograms for metric name \"series\""
                    .to_string()
            ],
            infos: vec![],
        });
}
