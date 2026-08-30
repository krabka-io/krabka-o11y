use super::*;

#[tokio::test]
pub(crate) async fn histogram_fraction_mixed_emits_exact_warning() {
    let engine = PromqlEngine::new(Arc::new(mixed_histogram_store()), EngineOpts::default());
    let (_, annotations) = engine
        .query_instant_with_annotations("tenant-a", "histogram_fraction(-Inf, 1, series)", 0)
        .await
        .expect("query");
    assert2::assert!(annotations.warnings.iter().any(|w| w
            == "PromQL warning: vector contains a mix of classic and native histograms for metric name \"series\""));
}
