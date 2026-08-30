use super::*;

#[tokio::test]
pub(crate) async fn instant_absent_over_time_with_or_matchers_returns_unlabeled_absence_sample() {
    let engine = PromqlEngine::new(Arc::new(InMemoryMetricStore::new()), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            r#"absent_over_time(up{job="api" or job="web"}[1m])"#,
            120_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.is_empty());
    check!(approx_eq(float_value(&samples[0].value), 1.0));
}
