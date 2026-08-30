use super::*;

#[tokio::test]
pub(crate) async fn vector_or_returns_left_union_unmatched_right_samples() {
    let engine = PromqlEngine::new(Arc::new(set_op_store()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "up or on (instance) target_info", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 3);
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("up") && sample.labels.get("instance") == Some("a")
    }));
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("up") && sample.labels.get("instance") == Some("b")
    }));
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("target_info")
            && sample.labels.get("instance") == Some("c")
            && sample.labels.get("region") == Some("east")
            && approx_eq(float_value(&sample.value), 30.0)
    }));
}
