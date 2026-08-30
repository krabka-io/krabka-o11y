use super::*;

#[tokio::test]
pub(crate) async fn vector_unless_keeps_left_samples_without_matching_right_key() {
    let engine = PromqlEngine::new(Arc::new(set_op_store()), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "up unless on (instance) target_info", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__") == Some("up"));
    check!(samples[0].labels.get("instance") == Some("a"));
    check!(approx_eq(float_value(&samples[0].value), 1.0));
}
