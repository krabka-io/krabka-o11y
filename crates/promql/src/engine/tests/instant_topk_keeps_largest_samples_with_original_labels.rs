use super::*;

#[tokio::test]
pub(crate) async fn instant_topk_keeps_largest_samples_with_original_labels() {
    let engine = memory_bytes_engine();
    let samples = instant_vector(&engine, "topk(2, memory_bytes)", 10_000).await;
    let mut projection = samples
        .iter()
        .map(|sample| {
            (
                sample.labels.get("__name__"),
                sample.labels.get("instance"),
                float_value(&sample.value),
            )
        })
        .collect::<Vec<_>>();
    projection.sort_by_key(|(_, instance, _)| *instance);
    check!(
        projection
            == vec![
                (Some("memory_bytes"), Some("b"), 3.0),
                (Some("memory_bytes"), Some("c"), 2.0),
            ]
    );
}
