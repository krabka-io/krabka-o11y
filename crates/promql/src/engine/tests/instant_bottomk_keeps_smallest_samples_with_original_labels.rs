use super::*;

#[tokio::test]
pub(crate) async fn instant_bottomk_keeps_smallest_samples_with_original_labels() {
    let engine = memory_bytes_engine();
    let samples = instant_vector(&engine, "bottomk(2, memory_bytes)", 10_000).await;
    check!(samples.len() == 2);
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("memory_bytes")
            && sample.labels.get("instance") == Some("a")
            && approx_eq(float_value(&sample.value), 1.0)
    }));
    check!(samples.iter().any(|sample| {
        sample.labels.get("__name__") == Some("memory_bytes")
            && sample.labels.get("instance") == Some("c")
            && approx_eq(float_value(&sample.value), 2.0)
    }));
}
