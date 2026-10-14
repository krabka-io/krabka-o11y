use super::*;

#[tokio::test]
pub(crate) async fn instant_topk_keeps_largest_samples_with_original_labels() {
    let engine = memory_bytes_engine();
    let samples = instant_vector(&engine, "topk(2, memory_bytes)", 10_000).await;
    let projection = topk_projection(&samples);
    assert2::assert!(projection.len() == 2);
    assert2::assert!(projection[0].metric_name == Some("memory_bytes"));
    assert2::assert!(projection[0].instance == Some("b"));
    assert2::assert!(projection[0].value == 3.0);
    assert2::assert!(projection[1].metric_name == Some("memory_bytes"));
    assert2::assert!(projection[1].instance == Some("c"));
    assert2::assert!(projection[1].value == 2.0);
}
