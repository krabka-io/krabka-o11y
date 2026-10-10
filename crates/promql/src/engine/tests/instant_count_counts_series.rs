use super::*;

#[tokio::test]
pub(crate) async fn instant_count_counts_series() {
    let engine = FloatStore::default()
        .sample(labels(&[("__name__", "up"), ("job", "api")]), 1.0)
        .sample(labels(&[("__name__", "up"), ("job", "web")]), 0.0)
        .engine();
    let samples = instant_vector(&engine, "count(up)", 10_000).await;
    assert2::assert!(samples.len() == 1);
    assert2::assert!(approx_eq(float_value(&samples[0].value), 2.0));
}
