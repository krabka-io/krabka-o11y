use super::*;

#[tokio::test]
pub(crate) async fn comparison_bool_returns_one_or_zero() {
    let engine = FloatStore::default()
        .sample(labels(&[("__name__", "a"), ("x", "1")]), 10.0)
        .engine();
    let samples = instant_vector(&engine, "a > bool 0", 10_000).await;
    check!(samples.len() == 1);
    check!(samples[0].labels.get("__name__").is_none());
    check!(approx_eq(float_value(&samples[0].value), 1.0));
}
