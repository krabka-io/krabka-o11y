use super::*;

#[tokio::test]
pub(crate) async fn instant_sum_aggregates_all_series() {
    let engine = FloatStore::default()
        .sample(
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "api"),
            ]),
            1.0,
        )
        .sample(
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "up"),
                crate::test_support::TestLabelPair::new("job", "web"),
            ]),
            2.0,
        )
        .engine();
    let samples = instant_vector(&engine, "sum(up)", 10_000).await;
    check!(samples.len() == 1);
    check!(samples[0].labels.is_empty());
    check!(approx_eq(float_value(&samples[0].value), 3.0));
}
