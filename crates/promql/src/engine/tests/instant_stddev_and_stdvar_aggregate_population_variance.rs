use super::*;

#[tokio::test]
pub(crate) async fn instant_stddev_and_stdvar_aggregate_population_variance() {
    let engine = api_latency_engine(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
    let stdvar_samples = instant_vector(&engine, "stdvar(latency_seconds)", 10_000).await;
    let stddev_samples = instant_vector(&engine, "stddev(latency_seconds)", 10_000).await;
    check!(stdvar_samples.len() == 1);
    check!(stddev_samples.len() == 1);
    check!(approx_eq(float_value(&stdvar_samples[0].value), 4.0));
    check!(approx_eq(float_value(&stddev_samples[0].value), 2.0));
}
