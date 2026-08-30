use super::*;

#[tokio::test]
pub(crate) async fn instant_subquery_aligns_start_to_step_grid() {
    let mut store = InMemoryMetricStore::new();
    for (index, value) in [
        1.0, 1.0, 2.0, 3.0, 5.0, 8.0, 13.0, 21.0, 34.0, 55.0, 89.0, 144.0,
    ]
    .into_iter()
    .enumerate()
    {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "metric_total")]),
            i64::try_from(index).unwrap() * 7_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "rate(metric_total[1m500ms:10s])", 80_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 1);
    assert2::assert!(approx_eq(
        float_value(&samples[0].value),
        2.366_666_666_666_666_7,
    ));
}
