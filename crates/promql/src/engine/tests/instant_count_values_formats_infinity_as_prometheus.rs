use super::*;

/// M19: `count_values` formats a non-finite value with the Prometheus formatter.
///
/// `count_values` uses the canonical Prometheus float formatter, so the label
/// value is `+Inf`, not the `inf` that `f64::to_string` returns.
#[tokio::test]
pub(crate) async fn instant_count_values_formats_infinity_as_prometheus() {
    let mut store = InMemoryMetricStore::new();
    for instance in 0..2 {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "ratio"),
                ("job", "api"),
                ("instance", &instance.to_string()),
            ]),
            10_000,
            f64::INFINITY,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let QueryResult::InstantVector(samples) = engine
        .query_instant("tenant-a", r#"count_values("v", ratio)"#, 10_000)
        .await
        .unwrap()
    else {
        panic!("expected vector");
    };
    check!(samples.len() == 1);
    check!(
        samples[0].labels.get("v") == Some("+Inf"),
        "count_values must render +Inf, got {:?}",
        samples[0].labels.get("v")
    );
    check!(approx_eq(float_value(&samples[0].value), 2.0));
}
