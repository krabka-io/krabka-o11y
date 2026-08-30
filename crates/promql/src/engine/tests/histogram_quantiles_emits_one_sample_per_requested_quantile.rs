use super::*;

#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn histogram_quantiles_emits_one_sample_per_requested_quantile() {
    let mut store = InMemoryMetricStore::new();
    for (le, value) in [("0.1", 0.0), ("0.2", 1.0), ("0.4", 3.0), ("+Inf", 3.0)] {
        store.push_float(
            "tenant-a",
            labels(&[
                ("__name__", "http_request_duration_seconds_bucket"),
                ("job", "api"),
                ("le", le),
            ]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant(
            "tenant-a",
            r#"histogram_quantiles(http_request_duration_seconds_bucket, "quantile", 0.5, 0.9)"#,
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    assert2::assert!(samples.len() == 2);
    let values = samples
        .iter()
        .map(|sample| {
            (
                sample.labels.get("quantile").expect("quantile label"),
                float_value(&sample.value),
            )
        })
        .collect::<BTreeMap<_, _>>();
    check!(approx_eq(*values.get("0.5").expect("p50 sample"), 0.25));
    check!(approx_eq(*values.get("0.9").expect("p90 sample"), 0.37));
    check!(samples.iter().all(|sample| {
        sample.labels.get("__name__").is_none()
            && sample.labels.get("job") == Some("api")
            && sample.labels.get("le").is_none()
    }));
}
