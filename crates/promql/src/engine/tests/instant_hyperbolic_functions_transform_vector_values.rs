use super::*;

#[tokio::test]
pub(crate) async fn instant_hyperbolic_functions_transform_vector_values() {
    let mut store = InMemoryMetricStore::new();
    for (case, value) in [("neg", -1.2), ("zero", 0.0), ("pos", 1.2)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "temperature_celsius"), ("case", case)]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        (
            "sinh(temperature_celsius)",
            [
                ("neg", (-1.2_f64).sinh()),
                ("zero", 0.0_f64.sinh()),
                ("pos", 1.2_f64.sinh()),
            ],
        ),
        (
            "cosh(temperature_celsius)",
            [
                ("neg", (-1.2_f64).cosh()),
                ("zero", 0.0_f64.cosh()),
                ("pos", 1.2_f64.cosh()),
            ],
        ),
        (
            "tanh(temperature_celsius)",
            [
                ("neg", (-1.2_f64).tanh()),
                ("zero", 0.0_f64.tanh()),
                ("pos", 1.2_f64.tanh()),
            ],
        ),
        (
            "asinh(temperature_celsius)",
            [
                ("neg", (-1.2_f64).asinh()),
                ("zero", 0.0_f64.asinh()),
                ("pos", 1.2_f64.asinh()),
            ],
        ),
        (
            "acosh(abs(temperature_celsius) + 1)",
            [
                ("neg", 2.2_f64.acosh()),
                ("zero", 1.0_f64.acosh()),
                ("pos", 2.2_f64.acosh()),
            ],
        ),
        (
            "atanh(temperature_celsius / 2)",
            [
                ("neg", (-0.6_f64).atanh()),
                ("zero", 0.0_f64.atanh()),
                ("pos", 0.6_f64.atanh()),
            ],
        ),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap();
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected vector");
        };
        assert2::assert!(samples.len() == 3);
        for (case, value) in expected {
            let sample = samples
                .iter()
                .find(|sample| sample.labels.get("case") == Some(case))
                .expect("sample for case");
            assert2::assert!(sample.labels.get("__name__") == None);
            assert2::assert!(approx_eq(float_value(&sample.value), value));
        }
    }
}
