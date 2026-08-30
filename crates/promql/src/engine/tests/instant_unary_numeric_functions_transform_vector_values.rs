use super::*;

#[tokio::test]
pub(crate) async fn instant_unary_numeric_functions_transform_vector_values() {
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
            "ceil(temperature_celsius)",
            [("neg", -1.0), ("zero", 0.0), ("pos", 2.0)],
        ),
        (
            "floor(temperature_celsius)",
            [("neg", -2.0), ("zero", 0.0), ("pos", 1.0)],
        ),
        (
            "sgn(temperature_celsius)",
            [("neg", -1.0), ("zero", 0.0), ("pos", 1.0)],
        ),
        (
            "abs(temperature_celsius)",
            [("neg", 1.2), ("zero", 0.0), ("pos", 1.2)],
        ),
        (
            "sqrt(abs(temperature_celsius))",
            [
                ("neg", 1.2_f64.sqrt()),
                ("zero", 0.0),
                ("pos", 1.2_f64.sqrt()),
            ],
        ),
        (
            "exp(abs(temperature_celsius))",
            [
                ("neg", 1.2_f64.exp()),
                ("zero", 1.0),
                ("pos", 1.2_f64.exp()),
            ],
        ),
        (
            "ln(abs(temperature_celsius))",
            [
                ("neg", 1.2_f64.ln()),
                ("zero", f64::NEG_INFINITY),
                ("pos", 1.2_f64.ln()),
            ],
        ),
        (
            "log2(abs(temperature_celsius))",
            [
                ("neg", 1.2_f64.log2()),
                ("zero", f64::NEG_INFINITY),
                ("pos", 1.2_f64.log2()),
            ],
        ),
        (
            "log10(abs(temperature_celsius))",
            [
                ("neg", 1.2_f64.log10()),
                ("zero", f64::NEG_INFINITY),
                ("pos", 1.2_f64.log10()),
            ],
        ),
        (
            "round(temperature_celsius)",
            [("neg", -1.0), ("zero", 0.0), ("pos", 1.0)],
        ),
        (
            "round(temperature_celsius, 0.5)",
            [("neg", -1.0), ("zero", 0.0), ("pos", 1.0)],
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
