use super::*;

#[tokio::test]
pub(crate) async fn instant_hyperbolic_functions_transform_vector_values() {
    let mut store = InMemoryMetricStore::new();
    push_cases(
        &mut store,
        "temperature_celsius",
        &[
            CaseValue::new("neg", -1.2),
            CaseValue::new("zero", 0.0),
            CaseValue::new("pos", 1.2),
        ],
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        (
            "sinh(temperature_celsius)",
            [
                CaseValue::new("neg", (-1.2_f64).sinh()),
                CaseValue::new("zero", 0.0_f64.sinh()),
                CaseValue::new("pos", 1.2_f64.sinh()),
            ],
        ),
        (
            "cosh(temperature_celsius)",
            [
                CaseValue::new("neg", (-1.2_f64).cosh()),
                CaseValue::new("zero", 0.0_f64.cosh()),
                CaseValue::new("pos", 1.2_f64.cosh()),
            ],
        ),
        (
            "tanh(temperature_celsius)",
            [
                CaseValue::new("neg", (-1.2_f64).tanh()),
                CaseValue::new("zero", 0.0_f64.tanh()),
                CaseValue::new("pos", 1.2_f64.tanh()),
            ],
        ),
        (
            "asinh(temperature_celsius)",
            [
                CaseValue::new("neg", (-1.2_f64).asinh()),
                CaseValue::new("zero", 0.0_f64.asinh()),
                CaseValue::new("pos", 1.2_f64.asinh()),
            ],
        ),
        (
            "acosh(abs(temperature_celsius) + 1)",
            [
                CaseValue::new("neg", 2.2_f64.acosh()),
                CaseValue::new("zero", 1.0_f64.acosh()),
                CaseValue::new("pos", 2.2_f64.acosh()),
            ],
        ),
        (
            "atanh(temperature_celsius / 2)",
            [
                CaseValue::new("neg", (-0.6_f64).atanh()),
                CaseValue::new("zero", 0.0_f64.atanh()),
                CaseValue::new("pos", 0.6_f64.atanh()),
            ],
        ),
    ] {
        let samples = instant_vector(&engine, query, 10_000).await;
        assert_case_values(&samples, &expected);
    }
}
