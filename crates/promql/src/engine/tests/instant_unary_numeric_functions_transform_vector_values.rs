use super::*;

#[tokio::test]
pub(crate) async fn instant_unary_numeric_functions_transform_vector_values() {
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
            "ceil(temperature_celsius)",
            [
                CaseValue::new("neg", -1.0),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 2.0),
            ],
        ),
        (
            "floor(temperature_celsius)",
            [
                CaseValue::new("neg", -2.0),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 1.0),
            ],
        ),
        (
            "sgn(temperature_celsius)",
            [
                CaseValue::new("neg", -1.0),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 1.0),
            ],
        ),
        (
            "abs(temperature_celsius)",
            [
                CaseValue::new("neg", 1.2),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 1.2),
            ],
        ),
        (
            "sqrt(abs(temperature_celsius))",
            [
                CaseValue::new("neg", 1.2_f64.sqrt()),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 1.2_f64.sqrt()),
            ],
        ),
        (
            "exp(abs(temperature_celsius))",
            [
                CaseValue::new("neg", 1.2_f64.exp()),
                CaseValue::new("zero", 1.0),
                CaseValue::new("pos", 1.2_f64.exp()),
            ],
        ),
        (
            "ln(abs(temperature_celsius))",
            [
                CaseValue::new("neg", 1.2_f64.ln()),
                CaseValue::new("zero", f64::NEG_INFINITY),
                CaseValue::new("pos", 1.2_f64.ln()),
            ],
        ),
        (
            "log2(abs(temperature_celsius))",
            [
                CaseValue::new("neg", 1.2_f64.log2()),
                CaseValue::new("zero", f64::NEG_INFINITY),
                CaseValue::new("pos", 1.2_f64.log2()),
            ],
        ),
        (
            "log10(abs(temperature_celsius))",
            [
                CaseValue::new("neg", 1.2_f64.log10()),
                CaseValue::new("zero", f64::NEG_INFINITY),
                CaseValue::new("pos", 1.2_f64.log10()),
            ],
        ),
        (
            "round(temperature_celsius)",
            [
                CaseValue::new("neg", -1.0),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 1.0),
            ],
        ),
        (
            "round(temperature_celsius, 0.5)",
            [
                CaseValue::new("neg", -1.0),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 1.0),
            ],
        ),
    ] {
        let samples = instant_vector(&engine, query, 10_000).await;
        assert_case_values(&samples, &expected);
    }
}
