use super::*;

#[tokio::test]
pub(crate) async fn instant_hyperbolic_functions_transform_vector_values() {
    assert_signed_temperature_cases(&[
        CaseQuery {
            query: "sinh(temperature_celsius)",
            expected: [
                CaseValue::new("neg", (-1.2_f64).sinh()),
                CaseValue::new("zero", 0.0_f64.sinh()),
                CaseValue::new("pos", 1.2_f64.sinh()),
            ],
        },
        CaseQuery {
            query: "cosh(temperature_celsius)",
            expected: [
                CaseValue::new("neg", (-1.2_f64).cosh()),
                CaseValue::new("zero", 0.0_f64.cosh()),
                CaseValue::new("pos", 1.2_f64.cosh()),
            ],
        },
        CaseQuery {
            query: "tanh(temperature_celsius)",
            expected: [
                CaseValue::new("neg", (-1.2_f64).tanh()),
                CaseValue::new("zero", 0.0_f64.tanh()),
                CaseValue::new("pos", 1.2_f64.tanh()),
            ],
        },
        CaseQuery {
            query: "asinh(temperature_celsius)",
            expected: [
                CaseValue::new("neg", (-1.2_f64).asinh()),
                CaseValue::new("zero", 0.0_f64.asinh()),
                CaseValue::new("pos", 1.2_f64.asinh()),
            ],
        },
        CaseQuery {
            query: "acosh(abs(temperature_celsius) + 1)",
            expected: [
                CaseValue::new("neg", 2.2_f64.acosh()),
                CaseValue::new("zero", 1.0_f64.acosh()),
                CaseValue::new("pos", 2.2_f64.acosh()),
            ],
        },
        CaseQuery {
            query: "atanh(temperature_celsius / 2)",
            expected: [
                CaseValue::new("neg", (-0.6_f64).atanh()),
                CaseValue::new("zero", 0.0_f64.atanh()),
                CaseValue::new("pos", 0.6_f64.atanh()),
            ],
        },
    ])
    .await;
}
