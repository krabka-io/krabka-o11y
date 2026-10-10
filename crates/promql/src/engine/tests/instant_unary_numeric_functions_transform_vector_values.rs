use super::*;

#[tokio::test]
pub(crate) async fn instant_unary_numeric_functions_transform_vector_values() {
    assert_signed_temperature_cases(&[
        CaseQuery {
            query: "ceil(temperature_celsius)",
            expected: [
                CaseValue::new("neg", -1.0),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 2.0),
            ],
        },
        CaseQuery {
            query: "floor(temperature_celsius)",
            expected: [
                CaseValue::new("neg", -2.0),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 1.0),
            ],
        },
        CaseQuery {
            query: "sgn(temperature_celsius)",
            expected: [
                CaseValue::new("neg", -1.0),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 1.0),
            ],
        },
        CaseQuery {
            query: "abs(temperature_celsius)",
            expected: [
                CaseValue::new("neg", 1.2),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 1.2),
            ],
        },
        CaseQuery {
            query: "sqrt(abs(temperature_celsius))",
            expected: [
                CaseValue::new("neg", 1.2_f64.sqrt()),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 1.2_f64.sqrt()),
            ],
        },
        CaseQuery {
            query: "exp(abs(temperature_celsius))",
            expected: [
                CaseValue::new("neg", 1.2_f64.exp()),
                CaseValue::new("zero", 1.0),
                CaseValue::new("pos", 1.2_f64.exp()),
            ],
        },
        CaseQuery {
            query: "ln(abs(temperature_celsius))",
            expected: [
                CaseValue::new("neg", 1.2_f64.ln()),
                CaseValue::new("zero", f64::NEG_INFINITY),
                CaseValue::new("pos", 1.2_f64.ln()),
            ],
        },
        CaseQuery {
            query: "log2(abs(temperature_celsius))",
            expected: [
                CaseValue::new("neg", 1.2_f64.log2()),
                CaseValue::new("zero", f64::NEG_INFINITY),
                CaseValue::new("pos", 1.2_f64.log2()),
            ],
        },
        CaseQuery {
            query: "log10(abs(temperature_celsius))",
            expected: [
                CaseValue::new("neg", 1.2_f64.log10()),
                CaseValue::new("zero", f64::NEG_INFINITY),
                CaseValue::new("pos", 1.2_f64.log10()),
            ],
        },
        CaseQuery {
            query: "round(temperature_celsius)",
            expected: [
                CaseValue::new("neg", -1.0),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 1.0),
            ],
        },
        CaseQuery {
            query: "round(temperature_celsius, 0.5)",
            expected: [
                CaseValue::new("neg", -1.0),
                CaseValue::new("zero", 0.0),
                CaseValue::new("pos", 1.0),
            ],
        },
    ])
    .await;
}
