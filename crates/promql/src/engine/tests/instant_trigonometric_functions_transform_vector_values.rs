use super::*;

#[tokio::test]
pub(crate) async fn instant_trigonometric_functions_transform_vector_values() {
    let mut store = InMemoryMetricStore::new();
    push_cases(
        &mut store,
        "angle_radians",
        &[
            CaseValue::new("zero", 0.0),
            CaseValue::new("half_pi", std::f64::consts::FRAC_PI_2),
            CaseValue::new("pi", std::f64::consts::PI),
        ],
    );
    push_cases(
        &mut store,
        "unit_value",
        &[
            CaseValue::new("neg", -0.5),
            CaseValue::new("zero", 0.0),
            CaseValue::new("pos", 0.5),
        ],
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        (
            "sin(angle_radians)",
            [
                CaseValue::new("zero", 0.0_f64.sin()),
                CaseValue::new("half_pi", std::f64::consts::FRAC_PI_2.sin()),
                CaseValue::new("pi", std::f64::consts::PI.sin()),
            ],
        ),
        (
            "cos(angle_radians)",
            [
                CaseValue::new("zero", 0.0_f64.cos()),
                CaseValue::new("half_pi", std::f64::consts::FRAC_PI_2.cos()),
                CaseValue::new("pi", std::f64::consts::PI.cos()),
            ],
        ),
        (
            "tan(angle_radians)",
            [
                CaseValue::new("zero", 0.0_f64.tan()),
                CaseValue::new("half_pi", std::f64::consts::FRAC_PI_2.tan()),
                CaseValue::new("pi", std::f64::consts::PI.tan()),
            ],
        ),
        (
            "deg(angle_radians)",
            [
                CaseValue::new("zero", 0.0),
                CaseValue::new("half_pi", 90.0),
                CaseValue::new("pi", 180.0),
            ],
        ),
        (
            "rad(deg(angle_radians))",
            [
                CaseValue::new("zero", 0.0),
                CaseValue::new("half_pi", std::f64::consts::FRAC_PI_2),
                CaseValue::new("pi", std::f64::consts::PI),
            ],
        ),
        (
            "asin(unit_value)",
            [
                CaseValue::new("neg", (-0.5_f64).asin()),
                CaseValue::new("zero", 0.0_f64.asin()),
                CaseValue::new("pos", 0.5_f64.asin()),
            ],
        ),
        (
            "acos(unit_value)",
            [
                CaseValue::new("neg", (-0.5_f64).acos()),
                CaseValue::new("zero", 0.0_f64.acos()),
                CaseValue::new("pos", 0.5_f64.acos()),
            ],
        ),
        (
            "atan(unit_value)",
            [
                CaseValue::new("neg", (-0.5_f64).atan()),
                CaseValue::new("zero", 0.0_f64.atan()),
                CaseValue::new("pos", 0.5_f64.atan()),
            ],
        ),
    ] {
        let samples = instant_vector(&engine, query, 10_000).await;
        assert_case_values(&samples, &expected);
    }
}
