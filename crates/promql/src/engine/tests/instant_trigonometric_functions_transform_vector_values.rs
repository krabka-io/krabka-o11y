use super::*;

#[tokio::test]
pub(crate) async fn instant_trigonometric_functions_transform_vector_values() {
    let mut store = InMemoryMetricStore::new();
    for (case, value) in [
        ("zero", 0.0),
        ("half_pi", std::f64::consts::FRAC_PI_2),
        ("pi", std::f64::consts::PI),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "angle_radians"), ("case", case)]),
            10_000,
            value,
        );
    }
    for (case, value) in [("neg", -0.5), ("zero", 0.0), ("pos", 0.5)] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", "unit_value"), ("case", case)]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (query, expected) in [
        (
            "sin(angle_radians)",
            [
                ("zero", 0.0_f64.sin()),
                ("half_pi", std::f64::consts::FRAC_PI_2.sin()),
                ("pi", std::f64::consts::PI.sin()),
            ],
        ),
        (
            "cos(angle_radians)",
            [
                ("zero", 0.0_f64.cos()),
                ("half_pi", std::f64::consts::FRAC_PI_2.cos()),
                ("pi", std::f64::consts::PI.cos()),
            ],
        ),
        (
            "tan(angle_radians)",
            [
                ("zero", 0.0_f64.tan()),
                ("half_pi", std::f64::consts::FRAC_PI_2.tan()),
                ("pi", std::f64::consts::PI.tan()),
            ],
        ),
        (
            "deg(angle_radians)",
            [("zero", 0.0), ("half_pi", 90.0), ("pi", 180.0)],
        ),
        (
            "rad(deg(angle_radians))",
            [
                ("zero", 0.0),
                ("half_pi", std::f64::consts::FRAC_PI_2),
                ("pi", std::f64::consts::PI),
            ],
        ),
        (
            "asin(unit_value)",
            [
                ("neg", (-0.5_f64).asin()),
                ("zero", 0.0_f64.asin()),
                ("pos", 0.5_f64.asin()),
            ],
        ),
        (
            "acos(unit_value)",
            [
                ("neg", (-0.5_f64).acos()),
                ("zero", 0.0_f64.acos()),
                ("pos", 0.5_f64.acos()),
            ],
        ),
        (
            "atan(unit_value)",
            [
                ("neg", (-0.5_f64).atan()),
                ("zero", 0.0_f64.atan()),
                ("pos", 0.5_f64.atan()),
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
