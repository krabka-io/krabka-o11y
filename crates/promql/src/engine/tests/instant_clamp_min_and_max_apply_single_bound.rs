use super::*;

#[derive(bon::Builder)]
struct ClampSeries {
    name: &'static str,
    value: f64,
}

#[tokio::test]
pub(crate) async fn instant_clamp_min_and_max_apply_single_bound() {
    let mut store = InMemoryMetricStore::new();
    for series in [
        ClampSeries::builder().name("below").value(-5.0).build(),
        ClampSeries::builder().name("inside").value(7.0).build(),
        ClampSeries::builder().name("above").value(20.0).build(),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "temperature_celsius"),
                crate::test_support::TestLabelPair::new("case", series.name),
            ]),
            10_000,
            series.value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let min_result = engine
        .query_instant(
            &tenant_id("tenant-a"),
            "clamp_min(temperature_celsius, 0)",
            10_000,
        )
        .await
        .unwrap();
    let max_result = engine
        .query_instant(
            &tenant_id("tenant-a"),
            "clamp_max(temperature_celsius, 10)",
            10_000,
        )
        .await
        .unwrap();

    let QueryResult::InstantVector(min_samples) = min_result else {
        panic!("expected vector");
    };
    let QueryResult::InstantVector(max_samples) = max_result else {
        panic!("expected vector");
    };
    check!(min_samples.len() == 3);
    check!(max_samples.len() == 3);
    check!(min_samples.iter().any(|sample| {
        sample.labels.get("case") == Some("below") && approx_eq(float_value(&sample.value), 0.0)
    }));
    check!(min_samples.iter().any(|sample| {
        sample.labels.get("case") == Some("above") && approx_eq(float_value(&sample.value), 20.0)
    }));
    check!(max_samples.iter().any(|sample| {
        sample.labels.get("case") == Some("below") && approx_eq(float_value(&sample.value), -5.0)
    }));
    check!(max_samples.iter().any(|sample| {
        sample.labels.get("case") == Some("above") && approx_eq(float_value(&sample.value), 10.0)
    }));
}
