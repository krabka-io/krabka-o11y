use super::*;

struct FillInput {
    metric: &'static str,
    instance: &'static str,
    value: f64,
}

#[tokio::test]
pub(crate) async fn vector_vector_arithmetic_fill_uses_missing_side_values() {
    let mut store = InMemoryMetricStore::new();
    for input in [
        FillInput {
            metric: "a",
            instance: "matched",
            value: 10.0,
        },
        FillInput {
            metric: "a",
            instance: "left-only",
            value: 7.0,
        },
        FillInput {
            metric: "b",
            instance: "matched",
            value: 3.0,
        },
        FillInput {
            metric: "b",
            instance: "right-only",
            value: 5.0,
        },
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", input.metric),
                crate::test_support::TestLabelPair::new("instance", input.instance),
            ]),
            10_000,
            input.value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "a + on (instance) fill(0) b", 10_000).await;
    let mut values = BTreeMap::new();
    for sample in &samples {
        values.insert(
            sample.labels.get("instance").expect("instance label"),
            float_value(&sample.value),
        );
    }
    assert2::assert!(values.len() == 3);
    check!(approx_eq(values["matched"], 13.0));
    check!(approx_eq(values["left-only"], 7.0));
    check!(approx_eq(values["right-only"], 5.0));
    check!(samples.iter().all(|sample| {
        let label_names = sample
            .labels
            .iter()
            .map(|label| label.name)
            .collect::<Vec<_>>();
        sample.labels.get("__name__").is_none() && label_names == vec!["instance"]
    }));
}
