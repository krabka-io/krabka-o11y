use super::*;

struct FillSample {
    metric: &'static str,
    job: &'static str,
    value: f64,
}

/// Fills are not commutative, but every fill test above uses `+`, where
/// `0 + present` and `present + 0` agree -- so the operand order on both fill
/// paths was free to be backwards. `-` tells them apart: a series present only
/// on the right must yield `0 - value`, not `value - 0`.
#[tokio::test]
pub(crate) async fn one_to_one_fill_subtracts_in_the_declared_operand_order() {
    let mut store = InMemoryMetricStore::new();
    for sample in [
        FillSample {
            metric: "a",
            job: "shared",
            value: 3.0,
        },
        FillSample {
            metric: "a",
            job: "left_only",
            value: 4.0,
        },
        FillSample {
            metric: "b",
            job: "shared",
            value: 1.0,
        },
        FillSample {
            metric: "b",
            job: "right_only",
            value: 5.0,
        },
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", sample.metric),
                crate::test_support::TestLabelPair::new("job", sample.job),
            ]),
            10_000,
            sample.value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let samples = instant_vector(&engine, "a - on (job) fill(0) b", 10_000).await;
    let mut values = BTreeMap::new();
    for sample in &samples {
        values.insert(
            sample.labels.get("job").expect("job label"),
            float_value(&sample.value),
        );
    }
    assert2::assert!(values.len() == 3);
    assert2::assert!(approx_eq(values["shared"], 2.0));
    assert2::assert!(approx_eq(values["left_only"], 4.0));
    assert2::assert!(approx_eq(values["right_only"], -5.0));
}
