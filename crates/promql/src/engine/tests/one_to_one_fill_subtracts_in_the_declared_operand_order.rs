use super::*;

/// Fills are not commutative, but every fill test above uses `+`, where
/// `0 + present` and `present + 0` agree -- so the operand order on both fill
/// paths was free to be backwards. `-` tells them apart: a series present only
/// on the right must yield `0 - value`, not `value - 0`.
#[tokio::test]
pub(crate) async fn one_to_one_fill_subtracts_in_the_declared_operand_order() {
    let mut store = InMemoryMetricStore::new();
    for (name, job, value) in [
        ("a", "shared", 3.0),
        ("a", "left_only", 4.0),
        ("b", "shared", 1.0),
        ("b", "right_only", 5.0),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", name), ("job", job)]),
            10_000,
            value,
        );
    }

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let result = engine
        .query_instant("tenant-a", "a - on (job) fill(0) b", 10_000)
        .await
        .unwrap();

    let QueryResult::InstantVector(samples) = result else {
        panic!("expected vector");
    };
    let values = samples
        .iter()
        .map(|sample| {
            (
                sample.labels.get("job").expect("job label"),
                float_value(&sample.value),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert2::assert!(values.len() == 3);
    assert2::assert!(approx_eq(values["shared"], 2.0));
    assert2::assert!(approx_eq(values["left_only"], 4.0));
    assert2::assert!(approx_eq(values["right_only"], -5.0));
}
