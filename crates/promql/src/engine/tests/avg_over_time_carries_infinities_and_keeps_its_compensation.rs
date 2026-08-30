use super::*;

/// `avg_over_time` carries infinities the way Prometheus does: a mean that has
/// gone infinite stays there through finite samples, an infinity of the same
/// sign keeps it, and only the opposite infinity or a NaN turns it into a NaN.
/// Nothing fed the fold an infinity before, which left every arm of that guard
/// free -- and hid that the Kahan compensation went NaN on the very first
/// infinite increment and rode all the way out to the result.
///
/// The last case pins the compensation itself. It differs from the uncorrected
/// mean only in the final bits, so it is compared bit-for-bit; a relative
/// tolerance would accept the fold running backwards.
#[tokio::test]
pub(crate) async fn avg_over_time_carries_infinities_and_keeps_its_compensation() {
    let mut store = InMemoryMetricStore::new();
    let series = [
        ("inf_then_finite", vec![f64::INFINITY, 1.0, 2.0]),
        ("neg_inf_then_finite", vec![f64::NEG_INFINITY, 1.0]),
        (
            "opposite_infinities",
            vec![f64::INFINITY, f64::NEG_INFINITY],
        ),
        ("inf_then_nan", vec![f64::INFINITY, f64::NAN]),
        ("inf_twice", vec![f64::INFINITY, f64::INFINITY]),
        ("finite_then_inf", vec![1.0, f64::INFINITY]),
        ("compensated", vec![1.0, 0.1, 0.1]),
    ];
    for (name, values) in &series {
        for (index, value) in values.iter().enumerate() {
            let ts_ms = 10_000 + 10_000 * i64::try_from(index).expect("a small index");
            store.push_float(
                "tenant-a",
                labels(&[("__name__", *name), ("series", "f")]),
                ts_ms,
                *value,
            );
        }
        // `*_over_time` lowers onto a float-only operator leaf unless the
        // selector also matches a native histogram, which routes the call to
        // the interpreter kernel's own fold. Without a sibling here the kernel
        // copy is never executed.
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", *name), ("series", "h")]),
            20_000,
            native_histogram(4.0, 10.0),
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for (name, want) in [
        // A finite sample cannot pull an infinite mean back.
        ("inf_then_finite", Some(f64::INFINITY)),
        ("neg_inf_then_finite", Some(f64::NEG_INFINITY)),
        // Opposite infinities, and a NaN, are the two things that do.
        ("opposite_infinities", None),
        ("inf_then_nan", None),
        // Same-sign infinity leaves it alone, and an infinity arriving after a
        // finite start still takes the mean there.
        ("inf_twice", Some(f64::INFINITY)),
        ("finite_then_inf", Some(f64::INFINITY)),
    ] {
        let query = format!("avg_over_time({name}[1m])");
        let QueryResult::InstantVector(samples) = engine
            .query_instant("tenant-a", &query, 60_000)
            .await
            .unwrap_or_else(|error| panic!("{query}: {error}"))
        else {
            panic!("expected a vector for {query}");
        };
        let float = samples
            .iter()
            .find(|sample| sample.labels.get("series") == Some("f"))
            .unwrap_or_else(|| panic!("{query}: no float series in the result"));
        let got = float_value(&float.value);
        match want {
            Some(want) => {
                assert2::assert!(got.to_bits() == want.to_bits(), "{query}: got {got}");
            }
            None => assert2::assert!(got.is_nan(), "{query}: got {got}"),
        }
    }

    let QueryResult::InstantVector(samples) = engine
        .query_instant("tenant-a", "avg_over_time(compensated[1m])", 60_000)
        .await
        .expect("a compensated mean")
    else {
        panic!("expected a vector");
    };
    let float = samples
        .iter()
        .find(|sample| sample.labels.get("series") == Some("f"))
        .expect("a float series in the result");
    assert2::assert!(
        float_value(&float.value).to_bits() == 0.399_999_999_999_999_97_f64.to_bits(),
        "the Kahan compensation is added, not subtracted or dropped"
    );
}
