use super::*;

#[derive(bon::Builder)]
struct SeriesCase {
    name: &'static str,
    values: Vec<f64>,
}

struct ExpectedMeanCase {
    name: &'static str,
    expected: Option<f64>,
}

/// `avg_over_time` carries infinities the way Prometheus does: a mean that has
/// gone infinite stays there through finite samples, an infinity of the same
/// sign keeps it, and only the opposite infinity or a NaN turns it into a NaN.
/// Nothing fed the fold an infinity before, which left every arm of that guard
/// free -- and hid that the Kahan compensation went NaN on the very first
/// infinite increment and rode all the way out to the result.
///
/// The last case pins the compensation itself. `1.0 + 0.1 + 0.1` sums to
/// `1.2000000000000002` without it, one ulp off the `1.2` that the compensated
/// sum reaches, so the mean differs in its final bit. It is compared
/// bit-for-bit; a relative tolerance would accept the fold running backwards.
#[tokio::test]
pub(crate) async fn avg_over_time_carries_infinities_and_keeps_its_compensation() {
    let mut store = InMemoryMetricStore::new();
    let series = [
        SeriesCase::builder()
            .name("inf_then_finite")
            .values(vec![f64::INFINITY, 1.0, 2.0])
            .build(),
        SeriesCase::builder()
            .name("neg_inf_then_finite")
            .values(vec![f64::NEG_INFINITY, 1.0])
            .build(),
        SeriesCase::builder()
            .name("opposite_infinities")
            .values(vec![f64::INFINITY, f64::NEG_INFINITY])
            .build(),
        SeriesCase::builder()
            .name("inf_then_nan")
            .values(vec![f64::INFINITY, f64::NAN])
            .build(),
        SeriesCase::builder()
            .name("inf_twice")
            .values(vec![f64::INFINITY, f64::INFINITY])
            .build(),
        SeriesCase::builder()
            .name("finite_then_inf")
            .values(vec![1.0, f64::INFINITY])
            .build(),
        SeriesCase::builder()
            .name("compensated")
            .values(vec![1.0, 0.1, 0.1])
            .build(),
    ];
    for case in &series {
        for index in 0..case.values.len() {
            let ts_ms = 10_000 + 10_000 * i64::try_from(index).expect("a small index");
            store.push_float(
                "tenant-a",
                labels(&[
                    crate::test_support::TestLabelPair::new("__name__", case.name),
                    crate::test_support::TestLabelPair::new("series", "f"),
                ]),
                ts_ms,
                case.values[index],
            );
        }
        // `*_over_time` lowers onto a float-only operator leaf unless the
        // selector also matches a native histogram, which routes the call to
        // the interpreter kernel's own fold. Without a sibling here the kernel
        // copy is never executed.
        store.push_histogram(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", case.name),
                crate::test_support::TestLabelPair::new("series", "h"),
            ]),
            20_000,
            native_histogram(4.0, 10.0),
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for case in [
        // A finite sample cannot pull an infinite mean back.
        ExpectedMeanCase {
            name: "inf_then_finite",
            expected: Some(f64::INFINITY),
        },
        ExpectedMeanCase {
            name: "neg_inf_then_finite",
            expected: Some(f64::NEG_INFINITY),
        },
        // Opposite infinities, and a NaN, are the two things that do.
        ExpectedMeanCase {
            name: "opposite_infinities",
            expected: None,
        },
        ExpectedMeanCase {
            name: "inf_then_nan",
            expected: None,
        },
        // Same-sign infinity leaves it alone, and an infinity arriving after a
        // finite start still takes the mean there.
        ExpectedMeanCase {
            name: "inf_twice",
            expected: Some(f64::INFINITY),
        },
        ExpectedMeanCase {
            name: "finite_then_inf",
            expected: Some(f64::INFINITY),
        },
    ] {
        let query = format!("avg_over_time({}[1m])", case.name);
        let QueryResult::InstantVector(samples) = engine
            .query_instant(&tenant_id("tenant-a"), &query, 60_000)
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
        match case.expected {
            Some(want) => {
                assert2::assert!(got.to_bits() == want.to_bits(), "{query}: got {got}");
            }
            None => assert2::assert!(got.is_nan(), "{query}: got {got}"),
        }
    }

    let QueryResult::InstantVector(samples) = engine
        .query_instant(
            &tenant_id("tenant-a"),
            "avg_over_time(compensated[1m])",
            60_000,
        )
        .await
        .expect("a compensated mean")
    else {
        panic!("expected a vector");
    };
    let float = sample_with_label(&samples, "series", "f");
    assert2::assert!(
        float_value(&float.value).to_bits() == 0.4_f64.to_bits(),
        "the Kahan compensation is added, not subtracted or dropped"
    );
}
