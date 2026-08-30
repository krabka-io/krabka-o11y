use super::*;

/// M18: an out-of-range or NaN `quantile` phi does not error.
///
/// The aggregate matches Prometheus and the `histogram_quantile` family in this
/// file. It returns `+Inf` for phi > 1, `-Inf` for phi < 0, and `NaN` for a NaN
/// phi. It also raises an `InvalidQuantileWarning`, and it never aborts.
#[tokio::test]
pub(crate) async fn quantile_out_of_range_phi_returns_signed_inf_with_warning() {
    let mut store = InMemoryMetricStore::new();
    for (instance, value) in [("0", 1.0), ("1", 2.0), ("2", 3.0)] {
        let lbls = labels(&[("__name__", "m"), ("instance", instance)]);
        store.push_float("t", lbls, 120_000, value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let time_ms = 120_000_i64;

    for (query, phi_text, predicate) in [
        (
            "quantile(1.1, m)",
            "1.1",
            f64::is_infinite as fn(f64) -> bool,
        ),
        ("quantile(-0.1, m)", "-0.1", f64::is_infinite),
        ("quantile(NaN, m)", "NaN", f64::is_nan),
    ] {
        let (result, annotations) = engine
            .query_instant_with_annotations("t", query, time_ms)
            .await
            .unwrap_or_else(|error| panic!("`{query}` must NOT error: {error}"));

        let QueryResult::InstantVector(samples) = result else {
            panic!("`{query}` must yield an instant vector");
        };
        assert2::assert!(samples.len() == 1);
        let value = float_value(&samples[0].value);
        assert2::assert!(predicate(value));
        // For the +/-Inf cases, also pin the sign.
        if query.contains("1.1") {
            assert2::assert!(value > 0.0);
        } else if query.contains("-0.1") {
            assert2::assert!(value < 0.0);
        }

        assert2::assert!(
            annotations.warnings
                == vec![format!(
                    "PromQL warning: quantile value should be between 0 and 1, got {phi_text}"
                )]
        );
    }
}
