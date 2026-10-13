use super::*;

struct QuantileSample {
    instance: &'static str,
    value: f64,
}

struct QuantileExpectation {
    query: &'static str,
    phi_text: &'static str,
    predicate: fn(f64) -> bool,
}

/// M18: an out-of-range or NaN `quantile` phi does not error.
///
/// The aggregate matches Prometheus and the `histogram_quantile` family in this
/// file. It returns `+Inf` for phi > 1, `-Inf` for phi < 0, and `NaN` for a NaN
/// phi. It also raises an `InvalidQuantileWarning`, and it never aborts.
#[tokio::test]
pub(crate) async fn quantile_out_of_range_phi_returns_signed_inf_with_warning() {
    let mut store = InMemoryMetricStore::new();
    let samples = [
        QuantileSample {
            instance: "0",
            value: 1.0,
        },
        QuantileSample {
            instance: "1",
            value: 2.0,
        },
        QuantileSample {
            instance: "2",
            value: 3.0,
        },
    ];
    for sample_index in 0..samples.len() {
        let sample = &samples[sample_index];
        let lbls = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "m"),
            crate::test_support::TestLabelPair::new("instance", sample.instance),
        ]);
        store.push_float("t", lbls, 120_000, sample.value);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let time_ms = 120_000_i64;

    let queries = [
        QuantileExpectation {
            query: "quantile(1.1, m)",
            phi_text: "1.1",
            predicate: f64::is_infinite,
        },
        QuantileExpectation {
            query: "quantile(-0.1, m)",
            phi_text: "-0.1",
            predicate: f64::is_infinite,
        },
        QuantileExpectation {
            query: "quantile(NaN, m)",
            phi_text: "NaN",
            predicate: f64::is_nan,
        },
    ];
    for query_index in 0..queries.len() {
        let case = &queries[query_index];
        let annotated = engine
            .query_instant_with_annotations(&tenant_id("t"), case.query, time_ms)
            .await
            .unwrap_or_else(|error| panic!("`{}` must NOT error: {error}", case.query));

        let QueryResult::InstantVector(samples) = annotated.result else {
            panic!("`{}` must yield an instant vector", case.query);
        };
        assert2::assert!(samples.len() == 1);
        let value = float_value(&samples[0].value);
        assert2::assert!((case.predicate)(value));
        // For the +/-Inf cases, also pin the sign.
        if case.query.contains("1.1") {
            assert2::assert!(value > 0.0);
        } else if case.query.contains("-0.1") {
            assert2::assert!(value < 0.0);
        }

        assert2::assert!(
            annotated.annotations.warnings
                == vec![format!(
                    "PromQL warning: quantile value should be between 0 and 1, got {} (1:10)",
                    case.phi_text
                )]
        );
    }
}
