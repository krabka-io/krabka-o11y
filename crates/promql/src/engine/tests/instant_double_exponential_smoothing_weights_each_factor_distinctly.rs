#[cfg(feature = "experimental-functions")]
use super::*;

#[cfg(feature = "experimental-functions")]
#[derive(bon::Builder)]
struct SampleCase {
    timestamp_ms: i64,
    value: f64,
}

#[cfg(feature = "experimental-functions")]
struct QueryCase {
    query: &'static str,
    expected: f64,
}

/// The same smoothing with *distinct* factors. Every other test here passes
/// `0.5, 0.5`, where `factor` and `1.0 - factor` are the same number, so
/// swapping either one is invisible -- as is reversing which of the two the
/// value and the trend are weighted by. Four samples also run the trend update
/// twice, which a shorter series never reaches, and the pair case pins the
/// two-sample minimum.
#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn instant_double_exponential_smoothing_weights_each_factor_distinctly() {
    let mut store = InMemoryMetricStore::new();
    // The initial trend is 2.0, not 1.0: at 1.0 the trend carry `(1 - tf) *
    // trend` equals `(1 - tf) / trend`, so that multiply would be free to be a
    // divide.
    for sample in [
        SampleCase::builder().timestamp_ms(0).value(1.0).build(),
        SampleCase::builder()
            .timestamp_ms(60_000)
            .value(3.0)
            .build(),
        SampleCase::builder()
            .timestamp_ms(120_000)
            .value(4.0)
            .build(),
        SampleCase::builder()
            .timestamp_ms(180_000)
            .value(8.0)
            .build(),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "g")]),
            sample.timestamp_ms,
            sample.value,
        );
    }
    for sample in [
        SampleCase::builder()
            .timestamp_ms(120_000)
            .value(1.0)
            .build(),
        SampleCase::builder()
            .timestamp_ms(180_000)
            .value(2.0)
            .build(),
    ] {
        store.push_float(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "pair")]),
            sample.timestamp_ms,
            sample.value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for case in [
        QueryCase {
            query: "double_exponential_smoothing(g[4m], 0.3, 0.4)",
            expected: 7.006,
        },
        // Two samples are the minimum the fold accepts.
        QueryCase {
            query: "double_exponential_smoothing(pair[2m], 0.3, 0.4)",
            expected: 2.0,
        },
    ] {
        assert_lone_value_at(
            &engine,
            LoneValueQuery {
                query: case.query,
                time_ms: 180_000,
                want: case.expected,
            },
        )
        .await;
    }
}
