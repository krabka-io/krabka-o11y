#[cfg(feature = "experimental-functions")]
use super::*;

#[cfg(feature = "experimental-functions")]
struct GaugeSample {
    timestamp_ms: i64,
    value: f64,
}

/// Differential parity for the experimental scalar and range functions.
///
/// The functions are `max_of`, `min_of`, `double_exponential_smoothing` over a
/// bare matrix selector, and the duration helpers. Each one delegates to the
/// same interpreter method, so the result is parity-exact by construction.
#[cfg(feature = "experimental-functions")]
#[tokio::test]
pub(crate) async fn experimental_call_planner_path_matches_interpreter() {
    let mut store = InMemoryMetricStore::new();
    for sample in [
        GaugeSample {
            timestamp_ms: 0,
            value: 1.0,
        },
        GaugeSample {
            timestamp_ms: 60_000,
            value: 2.0,
        },
        GaugeSample {
            timestamp_ms: 120_000,
            value: 4.0,
        },
        GaugeSample {
            timestamp_ms: 180_000,
            value: 8.0,
        },
        GaugeSample {
            timestamp_ms: 240_000,
            value: 16.0,
        },
    ] {
        store.push_float(
            "t",
            labels(&[crate::test_support::TestLabelPair::new("__name__", "g")]),
            sample.timestamp_ms,
            sample.value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries: &[QueryAt] = &[
        QueryAt {
            query: "max_of(1, 2)",
            time_ms: 120_000,
        },
        QueryAt {
            query: "min_of(1, 2)",
            time_ms: 120_000,
        },
        QueryAt {
            query: "max_of(scalar(g), 3)",
            time_ms: 240_000,
        },
        QueryAt {
            query: "double_exponential_smoothing(g[4m], 0.5, 0.5)",
            time_ms: 240_000,
        },
        // Duration helpers (instant query: no range context -> 0 on both
        // paths).
        QueryAt {
            query: "step()",
            time_ms: 120_000,
        },
        QueryAt {
            query: "start()",
            time_ms: 120_000,
        },
        QueryAt {
            query: "end()",
            time_ms: 120_000,
        },
        QueryAt {
            query: "range()",
            time_ms: 120_000,
        },
    ];

    // The recursive planner must claim every one of these.
    assert_call_planner_matches_interpreter(&engine, &queries).await;
}
