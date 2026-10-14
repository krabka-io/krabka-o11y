use super::*;

struct OverTimeCase {
    query: &'static str,
    expected: f64,
    preserves_name: bool,
}

#[tokio::test]
pub(crate) async fn instant_basic_over_time_functions_reduce_range_samples() {
    let store = three_point_gauge_store("queue_depth");
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for case in [
        OverTimeCase {
            query: "sum_over_time(queue_depth[2m])",
            expected: 8.0,
            preserves_name: false,
        },
        OverTimeCase {
            query: "avg_over_time(queue_depth[2m])",
            expected: 4.0,
            preserves_name: false,
        },
        OverTimeCase {
            query: "count_over_time(queue_depth[2m])",
            expected: 2.0,
            preserves_name: false,
        },
        OverTimeCase {
            query: "min_over_time(queue_depth[2m])",
            expected: 3.0,
            preserves_name: false,
        },
        OverTimeCase {
            query: "max_over_time(queue_depth[2m])",
            expected: 5.0,
            preserves_name: false,
        },
        OverTimeCase {
            query: "first_over_time(queue_depth[2m])",
            expected: 3.0,
            preserves_name: true,
        },
        OverTimeCase {
            query: "last_over_time(queue_depth[2m])",
            expected: 5.0,
            preserves_name: true,
        },
        OverTimeCase {
            query: "present_over_time(queue_depth[2m])",
            expected: 1.0,
            preserves_name: false,
        },
    ] {
        let samples = instant_vector(&engine, case.query, 120_000).await;
        assert2::assert!(samples.len() == 1);
        if case.preserves_name {
            assert2::assert!(samples[0].labels.get("__name__") == Some("queue_depth"));
        } else {
            assert2::assert!(samples[0].labels.get("__name__").is_none());
        }
        assert2::assert!(samples[0].labels.get("job") == Some("api"));
        assert2::assert!(approx_eq(float_value(&samples[0].value), case.expected));
    }
}
