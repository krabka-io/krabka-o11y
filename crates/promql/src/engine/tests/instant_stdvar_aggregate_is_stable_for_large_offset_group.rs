use super::*;

/// M16: `stdvar` and `stddev` must not cancel into a negative variance.
///
/// The group has a large offset and close values. A negative variance gives
/// NaN from `sqrt`. Welford returns the small positive population variance for
/// `{0,1,2}` -> 2/3.
#[tokio::test]
pub(crate) async fn instant_stdvar_aggregate_is_stable_for_large_offset_group() {
    let mut store = InMemoryMetricStore::new();
    for case in [
        InstanceValueCase {
            instance: "0",
            value: 1e8,
        },
        InstanceValueCase {
            instance: "1",
            value: 1e8 + 1.0,
        },
        InstanceValueCase {
            instance: "2",
            value: 1e8 + 2.0,
        },
    ] {
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "big"),
                crate::test_support::TestLabelPair::new("job", "api"),
                crate::test_support::TestLabelPair::new("instance", case.instance),
            ]),
            10_000,
            case.value,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let QueryResult::InstantVector(stdvar) = engine
        .query_instant(&tenant_id("tenant-a"), "stdvar(big)", 10_000)
        .await
        .unwrap()
    else {
        panic!("expected vector");
    };
    assert2::assert!(stdvar.len() == 1);
    let value = float_value(&stdvar[0].value);
    check!(!value.is_nan(), "stdvar must be finite, got NaN");
    check!(value > 0.0, "stdvar must be positive, got {value}");
    check!(approx_eq(value, 2.0 / 3.0), "stdvar == 2/3, got {value}");
}
