use super::*;

/// Fill modifiers are meaningless for set operators and are refused. Each
/// form below sets only one side, so a guard that joined the two checks with
/// `&&` instead of `||` would accept the single-sided ones -- `fill(0)` sets
/// both and is refused either way.
#[tokio::test]
pub(crate) async fn a_fill_modifier_is_refused_on_a_set_operator() {
    let mut store = InMemoryMetricStore::new();
    for name in ["a", "b"] {
        store.push_float(
            "tenant-a",
            labels(&[("__name__", name), ("job", "x")]),
            10_000,
            1.0,
        );
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for query in [
        "a and on (job) fill_left(0) b",
        "a or on (job) fill_right(0) b",
        "a unless on (job) fill(0) b",
    ] {
        let error = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .expect_err("a fill modifier on a set operator is a planning error");
        assert2::assert!(
            matches!(
                &error,
                PromqlError::Plan(message)
                    if message.contains("fill modifiers are invalid for set operators")
            ),
            "{query}: {error}"
        );
    }
}
