use super::*;

/// `histogram_fraction` over classic buckets, where the outermost bounds are
/// infinite. An open-ended bucket counts in full only when the query's own
/// bound is open the same way -- a finite bound never reaches into it, however
/// far out it goes. Nothing exercised those two arms, nor a query bound that
/// lands inside a bucket rather than on its edge.
#[tokio::test]
pub(crate) async fn histogram_fraction_counts_open_ended_classic_buckets_only_against_open_bounds() {
    let mut store = InMemoryMetricStore::new();
    // Cumulative counts. The first `le` is negative, so the opening bucket runs
    // from -Inf, and the `+Inf` bucket closes the series at the other end.
    for (name, points) in [
        (
            "hc",
            vec![("-1", 2.0), ("0", 3.0), ("1", 6.0), ("+Inf", 10.0)],
        ),
        ("hc2", vec![("-1", 2.0), ("+Inf", 5.0)]),
        // A bucket four wide, so dividing by its width is not the same as
        // multiplying by it -- which it is for every bucket above.
        ("hc3", vec![("0", 0.0), ("4", 8.0), ("+Inf", 10.0)]),
    ] {
        for (le, count) in points {
            store.push_float(
                "tenant-a",
                labels(&[("__name__", name), ("le", le)]),
                10_000,
                count,
            );
        }
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    for (query, want) in [
        // A range on bucket edges takes whole buckets.
        ("histogram_fraction(0, 1, hc)", 0.3),
        // The -Inf bucket counts only against an -Inf bound...
        ("histogram_fraction(-Inf, -1, hc)", 0.2),
        ("histogram_fraction(-2, -1, hc)", 0.0),
        // ...and the +Inf bucket only against a +Inf bound.
        ("histogram_fraction(1, Inf, hc)", 0.4),
        ("histogram_fraction(1, 2, hc)", 0.0),
        // A bucket open at the top with a finite lower edge is still reached
        // through the upper arm, not the lower one.
        ("histogram_fraction(-1, Inf, hc2)", 0.6),
        // Everything, and a bound landing inside a bucket rather than on it.
        ("histogram_fraction(-Inf, Inf, hc)", 1.0),
        ("histogram_fraction(0.5, 1, hc)", 0.15),
        // Partial cover of a four-wide bucket: half of it, not eight times it.
        ("histogram_fraction(1, 3, hc3)", 0.4),
        ("histogram_fraction(0, 4, hc3)", 0.8),
        ("histogram_fraction(2, Inf, hc3)", 0.6),
    ] {
        let result = engine
            .query_instant("tenant-a", query, 10_000)
            .await
            .unwrap_or_else(|error| panic!("{query}: {error}"));
        let QueryResult::InstantVector(samples) = result else {
            panic!("expected a vector for {query}");
        };
        assert2::assert!(samples.len() == 1, "{query}");
        assert2::assert!(approx_eq(float_value(&samples[0].value), want), "{query}");
    }
}
