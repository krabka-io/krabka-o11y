use super::*;

struct BucketFixture {
    upper_bound: &'static str,
    count: f64,
}

#[derive(bon::Builder)]
struct QueryExpectation {
    query: &'static str,
    want: f64,
}

/// A classic histogram answers correctly alongside a native sibling under the
/// same name -- the mixed case, which every other classic test avoids by
/// keeping the two apart. The window bounds are the infinite ones, so the
/// buckets open at each end are the ones being read.
#[tokio::test]
pub(crate) async fn a_classic_histogram_answers_beside_a_native_sibling() {
    let mut store = InMemoryMetricStore::new();
    // A classic histogram closed at both ends by an infinity.
    let buckets = [
        BucketFixture {
            upper_bound: "-1",
            count: 2.0,
        },
        BucketFixture {
            upper_bound: "0",
            count: 3.0,
        },
        BucketFixture {
            upper_bound: "1",
            count: 6.0,
        },
        BucketFixture {
            upper_bound: "+Inf",
            count: 10.0,
        },
    ];
    for bucket_index in 0..buckets.len() {
        let bucket = &buckets[bucket_index];
        store.push_float(
            "tenant-a",
            labels(&[
                crate::test_support::TestLabelPair::new("__name__", "hk"),
                crate::test_support::TestLabelPair::new("kind", "classic"),
                crate::test_support::TestLabelPair::new("le", bucket.upper_bound),
            ]),
            10_000,
            bucket.count,
        );
    }
    // The native sibling that sends the call to the kernel.
    store.push_histogram(
        "tenant-a",
        labels(&[
            crate::test_support::TestLabelPair::new("__name__", "hk"),
            crate::test_support::TestLabelPair::new("kind", "native"),
        ]),
        10_000,
        native_histogram(4.0, 8.0),
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());

    let queries = [
        // The bucket running from -Inf, and the one running to +Inf.
        QueryExpectation::builder()
            .query("histogram_fraction(-Inf, -1, hk)")
            .want(0.2)
            .build(),
        QueryExpectation::builder()
            .query("histogram_fraction(1, Inf, hk)")
            .want(0.4)
            .build(),
        QueryExpectation::builder()
            .query("histogram_fraction(0, 1, hk)")
            .want(0.3)
            .build(),
    ];
    for query_index in 0..queries.len() {
        let query = &queries[query_index];
        let QueryResult::InstantVector(samples) = engine
            .query_instant(&tenant_id("tenant-a"), query.query, 10_000)
            .await
            .unwrap_or_else(|error| panic!("{}: {error}", query.query))
        else {
            panic!("expected a vector for {}", query.query);
        };
        let classic = samples
            .iter()
            .find(|sample| sample.labels.get("kind") == Some("classic"))
            .unwrap_or_else(|| panic!("{}: no classic series in the result", query.query));
        assert2::assert!(
            approx_eq(float_value(&classic.value), query.want),
            "{}",
            query.query
        );
    }
}
