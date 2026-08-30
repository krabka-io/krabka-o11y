use super::*;

/// `increase` over native histograms needs every consecutive pair in the
/// window to line up -- same schema, same shape, same zero threshold -- and
/// yields nothing when they do not. The check is an eight-clause conjunction
/// and no test ever gave it a mismatched pair, so any one of its `&&` could
/// have been an `||` and the fold would have run on histograms it cannot
/// combine. Each variant below differs in exactly one of those clauses.
#[tokio::test]
pub(crate) async fn increase_refuses_native_histograms_that_do_not_line_up() {
    fn base() -> NativeHistogram {
        NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::No,
            zero_threshold: 1.0,
            zero_count: 1.0,
            count: 6.0,
            sum: 10.0,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 2,
            }],
            positive_counts: vec![1.0, 2.0],
            negative_spans: vec![BucketSpan {
                offset: 0,
                length: 1,
            }],
            negative_counts: vec![1.0],
            custom_values: None,
            start_timestamp_ms: None,
        }
    }
    fn grown() -> NativeHistogram {
        NativeHistogram {
            count: 12.0,
            sum: 20.0,
            positive_counts: vec![3.0, 4.0],
            negative_counts: vec![2.0],
            ..base()
        }
    }

    let variants = [
        (
            "schema",
            NativeHistogram {
                schema: 1,
                ..grown()
            },
        ),
        (
            "is_float",
            NativeHistogram {
                is_float: false,
                ..grown()
            },
        ),
        (
            "zero_threshold",
            NativeHistogram {
                zero_threshold: 2.0,
                ..grown()
            },
        ),
        (
            "custom_values",
            NativeHistogram {
                custom_values: Some(vec![1.0]),
                ..grown()
            },
        ),
        (
            "positive_spans",
            NativeHistogram {
                positive_spans: vec![BucketSpan {
                    offset: 1,
                    length: 2,
                }],
                ..grown()
            },
        ),
        (
            "negative_spans",
            NativeHistogram {
                negative_spans: vec![BucketSpan {
                    offset: 1,
                    length: 1,
                }],
                ..grown()
            },
        ),
        // The two counts-length clauses cannot be isolated: the store rejects
        // a histogram whose counts disagree with its spans, so equal spans
        // already force equal counts lengths. These grow both together.
        (
            "positive bucket count",
            NativeHistogram {
                positive_spans: vec![BucketSpan {
                    offset: 0,
                    length: 3,
                }],
                positive_counts: vec![3.0, 4.0, 5.0],
                ..grown()
            },
        ),
        (
            "negative bucket count",
            NativeHistogram {
                negative_spans: vec![BucketSpan {
                    offset: 0,
                    length: 2,
                }],
                negative_counts: vec![2.0, 3.0],
                ..grown()
            },
        ),
    ];

    let mut store = InMemoryMetricStore::new();
    for (index, (_, variant)) in variants.iter().enumerate() {
        let name = format!("hx{index}");
        store.push_histogram("tenant-a", labels(&[("__name__", &name)]), 10_000, base());
        store.push_histogram(
            "tenant-a",
            labels(&[("__name__", &name)]),
            20_000,
            variant.clone(),
        );
    }
    // A pair that does line up, so a check stuck at false is caught too.
    store.push_histogram("tenant-a", labels(&[("__name__", "ok")]), 10_000, base());
    store.push_histogram("tenant-a", labels(&[("__name__", "ok")]), 20_000, grown());

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for (index, (field, _)) in variants.iter().enumerate() {
        let query = format!("increase(hx{index}[5m])");
        let QueryResult::InstantVector(samples) = engine
            .query_instant("tenant-a", &query, 20_000)
            .await
            .unwrap_or_else(|error| panic!("{field}: {error}"))
        else {
            panic!("expected a vector for {field}");
        };
        assert2::assert!(samples.is_empty(), "{field} differs, so there is no sample");
    }

    let QueryResult::InstantVector(samples) = engine
        .query_instant("tenant-a", "increase(ok[5m])", 20_000)
        .await
        .expect("a matched pair")
    else {
        panic!("expected a vector");
    };
    assert2::assert!(samples.len() == 1, "a matched pair does produce a sample");
}
