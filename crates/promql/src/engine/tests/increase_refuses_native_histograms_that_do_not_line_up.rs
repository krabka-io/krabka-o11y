use super::*;

struct HistogramLayoutCase {
    field: &'static str,
    histogram: NativeHistogram,
}

/// `increase` reconciles compatible native-histogram layouts before subtraction.
#[tokio::test]
pub(crate) async fn increase_reconciles_native_histograms_that_do_not_line_up() {
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
        HistogramLayoutCase {
            field: "schema",
            histogram: NativeHistogram {
                schema: 1,
                ..grown()
            },
        },
        HistogramLayoutCase {
            field: "is_float",
            histogram: NativeHistogram {
                is_float: false,
                ..grown()
            },
        },
        HistogramLayoutCase {
            field: "zero_threshold",
            histogram: NativeHistogram {
                zero_threshold: 2.0,
                ..grown()
            },
        },
        HistogramLayoutCase {
            field: "custom_values",
            histogram: NativeHistogram {
                custom_values: Some(vec![1.0]),
                ..grown()
            },
        },
        HistogramLayoutCase {
            field: "positive_spans",
            histogram: NativeHistogram {
                positive_spans: vec![BucketSpan {
                    offset: 1,
                    length: 2,
                }],
                ..grown()
            },
        },
        HistogramLayoutCase {
            field: "negative_spans",
            histogram: NativeHistogram {
                negative_spans: vec![BucketSpan {
                    offset: 1,
                    length: 1,
                }],
                ..grown()
            },
        },
        // The two counts-length clauses cannot be isolated: the store rejects
        // a histogram whose counts disagree with its spans, so equal spans
        // already force equal counts lengths. These grow both together.
        HistogramLayoutCase {
            field: "positive bucket count",
            histogram: NativeHistogram {
                positive_spans: vec![BucketSpan {
                    offset: 0,
                    length: 3,
                }],
                positive_counts: vec![3.0, 4.0, 5.0],
                ..grown()
            },
        },
        HistogramLayoutCase {
            field: "negative bucket count",
            histogram: NativeHistogram {
                negative_spans: vec![BucketSpan {
                    offset: 0,
                    length: 2,
                }],
                negative_counts: vec![2.0, 3.0],
                ..grown()
            },
        },
    ];

    let mut store = InMemoryMetricStore::new();
    for index in 0..variants.len() {
        let variant = &variants[index].histogram;
        let name = format!("hx{index}");
        store.push_histogram(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", &name)]),
            10_000,
            base(),
        );
        store.push_histogram(
            "tenant-a",
            labels(&[crate::test_support::TestLabelPair::new("__name__", &name)]),
            20_000,
            variant.clone(),
        );
    }
    // A pair that does line up, so a check stuck at false is caught too.
    store.push_histogram(
        "tenant-a",
        labels(&[crate::test_support::TestLabelPair::new("__name__", "ok")]),
        10_000,
        base(),
    );
    store.push_histogram(
        "tenant-a",
        labels(&[crate::test_support::TestLabelPair::new("__name__", "ok")]),
        20_000,
        grown(),
    );

    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    for index in 0..variants.len() {
        let field = variants[index].field;
        let query = format!("increase(hx{index}[5m])");
        let QueryResult::InstantVector(samples) = engine
            .query_instant(&tenant_id("tenant-a"), &query, 20_000)
            .await
            .unwrap_or_else(|error| panic!("{field}: {error}"))
        else {
            panic!("expected a vector for {field}");
        };
        assert2::assert!(samples.len() == 1, "{field} is reconciled");
    }

    let QueryResult::InstantVector(samples) = engine
        .query_instant(&tenant_id("tenant-a"), "increase(ok[5m])", 20_000)
        .await
        .expect("a matched pair")
    else {
        panic!("expected a vector");
    };
    assert2::assert!(samples.len() == 1, "a matched pair does produce a sample");
}
