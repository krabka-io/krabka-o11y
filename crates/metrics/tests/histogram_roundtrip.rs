use krabka_metrics::{
    BucketSpan, NativeHistogram, ResetHint, decode_native_histograms, encode_native_histograms,
};
use proptest::prelude::*;

#[derive(Debug)]
struct SpanCounts {
    spans: Vec<BucketSpan>,
    counts: Vec<f64>,
}

fn arb_span_and_counts() -> impl Strategy<Value = SpanCounts> {
    proptest::collection::vec(
        (0_i32..8).prop_flat_map(|offset| {
            (1_u32..4).prop_map(move |length| BucketSpan { offset, length })
        }),
        0..3,
    )
    .prop_map(|spans| {
        let total: usize = spans.iter().map(|span| span.length as usize).sum();
        let mut next_count = 1.0;
        let counts = (0..total)
            .map(|_| {
                let count = next_count;
                next_count += 1.0;
                count
            })
            .collect();
        SpanCounts { spans, counts }
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    #[test]
    fn random_native_histograms_round_trip(
        schema in -53_i8..=8,
        is_float in any::<bool>(),
        sum in -1e6_f64..1e6,
        positive in arb_span_and_counts(),
        negative in arb_span_and_counts(),
    ) {
        let h = NativeHistogram {
            schema,
            is_float,
            reset_hint: ResetHint::No,
            zero_threshold: 1e-100,
            zero_count: 0.0,
            count: positive.counts.iter().chain(&negative.counts).sum(),
            sum,
            positive_spans: positive.spans,
            positive_counts: positive.counts,
            negative_spans: negative.spans,
            negative_counts: negative.counts,
            custom_values: None,
            start_timestamp_ms: None,
        };
        let rows = vec![krabka_metrics::NativeHistogramRow {
            fingerprint: 7,
            timestamp_ms: 99,
            hist: h.clone(),
        }];
        let batch = encode_native_histograms(&rows).unwrap();
        let back = decode_native_histograms(&batch).unwrap();
        prop_assert_eq!(back, rows);
    }
}
