//! Decode `remote_write` histogram samples into absolute native histograms.

use num_traits::ToPrimitive;

use super::{WireError, pb};
use crate::{BucketSpan, NativeHistogram, ResetHint};

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    /// `is_v2_float` asks whether a v2 histogram carries float counts, and
    /// four independent signals each answer yes. They are joined by `||`, so
    /// every one has to be decisive on its own: a histogram carrying only
    /// that signal must be float, or the clause could be `&&`-ed away
    /// unnoticed.
    #[test]
    fn a_v2_histogram_is_float_on_any_one_of_its_four_signals() {
        let float = super::is_v2_float;

        // An integer histogram, and the baseline: no signal at all.
        check!(!float(&pb::v2::Histogram::default()));
        check!(!float(&pb::v2::Histogram {
            count: Some(pb::v2::histogram::Count::CountInt(11)),
            zero_count: Some(pb::v2::histogram::ZeroCount::ZeroCountInt(22)),
            positive_deltas: vec![1, 2],
            negative_deltas: vec![3],
            ..Default::default()
        }));

        // Each signal alone, with everything else left integer or empty.
        check!(float(&pb::v2::Histogram {
            count: Some(pb::v2::histogram::Count::CountFloat(1.5)),
            ..Default::default()
        }));
        check!(float(&pb::v2::Histogram {
            zero_count: Some(pb::v2::histogram::ZeroCount::ZeroCountFloat(1.5)),
            ..Default::default()
        }));
        check!(float(&pb::v2::Histogram {
            positive_counts: vec![1.5],
            ..Default::default()
        }));
        check!(float(&pb::v2::Histogram {
            negative_counts: vec![1.5],
            ..Default::default()
        }));

        // The two count vectors are read for emptiness, not for content: a
        // zero is still a float count.
        check!(float(&pb::v2::Histogram {
            positive_counts: vec![0.0],
            ..Default::default()
        }));
        check!(float(&pb::v2::Histogram {
            negative_counts: vec![0.0],
            ..Default::default()
        }));
    }

    fn bucket_span(offset: i32, length: u32) -> BucketSpan {
        BucketSpan { offset, length }
    }

    /// Spans and counts have to agree about how many buckets there are, on
    /// each side independently.
    #[test]
    fn spans_and_counts_must_agree_on_each_side() {
        let ok = validate_spans_and_counts(
            0,
            &[bucket_span(0, 2)],
            &[1.0, 2.0],
            &[bucket_span(0, 1)],
            &[3.0],
            None,
        );
        check!(ok.is_ok());

        let err = validate_spans_and_counts(0, &[bucket_span(0, 2)], &[1.0], &[], &[], None)
            .unwrap_err()
            .to_string();
        check!(
            err.contains("positive spans declare 2 buckets but 1 counts"),
            "got: {err}"
        );

        let err = validate_spans_and_counts(0, &[], &[], &[bucket_span(0, 1)], &[], None)
            .unwrap_err()
            .to_string();
        check!(
            err.contains("negative spans declare 1 buckets but 0 counts"),
            "got: {err}"
        );

        // Lengths sum across spans rather than being taken from the first.
        let err = validate_spans_and_counts(
            0,
            &[bucket_span(0, 2), bucket_span(1, 3)],
            &[1.0, 2.0],
            &[],
            &[],
            None,
        )
        .unwrap_err()
        .to_string();
        check!(err.contains("declare 5 buckets but 2 counts"), "got: {err}");
    }

    /// Schema -53 is the custom-bucket form. Its boundaries live in
    /// `custom_values`, which must cover every populated bucket but the last --
    /// that one is the implicit `+Inf` bucket and has no upper bound to state.
    /// A custom-bucket histogram carries no negative side at all.
    #[test]
    fn custom_bucket_histograms_need_bounds_for_every_bucket_but_the_last() {
        let nhcb = |positive: &[BucketSpan], counts: &[f64], custom: Option<&[f64]>| {
            validate_spans_and_counts(-53, positive, counts, &[], &[], custom)
        };

        // One bound short of the bucket count is the normal shape: a classic
        // histogram with bounds 1 and 2 converts to three buckets, the last of
        // them `+Inf`. As many bounds as buckets is fine too, and more is fine.
        check!(nhcb(&[bucket_span(0, 3)], &[1.0, 2.0, 3.0], Some(&[1.0, 2.0])).is_ok());
        check!(nhcb(&[bucket_span(0, 2)], &[1.0, 2.0], Some(&[1.0, 2.0])).is_ok());
        check!(nhcb(&[bucket_span(0, 2)], &[1.0, 2.0], Some(&[1.0, 2.0, 3.0])).is_ok());

        // Two bounds short is not.
        let err = nhcb(&[bucket_span(0, 3)], &[1.0, 2.0, 3.0], Some(&[1.0]))
            .unwrap_err()
            .to_string();
        check!(
            err.contains("spans 3 buckets but only 1 custom values"),
            "got: {err}"
        );

        // A span offset skips over buckets that still need bounds behind them,
        // so it counts towards the span length the same way a length does. Two
        // bounds cover a span of three; the same one bucket pushed one place
        // further out spans four and no longer fits. The pinned Prometheus
        // image answers both shapes exactly this way -- it accepts the first
        // and refuses the second with "only 2 custom bounds defined which is
        // insufficient to cover total span length of 4".
        check!(nhcb(&[bucket_span(2, 1)], &[1.0], Some(&[1.0, 2.0])).is_ok());
        let err = nhcb(&[bucket_span(3, 1)], &[1.0], Some(&[1.0, 2.0]))
            .unwrap_err()
            .to_string();
        check!(
            err.contains("spans 4 buckets but only 2 custom values"),
            "got: {err}"
        );

        // No custom values at all still admits one bucket: that is the `+Inf`
        // bucket, and a classic histogram whose only bucket is `+Inf` converts
        // to exactly this. Two buckets with no bounds is refused.
        check!(nhcb(&[bucket_span(0, 1)], &[1.0], None).is_ok());
        let err = nhcb(&[bucket_span(0, 2)], &[1.0, 2.0], None)
            .unwrap_err()
            .to_string();
        check!(
            err.contains("spans 2 buckets but only 0 custom values"),
            "got: {err}"
        );

        // A negative side is rejected outright, from either field alone.
        let err = validate_spans_and_counts(
            -53,
            &[bucket_span(0, 1)],
            &[1.0],
            &[bucket_span(0, 1)],
            &[2.0],
            Some(&[1.0]),
        )
        .unwrap_err()
        .to_string();
        check!(
            err.contains("must not carry negative buckets"),
            "got: {err}"
        );

        // A span of zero length declares no buckets, so the count check
        // passes with no counts at all -- and the negative side is still
        // present. That is the only shape where rejecting on either field and
        // rejecting on both give different answers.
        let err = validate_spans_and_counts(
            -53,
            &[bucket_span(0, 1)],
            &[1.0],
            &[bucket_span(0, 0)],
            &[],
            Some(&[1.0]),
        )
        .unwrap_err()
        .to_string();
        check!(
            err.contains("must not carry negative buckets"),
            "got: {err}"
        );

        // Any other schema does not get these checks.
        check!(
            validate_spans_and_counts(0, &[bucket_span(0, 1)], &[1.0], &[], &[], None).is_ok(),
            "a normal schema needs no custom values"
        );
    }

    #[test]
    fn v1_integer_histogram_delta_decodes_to_absolute_counts() {
        let histogram = pb::v1::Histogram {
            schema: 1,
            zero_threshold: 0.001,
            positive_spans: vec![pb::v1::BucketSpan {
                offset: 0,
                length: 3,
            }],
            positive_deltas: vec![4, -1, 3],
            negative_spans: vec![pb::v1::BucketSpan {
                offset: 0,
                length: 2,
            }],
            negative_deltas: vec![2, 1],
            count: Some(pb::v1::histogram::Count::CountInt(9)),
            zero_count: Some(pb::v1::histogram::ZeroCount::ZeroCountInt(1)),
            reset_hint: pb::v1::histogram::ResetHint::Yes as i32,
            timestamp: 42,
            ..Default::default()
        };

        let native = v1_histogram_to_native(&histogram).unwrap();

        check!(!native.is_float);
        check!((native.count - 9.0).abs() < f64::EPSILON);
        check!((native.zero_count - 1.0).abs() < f64::EPSILON);
        check!(native.positive_counts == vec![4.0, 3.0, 6.0]);
        check!(native.negative_counts == vec![2.0, 3.0]);
        check!(native.reset_hint == ResetHint::Yes);
    }

    /// `is_v2_float` is a four-way disjunction, and each clause alone is enough
    /// to make a histogram float-valued: a float count, a float zero-count, or
    /// any float bucket counts on either side. Joined with `&&` instead of
    /// `||`, a histogram must satisfy all four at once -- so one carrying only
    /// float bucket counts is read as integer-valued and decoded down the wrong
    /// path.
    /// The v1 detector is the twin of the v2 one below and needs the same
    /// treatment: four independent signals joined by `||`, each of which has
    /// to be sufficient on its own. Testing them together would let a chain
    /// read as `&&` still pass.
    #[test]
    fn any_single_float_field_makes_a_v1_histogram_float() {
        let integer = pb::v1::Histogram {
            schema: 0,
            count: Some(pb::v1::histogram::Count::CountInt(4)),
            zero_count: Some(pb::v1::histogram::ZeroCount::ZeroCountInt(1)),
            positive_deltas: vec![1, 2],
            ..Default::default()
        };
        check!(!is_v1_float(&integer), "no float field anywhere");

        check!(
            is_v1_float(&pb::v1::Histogram {
                count: Some(pb::v1::histogram::Count::CountFloat(4.0)),
                ..integer.clone()
            }),
            "float count alone"
        );
        check!(
            is_v1_float(&pb::v1::Histogram {
                zero_count: Some(pb::v1::histogram::ZeroCount::ZeroCountFloat(0.5)),
                ..integer.clone()
            }),
            "float zero-count alone"
        );
        check!(
            is_v1_float(&pb::v1::Histogram {
                positive_counts: vec![1.5],
                ..integer.clone()
            }),
            "positive float counts alone"
        );
        check!(
            is_v1_float(&pb::v1::Histogram {
                negative_counts: vec![1.5],
                ..integer.clone()
            }),
            "negative float counts alone"
        );
    }

    /// The four count readers each pull one field out of a oneof that may
    /// hold an integer, a float, or nothing. They are near-identical, and
    /// there are two axes to confuse: the wire version, and count against
    /// zero-count. Every value below is distinct, so a reader reaching for its
    /// neighbour's field returns a recognisably wrong number rather than a
    /// plausible one.
    #[test]
    fn count_readers_take_their_own_field_from_either_representation() {
        let is = |actual: f64, expected: f64| (actual - expected).abs() < f64::EPSILON;

        // v2, integers.
        let v2_int = pb::v2::Histogram {
            count: Some(pb::v2::histogram::Count::CountInt(11)),
            zero_count: Some(pb::v2::histogram::ZeroCount::ZeroCountInt(22)),
            ..Default::default()
        };
        check!(is(v2_count(&v2_int), 11.0));
        check!(is(v2_zero_count(&v2_int), 22.0), "not the count beside it");

        // v2, floats: the same fields in their other representation.
        let v2_float = pb::v2::Histogram {
            count: Some(pb::v2::histogram::Count::CountFloat(33.5)),
            zero_count: Some(pb::v2::histogram::ZeroCount::ZeroCountFloat(44.5)),
            ..Default::default()
        };
        check!(is(v2_count(&v2_float), 33.5), "a float is not truncated");
        check!(is(v2_zero_count(&v2_float), 44.5));

        // v1 reads its own message, with different values again.
        let v1_int = pb::v1::Histogram {
            count: Some(pb::v1::histogram::Count::CountInt(55)),
            zero_count: Some(pb::v1::histogram::ZeroCount::ZeroCountInt(66)),
            ..Default::default()
        };
        check!(is(v1_count(&v1_int), 55.0));
        check!(is(v1_zero_count(&v1_int), 66.0));

        // An absent oneof is zero rather than an error or a default of one.
        let empty = pb::v2::Histogram::default();
        check!(is(v2_count(&empty), 0.0), "absent means zero");
        check!(is(v2_zero_count(&empty), 0.0));

        // Zero is a value in its own right, distinct from absent only in that
        // both answer zero -- so the integer path is exercised at zero too.
        let zeroed = pb::v2::Histogram {
            count: Some(pb::v2::histogram::Count::CountInt(0)),
            ..Default::default()
        };
        check!(is(v2_count(&zeroed), 0.0));
    }

    #[test]
    fn any_single_float_field_makes_a_v2_histogram_float() {
        let integer = pb::v2::Histogram {
            schema: 0,
            count: Some(pb::v2::histogram::Count::CountInt(4)),
            zero_count: Some(pb::v2::histogram::ZeroCount::ZeroCountInt(1)),
            positive_deltas: vec![1, 2],
            ..Default::default()
        };
        check!(!is_v2_float(&integer), "no float field anywhere");

        check!(
            is_v2_float(&pb::v2::Histogram {
                count: Some(pb::v2::histogram::Count::CountFloat(4.0)),
                ..integer.clone()
            }),
            "float count alone"
        );
        check!(
            is_v2_float(&pb::v2::Histogram {
                zero_count: Some(pb::v2::histogram::ZeroCount::ZeroCountFloat(0.5)),
                ..integer.clone()
            }),
            "float zero-count alone"
        );
        check!(
            is_v2_float(&pb::v2::Histogram {
                positive_counts: vec![1.5],
                ..integer.clone()
            }),
            "positive float counts alone"
        );
        check!(
            is_v2_float(&pb::v2::Histogram {
                negative_counts: vec![1.5],
                ..integer.clone()
            }),
            "negative float counts alone"
        );
    }

    #[test]
    fn v2_float_histogram_preserves_absolute_counts_and_start_timestamp() {
        let histogram = pb::v2::Histogram {
            schema: -53,
            positive_spans: vec![pb::v2::BucketSpan {
                offset: 0,
                length: 2,
            }],
            positive_counts: vec![1.5, 2.5],
            custom_values: vec![0.1, 0.2, 0.3],
            count: Some(pb::v2::histogram::Count::CountFloat(4.0)),
            zero_count: Some(pb::v2::histogram::ZeroCount::ZeroCountFloat(0.5)),
            reset_hint: pb::v2::histogram::ResetHint::Gauge as i32,
            start_timestamp: 7,
            ..Default::default()
        };

        let native = v2_histogram_to_native(&histogram).unwrap();

        check!(native.is_float);
        check!(native.is_nhcb());
        check!(native.positive_counts == vec![1.5, 2.5]);
        check!(native.custom_values == Some(vec![0.1, 0.2, 0.3]));
        check!(native.start_timestamp_ms == Some(7));
        check!(native.reset_hint == ResetHint::Gauge);
    }

    #[test]
    fn remote_write_histograms_reject_invalid_schemas() {
        for schema in [-54, -5, 9] {
            let v1 = pb::v1::Histogram {
                schema,
                ..Default::default()
            };
            let v2 = pb::v2::Histogram {
                schema,
                ..Default::default()
            };

            assert!(matches!(
                v1_histogram_to_native(&v1),
                Err(WireError::Invalid(_))
            ));
            assert!(matches!(
                v2_histogram_to_native(&v2),
                Err(WireError::Invalid(_))
            ));
        }
    }

    #[test]
    fn v1_histogram_rejects_span_count_mismatch() {
        // positive_spans claim 3 buckets, but only 2 deltas are supplied.
        let histogram = pb::v1::Histogram {
            schema: 1,
            positive_spans: vec![pb::v1::BucketSpan {
                offset: 0,
                length: 3,
            }],
            positive_deltas: vec![1, 2],
            count: Some(pb::v1::histogram::Count::CountInt(3)),
            ..Default::default()
        };

        let err = v1_histogram_to_native(&histogram).unwrap_err();

        assert!(matches!(err, WireError::Invalid(_)));
        assert!(format!("{err}").contains("positive spans declare 3 buckets but 2 counts"));
    }

    #[test]
    fn v2_histogram_rejects_negative_span_count_mismatch() {
        // negative_spans claim 1 bucket, but two float counts are supplied.
        let histogram = pb::v2::Histogram {
            schema: 0,
            negative_spans: vec![pb::v2::BucketSpan {
                offset: 0,
                length: 1,
            }],
            negative_counts: vec![1.0, 2.0],
            count: Some(pb::v2::histogram::Count::CountFloat(3.0)),
            ..Default::default()
        };

        let err = v2_histogram_to_native(&histogram).unwrap_err();

        assert!(matches!(err, WireError::Invalid(_)));
        assert!(format!("{err}").contains("negative spans declare 1 buckets but 2 counts"));
    }

    #[test]
    fn nhcb_histogram_rejects_too_few_custom_values() {
        // NHCB with 3 populated positive buckets but only 1 custom boundary.
        // Two buckets would be legal: the last one is the implicit `+Inf`.
        let histogram = pb::v2::Histogram {
            schema: -53,
            positive_spans: vec![pb::v2::BucketSpan {
                offset: 0,
                length: 3,
            }],
            positive_counts: vec![1.0, 2.0, 3.0],
            custom_values: vec![0.5],
            count: Some(pb::v2::histogram::Count::CountFloat(6.0)),
            ..Default::default()
        };

        let err = v2_histogram_to_native(&histogram).unwrap_err();

        assert!(matches!(err, WireError::Invalid(_)));
        assert!(format!("{err}").contains("custom values"));
    }

    #[test]
    fn nhcb_histogram_rejects_negative_buckets() {
        let histogram = pb::v1::Histogram {
            schema: -53,
            positive_spans: vec![pb::v1::BucketSpan {
                offset: 0,
                length: 1,
            }],
            positive_counts: vec![1.0],
            negative_spans: vec![pb::v1::BucketSpan {
                offset: 0,
                length: 1,
            }],
            negative_counts: vec![1.0],
            custom_values: vec![0.5],
            count: Some(pb::v1::histogram::Count::CountFloat(2.0)),
            ..Default::default()
        };

        let err = v1_histogram_to_native(&histogram).unwrap_err();

        assert!(matches!(err, WireError::Invalid(_)));
        assert!(format!("{err}").contains("must not carry negative buckets"));
    }

    /// An integer histogram's buckets arrive as a run of deltas that is summed
    /// into an `i64`. `i64::MAX` is the last total that run can reach; a step
    /// past it, from either end, is malformed input and has to come back as a
    /// decode error rather than a panic or a wrapped bucket count.
    #[test]
    fn a_delta_run_decodes_up_to_i64_max_and_no_further() {
        // `i64::MAX` rounded to the nearest `f64`, which is 2^63 exactly.
        const I64_MAX_AS_F64: f64 = 9.223_372_036_854_776e18;

        let v1 = |deltas: Vec<i64>| pb::v1::Histogram {
            schema: 0,
            positive_spans: vec![pb::v1::BucketSpan {
                offset: 0,
                length: u32::try_from(deltas.len()).unwrap(),
            }],
            positive_deltas: deltas,
            ..Default::default()
        };

        // Landing exactly on the boundary is a decode, not an error, and the
        // second bucket holds the same absolute count as the first.
        let native = v1_histogram_to_native(&v1(vec![i64::MAX, 0])).unwrap();
        check!(native.positive_counts == vec![I64_MAX_AS_F64, I64_MAX_AS_F64]);

        // One more than the boundary is not.
        for deltas in [
            vec![i64::MAX, 1],
            vec![i64::MIN, -1],
            vec![1, i64::MAX, i64::MAX],
        ] {
            let err = v1_histogram_to_native(&v1(deltas.clone())).unwrap_err();

            check!(matches!(err, WireError::Invalid(_)), "deltas {deltas:?}");
            check!(
                format!("{err}").contains("overflows i64"),
                "deltas {deltas:?}: {err}"
            );
            check!(err.status_code() == 400, "deltas {deltas:?}");
        }

        // The v2 message decodes through the same delta run, on the negative
        // side as well as the positive one.
        let v2 = pb::v2::Histogram {
            schema: 0,
            negative_spans: vec![pb::v2::BucketSpan {
                offset: 0,
                length: 2,
            }],
            negative_deltas: vec![i64::MAX, 1],
            ..Default::default()
        };
        let err = v2_histogram_to_native(&v2).unwrap_err();
        check!(matches!(err, WireError::Invalid(_)));
        check!(format!("{err}").contains("overflows i64"), "{err}");
    }

    /// The 44-byte `remote_write` body a fuzzer found. It holds one series,
    /// labelled `up`, carrying a v1 histogram whose two `positive_deltas` are
    /// each `i64::MAX`: the running bucket count overflows on the second. The
    /// bytes are spelled out rather than re-encoded, so the test drives what
    /// actually reaches the ingest handler.
    #[test]
    fn the_fuzzed_overflowing_histogram_body_is_a_400_not_a_panic() {
        use krabka_units::prelude::mebibytes;

        use crate::wire::decode_v1;

        const BODY: &[u8] = &[
            // Snappy block: 42 bytes uncompressed, as one 42-byte literal.
            0x2a, 0xa4, //
            // WriteRequest.timeseries, 40 bytes.
            0x0a, 0x28, //
            // TimeSeries.labels: {__name__="up"}.
            0x0a, 0x0e, 0x0a, 0x08, b'_', b'_', b'n', b'a', b'm', b'e', b'_', b'_', 0x12, 0x02,
            b'u', b'p', //
            // TimeSeries.histograms, 22 bytes.
            0x22, 0x16, //
            // Histogram.positive_deltas, twice, each the zigzag of `i64::MAX`.
            0x60, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01, //
            0x60, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01,
        ];

        let err = decode_v1(BODY, mebibytes(1)).unwrap_err();

        assert!(matches!(err, WireError::Invalid(_)));
        check!(format!("{err}").contains("overflows i64"), "{err}");
        check!(err.status_code() == 400);
    }
}

mod check_side;
mod counts;
mod is_v1_float;
mod is_v2_float;
mod schema_i8;
mod span_bucket_total;
mod v1_count;
mod v1_histogram_to_native;
mod v1_reset_hint;
mod v1_spans;
mod v1_zero_count;
mod v2_count;
mod v2_histogram_to_native;
mod v2_reset_hint;
mod v2_spans;
mod v2_zero_count;
mod validate_spans_and_counts;

use check_side::check_side;
use counts::counts;
use is_v1_float::is_v1_float;
use is_v2_float::is_v2_float;
use schema_i8::schema_i8;
use span_bucket_total::span_bucket_total;
use v1_count::v1_count;
pub use v1_histogram_to_native::v1_histogram_to_native;
use v1_reset_hint::v1_reset_hint;
use v1_spans::v1_spans;
use v1_zero_count::v1_zero_count;
use v2_count::v2_count;
pub use v2_histogram_to_native::v2_histogram_to_native;
use v2_reset_hint::v2_reset_hint;
use v2_spans::v2_spans;
use v2_zero_count::v2_zero_count;
use validate_spans_and_counts::validate_spans_and_counts;
