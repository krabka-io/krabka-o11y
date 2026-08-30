//! OTLP metrics translation into the shared ingest decode target.

use std::collections::BTreeMap;

use krabka_blockstore::Labels;
use num_traits::ToPrimitive;
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
    metrics::v1::{
        AggregationTemporality, Exemplar as OtlpExemplar, ExponentialHistogram,
        ExponentialHistogramDataPoint, Gauge, Histogram, HistogramDataPoint, Metric, MetricsData,
        NumberDataPoint, ScopeMetrics, Sum, Summary, SummaryDataPoint, exemplar as otlp_exemplar,
        exponential_histogram_data_point, metric, number_data_point,
    },
};
use prost::Message as _;

use crate::{
    BucketSpan, NativeHistogram, ResetHint,
    wire::{DecodedExemplar, DecodedMetadata, DecodedSample, DecodedSeries},
};

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_blockstore::Labels;
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value},
        metrics::v1::{
            AggregationTemporality, Exemplar, ExponentialHistogram, ExponentialHistogramDataPoint,
            Gauge, Histogram, HistogramDataPoint, Metric, MetricsData, NumberDataPoint,
            ResourceMetrics, ScopeMetrics, Sum, Summary, SummaryDataPoint,
            exemplar as otlp_exemplar, exponential_histogram_data_point, metric, number_data_point,
            summary_data_point,
        },
        resource::v1::Resource,
    };

    use crate::wire::DecodedExemplar;

    /// `accumulate_delta_float_series` turns delta sums into running totals and
    /// stamps each sample with the series start time -- but only when there IS
    /// one. A zero start time is OTLP's "unset", not an instant at the epoch,
    /// so it must leave the stamp absent rather than record a 1970 start.
    #[test]
    fn delta_accumulation_stamps_a_start_time_only_when_one_was_sent() {
        use crate::wire::{DecodedSample, DecodedSeries};

        let series_at = |value| {
            let mut labels = Labels::new();
            labels.insert("__name__", "requests_total");
            vec![DecodedSeries {
                labels,
                samples: vec![DecodedSample::new(10, value)],
                histograms: Vec::new(),
                exemplars: Vec::new(),
                metadata: None,
            }]
        };

        // With a start time, the stamp is set -- and converted to millis.
        let mut accumulator = super::DeltaAccumulator::default();
        let mut series = series_at(1.0);
        super::accumulate_delta_float_series(&mut series, 1_500_000_000, &mut accumulator);
        check!(series[0].samples[0].start_timestamp_ms == Some(1_500));

        // Deltas accumulate across calls at the same start time.
        let mut next = series_at(2.0);
        super::accumulate_delta_float_series(&mut next, 1_500_000_000, &mut accumulator);
        check!(
            (next[0].samples[0].value - 3.0).abs() < f64::EPSILON,
            "the running total, not the delta"
        );
        check!(next[0].samples[0].start_timestamp_ms == Some(1_500));

        // Zero means unset. The value still accumulates, but no start time is
        // recorded: stamping it would claim the series began at the epoch.
        let mut unset = super::DeltaAccumulator::default();
        let mut series = series_at(1.0);
        super::accumulate_delta_float_series(&mut series, 0, &mut unset);
        check!(
            series[0].samples[0].start_timestamp_ms.is_none(),
            "an unset start time is absent, not 1970"
        );
        check!((series[0].samples[0].value - 1.0).abs() < f64::EPSILON);
    }

    /// A cumulative or unspecified sum or histogram is ingested as-is, a delta
    /// one is accumulated -- `decode_otlp` supplies a per-call accumulator, so
    /// delta is supported here rather than refused -- and an aggregation
    /// temporality that is none of the three is refused rather than guessed at.
    ///
    /// The guard reads `!= Cumulative && != Unspecified`, so BOTH accepted
    /// temporalities have to be exercised: with only one of them, flipping
    /// either comparison leaves the other still passing. All three metric
    /// kinds carry their own copy of the guard, so each is checked.
    #[test]
    fn only_cumulative_and_unspecified_temporalities_are_ingested_directly() {
        use prost::Message as _;

        let point = || NumberDataPoint {
            time_unix_nano: 1_000_000_000,
            value: Some(number_data_point::Value::AsDouble(1.0)),
            ..NumberDataPoint::default()
        };
        let body = |data: metric::Data| {
            let mut bytes = Vec::new();
            MetricsData {
                resource_metrics: vec![ResourceMetrics {
                    resource: Some(Resource::default()),
                    scope_metrics: vec![ScopeMetrics {
                        scope: Some(InstrumentationScope::default()),
                        metrics: vec![Metric {
                            name: "requests".to_string(),
                            data: Some(data),
                            ..Metric::default()
                        }],
                        ..ScopeMetrics::default()
                    }],
                    ..ResourceMetrics::default()
                }],
            }
            .encode(&mut bytes)
            .expect("the payload encodes");
            bytes
        };
        let decodes = |data: metric::Data| {
            super::decode_otlp_bytes(&body(data), TranslationStrategy::default()).is_ok()
        };

        // An unrecognised temporality. Neither of the accepted values, and
        // not delta either, so only the guard can reject it.
        let unknown = 99_i32;
        let cumulative = AggregationTemporality::Cumulative as i32;
        let unspecified = AggregationTemporality::Unspecified as i32;
        let delta = AggregationTemporality::Delta as i32;

        let sum = |temporality| {
            metric::Data::Sum(Sum {
                data_points: vec![point()],
                aggregation_temporality: temporality,
                is_monotonic: true,
            })
        };
        check!(decodes(sum(cumulative)));
        check!(decodes(sum(unspecified)));
        check!(
            decodes(sum(delta)),
            "delta is accumulated, not refused, on this path"
        );
        check!(
            !decodes(sum(unknown)),
            "but an unknown temporality is refused"
        );

        let histogram = |temporality| {
            metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    time_unix_nano: 1_000_000_000,
                    count: 0,
                    ..HistogramDataPoint::default()
                }],
                aggregation_temporality: temporality,
            })
        };
        check!(decodes(histogram(cumulative)));
        check!(decodes(histogram(unspecified)));
        check!(decodes(histogram(delta)));
        check!(!decodes(histogram(unknown)));

        let exponential = |temporality| {
            metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 1_000_000_000,
                    count: 0,
                    scale: 0,
                    ..ExponentialHistogramDataPoint::default()
                }],
                aggregation_temporality: temporality,
            })
        };
        check!(decodes(exponential(cumulative)));
        check!(decodes(exponential(unspecified)));
        check!(decodes(exponential(delta)));
        check!(!decodes(exponential(unknown)));
    }

    /// `exponential_histogram_to_native` maps an OTLP scale onto a native
    /// histogram schema. The two ends behave differently and that asymmetry is
    /// the point: a scale below the minimum is REFUSED, because there is no
    /// schema to represent it, while one above the maximum is CLAMPED, because
    /// a finer scale can always be downscaled into a coarser schema.
    ///
    /// Both are strict boundaries, so each is checked exactly at its edge and
    /// one step outside it.
    #[test]
    fn an_exponential_histogram_scale_is_clamped_above_and_refused_below() {
        let at_scale = |scale| ExponentialHistogramDataPoint {
            scale,
            count: 0,
            sum: None,
            zero_count: 0,
            positive: None,
            negative: None,
            ..ExponentialHistogramDataPoint::default()
        };
        let schema = |scale| {
            super::exponential_histogram_to_native(&at_scale(scale))
                .ok()
                .map(|native| native.schema)
        };

        // -4 is the lowest schema there is, so it converts rather than being
        // refused as out of range.
        check!(schema(-4) == Some(-4), "exactly at the minimum");
        check!(schema(-5).is_none(), "one below it has no schema");
        check!(schema(-100).is_none());

        // Between the ends the scale passes through untouched.
        check!(schema(0) == Some(0));
        check!(schema(3) == Some(3));

        // 8 is the highest schema, and anything finer is clamped to it rather
        // than refused -- the buckets are merged down instead.
        check!(schema(8) == Some(8), "exactly at the maximum");
        check!(schema(9) == Some(8), "one above it clamps, not errors");
        check!(schema(127) == Some(8));
    }

    /// `reject_far_future_points` refuses a batch carrying any timestamp past
    /// the year 2200. A clock-skewed producer can otherwise pin a series'
    /// upper time bound centuries out, which no query range will ever reach
    /// again -- the damage outlives the bad batch.
    ///
    /// The bound is exclusive of itself: a point landing exactly on the limit
    /// is accepted and one millisecond past it is not. All five data kinds
    /// carry their own extraction, so each is checked, and a good point is
    /// placed BEFORE a bad one so the scan cannot pass by looking only at the
    /// first.
    #[test]
    fn a_far_future_data_point_is_refused_whatever_kind_carries_it() {
        let limit_ns = super::MAX_SAMPLE_TIMESTAMP_MS * 1_000_000;
        let ok = 1_500_000_000_000_000_000_u64;

        let number = |time_unix_nano| NumberDataPoint {
            time_unix_nano,
            ..NumberDataPoint::default()
        };
        let check_kind = |data: metric::Data| super::reject_far_future_points("m", &data);

        // Each kind, with a good point first and a far-future one after it.
        check!(
            check_kind(metric::Data::Gauge(Gauge {
                data_points: vec![number(ok), number(limit_ns + 1_000_000)],
            }))
            .is_err(),
            "a gauge's later point is still scanned"
        );
        check!(
            check_kind(metric::Data::Sum(Sum {
                data_points: vec![number(ok), number(limit_ns + 1_000_000)],
                ..Sum::default()
            }))
            .is_err()
        );
        check!(
            check_kind(metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    time_unix_nano: limit_ns + 1_000_000,
                    ..HistogramDataPoint::default()
                }],
                ..Histogram::default()
            }))
            .is_err()
        );
        check!(
            check_kind(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: limit_ns + 1_000_000,
                    ..ExponentialHistogramDataPoint::default()
                }],
                ..ExponentialHistogram::default()
            }))
            .is_err()
        );
        check!(
            check_kind(metric::Data::Summary(Summary {
                data_points: vec![SummaryDataPoint {
                    time_unix_nano: limit_ns + 1_000_000,
                    ..SummaryDataPoint::default()
                }],
            }))
            .is_err()
        );

        // Exactly at the limit is accepted; one millisecond past is not.
        check!(
            check_kind(metric::Data::Gauge(Gauge {
                data_points: vec![number(limit_ns)],
            }))
            .is_ok(),
            "the limit itself is a usable timestamp"
        );
        check!(
            check_kind(metric::Data::Gauge(Gauge {
                data_points: vec![number(limit_ns + 1_000_000)],
            }))
            .is_err(),
            "one millisecond past it is not"
        );
        // Sub-millisecond precision is truncated before the comparison, so a
        // point within the same millisecond as the limit is still accepted.
        check!(
            check_kind(metric::Data::Gauge(Gauge {
                data_points: vec![number(limit_ns + 999_999)],
            }))
            .is_ok(),
            "still the same millisecond"
        );

        // Ordinary and empty batches pass.
        check!(
            check_kind(metric::Data::Gauge(Gauge {
                data_points: vec![number(ok)],
            }))
            .is_ok()
        );
        check!(
            check_kind(metric::Data::Gauge(Gauge {
                data_points: Vec::new(),
            }))
            .is_ok()
        );

        // The refusal names the metric and the offending timestamp.
        let error = check_kind(metric::Data::Gauge(Gauge {
            data_points: vec![number(limit_ns + 1_000_000)],
        }))
        .expect_err("a far-future point is refused");
        check!(
            error
                .to_string()
                .contains(&(limit_ns + 1_000_000).to_string())
        );
    }

    /// `resource_metrics_timestamp_ms` reports when a batch was observed,
    /// taking the first data point it can find and converting nanos to
    /// millis. It walks five data kinds, so the timestamp is read from each in
    /// turn -- and the value is chosen so a body collapsed to a constant, or
    /// one that forgets to convert, is a different number.
    #[test]
    fn a_resource_batch_reports_the_first_timestamp_it_can_find() {
        let point = |nanos| NumberDataPoint {
            time_unix_nano: nanos,
            ..NumberDataPoint::default()
        };
        let batch = |data: Option<metric::Data>| ResourceMetrics {
            resource: Some(Resource::default()),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "m".to_string(),
                    data,
                    ..Metric::default()
                }],
                ..ScopeMetrics::default()
            }],
            ..ResourceMetrics::default()
        };
        let at = |data| super::resource_metrics_timestamp_ms(&batch(Some(data)));

        // 1_500_000_000ns is 1500ms: not 1, and not the nanos it came from.
        let nanos = 1_500_000_000;
        check!(
            at(metric::Data::Gauge(Gauge {
                data_points: vec![point(nanos)],
            })) == Some(1_500)
        );
        check!(
            at(metric::Data::Sum(Sum {
                data_points: vec![point(nanos)],
                ..Sum::default()
            })) == Some(1_500)
        );
        check!(
            at(metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    time_unix_nano: nanos,
                    ..HistogramDataPoint::default()
                }],
                ..Histogram::default()
            })) == Some(1_500)
        );
        check!(
            at(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: nanos,
                    ..ExponentialHistogramDataPoint::default()
                }],
                ..ExponentialHistogram::default()
            })) == Some(1_500)
        );
        check!(
            at(metric::Data::Summary(Summary {
                data_points: vec![SummaryDataPoint {
                    time_unix_nano: nanos,
                    ..SummaryDataPoint::default()
                }],
            })) == Some(1_500)
        );

        // The FIRST point wins, not the last.
        check!(
            at(metric::Data::Gauge(Gauge {
                data_points: vec![point(nanos), point(9_000_000_000)],
            })) == Some(1_500)
        );

        // Nothing to report: no data, no points, and no metrics at all.
        check!(super::resource_metrics_timestamp_ms(&batch(None)).is_none());
        check!(
            at(metric::Data::Gauge(Gauge {
                data_points: Vec::new(),
            }))
            .is_none()
        );
        check!(
            super::resource_metrics_timestamp_ms(&ResourceMetrics::default()).is_none(),
            "an empty batch has no timestamp rather than a zero one"
        );
    }

    /// `compact_spanned_histogram_counts` run-length-encodes non-zero buckets
    /// into spans, where each span's offset is measured from the END of the
    /// previous one rather than from zero. That relative encoding is all
    /// subtraction and off-by-ones, and the survivors were every one of those
    /// operators -- so the fixture has three spans whose offsets and lengths
    /// are all distinct, and none of which is zero or one.
    #[test]
    fn compacting_histogram_buckets_encodes_each_span_relative_to_the_last() {
        let compact = |buckets: &[(i32, f64)]| {
            super::compact_spanned_histogram_counts(buckets.iter().copied().collect())
        };
        let span = |offset, length| BucketSpan { offset, length };

        // Buckets 1-2, then 5, then 9-10. Offsets are 1, 2 and 3 counting
        // from each previous span's end; lengths are 2, 1 and 2.
        check!(
            compact(&[(1, 5.0), (2, 6.0), (5, 7.0), (9, 8.0), (10, 9.0)])
                == (
                    vec![span(1, 2), span(2, 1), span(3, 2)],
                    vec![5.0, 6.0, 7.0, 8.0, 9.0],
                )
        );

        // Empty buckets are dropped before spanning, so a zero in the middle
        // splits one span into two rather than being carried as a count.
        check!(
            compact(&[(1, 5.0), (2, 0.0), (3, 6.0)])
                == (vec![span(1, 1), span(1, 1)], vec![5.0, 6.0])
        );

        // A single bucket at zero is offset zero, length one -- the identity
        // case, which is why the fixture above avoids those values.
        check!(compact(&[(0, 5.0)]) == (vec![span(0, 1)], vec![5.0]));

        // Negative indices are below the zero bucket and offset accordingly.
        check!(compact(&[(-2, 5.0)]) == (vec![span(-2, 1)], vec![5.0]));
        check!(
            compact(&[(-2, 5.0), (-1, 6.0), (2, 7.0)])
                == (vec![span(-2, 2), span(2, 1)], vec![5.0, 6.0, 7.0])
        );

        // Nothing to encode: no buckets, and buckets that are all empty.
        check!(compact(&[]) == (Vec::new(), Vec::new()));
        check!(compact(&[(1, 0.0), (2, 0.0)]) == (Vec::new(), Vec::new()));
    }

    /// `strip_ucum_annotations` drops the `{...}` annotations UCUM allows on a
    /// unit, keeping everything else. The two braces are handled by different
    /// arms and are deliberately not symmetric: an unmatched `{` swallows the
    /// rest of the string, while an unmatched `}` is kept as an ordinary
    /// character. Both are pinned, since a mutant collapsing either arm looks
    /// reasonable against balanced input alone.
    #[test]
    fn stripping_ucum_annotations_keeps_everything_outside_the_braces() {
        let strip = super::strip_ucum_annotations;

        // Nothing to strip.
        check!(strip("By") == "By");
        check!(strip("") == "");

        // A whole unit that is only an annotation leaves nothing.
        check!(strip("{packets}") == "");

        // Annotations are removed in place, before and after real text.
        check!(strip("1{fraction}") == "1");
        check!(strip("{spans}/s") == "/s");
        check!(strip("m{tilt}/s") == "m/s");
        check!(strip("{a}b{c}d") == "bd", "several annotations in one unit");

        // Braces are not symmetric. An unmatched `{` opens an annotation that
        // never closes, so the remainder is dropped ...
        check!(strip("a{b") == "a");
        // ... while an unmatched `}` was never in one, and is kept.
        check!(strip("a}b") == "a}b");
        check!(strip("}") == "}");

        // A nested `{` inside an annotation stays inside it: the flag is a
        // boolean, not a depth counter, so the first `}` closes it.
        check!(strip("{a{b}c") == "c");
    }

    /// `prometheus_unit_suffix` turns a UCUM unit into a metric-name suffix.
    /// The dimensionless spellings produce no suffix at all, which is distinct
    /// from producing an empty one, and a rate keeps its numerator's suffix
    /// rather than the whole unit's.
    #[test]
    fn unit_suffixes_drop_the_dimensionless_and_keep_rate_numerators() {
        let suffix = super::prometheus_unit_suffix;

        check!(suffix("s") == Some("seconds".to_string()));
        check!(suffix("By") == Some("bytes".to_string()));

        // Dimensionless: no suffix, not an empty one. These reach None twice
        // over -- the early return catches them, and the unit table has no
        // entry for them either -- so the guard is a statement of intent
        // rather than the thing producing the answer.
        check!(suffix("") == None);
        check!(suffix("1") == None, "one is dimensionless");
        check!(suffix("  ") == None, "whitespace trims to empty");
        check!(
            suffix("{requests}") == None,
            "an annotation alone is dimensionless"
        );

        // Surrounding whitespace and annotations are removed before matching.
        check!(suffix(" s ") == Some("seconds".to_string()));
        check!(suffix("s{cpu}") == Some("seconds".to_string()));

        // A rate takes its numerator's suffix and appends the period.
        check!(suffix("By/s") == Some("bytes_per_second".to_string()));
        check!(suffix("s/s") == Some("seconds_per_second".to_string()));

        // A rate whose numerator is not a known unit is not a rate.
        check!(suffix("zz/s") == None);
        // Nor is an unknown unit a unit.
        check!(suffix("zz") == None);
    }

    /// `exemplar_belongs_to_bucket` places a value in a half-open bucket:
    /// above the lower bound, at or below the upper. Both edges are checked
    /// on the same bucket, since a bound that flipped inclusivity would move
    /// the value only at the boundary itself.
    #[test]
    fn an_exemplar_belongs_to_the_bucket_that_upper_bounds_it() {
        let point = HistogramDataPoint {
            explicit_bounds: vec![10.0, 20.0],
            ..Default::default()
        };
        let belongs =
            |value: f64, bucket: usize| super::exemplar_belongs_to_bucket(value, &point, bucket);

        // The first bucket has no lower bound and is closed at ten.
        check!(belongs(5.0, 0));
        check!(
            belongs(10.0, 0),
            "the upper bound belongs to its own bucket"
        );
        check!(!belongs(10.1, 0));

        // The middle bucket is open below and closed above.
        check!(
            !belongs(10.0, 1),
            "the lower bound belongs to the bucket below"
        );
        check!(belongs(10.1, 1));
        check!(belongs(20.0, 1));
        check!(!belongs(20.1, 1));

        // The last bucket has no upper bound.
        check!(belongs(20.1, 2));
        check!(belongs(1e9, 2));
        check!(
            !belongs(20.0, 2),
            "twenty belongs to the bucket that closes at it"
        );
    }

    /// `strip_ucum_annotations` removes the `{...}` parts of a UCUM unit,
    /// which carry a human note rather than a dimension. The braces are the
    /// only delimiters, and the states they move between are what the cases
    /// below separate.
    #[test]
    fn ucum_annotations_are_removed_and_the_rest_kept() {
        let strip = super::strip_ucum_annotations;

        check!(strip("s") == "s", "a bare unit is untouched");
        check!(strip("") == "");
        check!(
            strip("{requests}") == "",
            "an annotation alone leaves nothing"
        );
        check!(
            strip("1{requests}") == "1",
            "the unit survives, the note does not"
        );
        check!(strip("{requests}1") == "1", "whichever side it is on");
        check!(
            strip("m{note}s") == "ms",
            "and in the middle, joining what it split"
        );
        check!(strip("{}") == "", "an empty annotation");
        check!(strip("{a}{b}") == "", "two of them");
        check!(strip("k{a}g{b}") == "kg");

        // A brace that never closes swallows the rest, since there is no
        // annotation end to return from.
        check!(strip("s{note") == "s");
        // A closing brace with nothing open is *kept*, because the arm that
        // consumes it is guarded on being inside an annotation and an
        // unguarded character falls through to the one that copies it. That is
        // asymmetric with the unclosed open above, so it is pinned rather than
        // assumed to mirror it.
        check!(strip("s}") == "s}");
        check!(strip("}s") == "}s");
    }

    /// Every OTLP ingest failure is a client error, whatever went wrong. The
    /// code is pinned for each variant rather than once, since a per-variant
    /// answer is what this would grow into and 400 for one is not 400 for all.
    #[test]
    fn every_otlp_error_reports_a_client_error_status() {
        use super::OtlpError;

        for error in [
            OtlpError::DeltaUnsupported("m".into()),
            OtlpError::Invalid("m".into(), "why".into()),
            OtlpError::Unsupported("m".into(), "why".into()),
        ] {
            check!(error.status_code() == 400, "{error}");
        }

        // A decode failure is a client error too, not a server one. The error
        // comes from a real failed decode rather than a constructed one, so
        // the variant is reached the way ingest reaches it.
        let decode = super::decode_otlp_bytes(&[0xff, 0xff], TranslationStrategy::default())
            .expect_err("two 0xff bytes are not a MetricsData");
        check!(
            matches!(decode, OtlpError::ProtobufDecode(_)),
            "got {decode:?}"
        );
        check!(decode.status_code() == 400);
        check!(decode.status_code() != 500, "not a server error");
    }

    /// `accumulate_histogram` folds each delta into a running cumulative, and
    /// starts over when the series reports a new start time. It shares the
    /// three-condition reset guard with `accumulate_sum`, so each condition is
    /// checked with the other two satisfied, and the counts differ per delta
    /// so a fold that replaces instead of adding is visible.
    #[test]
    fn delta_histograms_fold_until_the_series_restarts() {
        use krabka_blockstore::Labels;

        use crate::{ResetHint, histogram::NativeHistogram};

        let hist = |count: f64| NativeHistogram {
            schema: 0,
            is_float: false,
            reset_hint: ResetHint::Unknown,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count,
            sum: count,
            positive_spans: Vec::new(),
            positive_counts: Vec::new(),
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: None,
            start_timestamp_ms: None,
        };
        let mut labels = Labels::default();
        labels.insert("__name__", "latency");
        let mut acc = super::DeltaAccumulator::default();
        let is = |actual: f64, expected: f64| (actual - expected).abs() < f64::EPSILON;

        // Deltas fold together while the start time holds.
        let first = acc
            .accumulate_histogram("m", &labels, 100, hist(1.0))
            .expect("folds");
        check!(is(first.count, 1.0));
        let second = acc
            .accumulate_histogram("m", &labels, 100, hist(2.0))
            .expect("folds");
        check!(is(second.count, 3.0), "folded, not replaced");
        let third = acc
            .accumulate_histogram("m", &labels, 100, hist(4.0))
            .expect("folds");
        check!(is(third.count, 7.0));

        // A new start time restarts the cumulative at the delta.
        let restarted = acc
            .accumulate_histogram("m", &labels, 200, hist(5.0))
            .expect("folds");
        check!(is(restarted.count, 5.0), "reset");
        let after = acc
            .accumulate_histogram("m", &labels, 200, hist(1.0))
            .expect("folds");
        check!(is(after.count, 6.0), "then folds again");

        // A start time of zero means "not reported" and must not restart.
        let unreported = acc
            .accumulate_histogram("m", &labels, 0, hist(1.0))
            .expect("folds");
        check!(is(unreported.count, 7.0), "zero does not reset");

        // A second series keeps its own cumulative.
        let mut other = Labels::default();
        other.insert("__name__", "other");
        let separate = acc
            .accumulate_histogram("m", &other, 100, hist(9.0))
            .expect("folds");
        check!(is(separate.count, 9.0), "separate key");
        let back = acc
            .accumulate_histogram("m", &labels, 200, hist(1.0))
            .expect("folds");
        check!(is(back.count, 8.0), "the first is untouched");

        // A delta whose layout differs is refused rather than folded.
        let mut incompatible = hist(1.0);
        incompatible.schema = 3;
        check!(
            acc.accumulate_histogram("m", &labels, 200, incompatible)
                .is_err(),
            "an incompatible layout cannot fold"
        );
    }

    /// `accumulate_sum` adds each delta to a running total, and starts over
    /// when the series reports a new start time. Three conditions have to hold
    /// together for that reset, so each is checked with the other two
    /// satisfied -- otherwise a condition flipped from `!=` to `==` is masked
    /// by one of its neighbours already being false.
    #[test]
    fn delta_sums_accumulate_until_the_series_restarts() {
        use krabka_blockstore::Labels;

        let mut labels = Labels::default();
        labels.insert("__name__", "requests");
        let mut acc = super::DeltaAccumulator::default();
        let is = |actual: f64, expected: f64| (actual - expected).abs() < f64::EPSILON;

        // Deltas add up while the start time stays the same.
        check!(is(acc.accumulate_sum(&labels, 100, 1.0), 1.0));
        check!(
            is(acc.accumulate_sum(&labels, 100, 2.0), 3.0),
            "added, not replaced"
        );
        check!(is(acc.accumulate_sum(&labels, 100, 4.0), 7.0));

        // A new start time means a new series: the total starts over at the
        // delta rather than continuing.
        check!(is(acc.accumulate_sum(&labels, 200, 5.0), 5.0), "reset");
        check!(
            is(acc.accumulate_sum(&labels, 200, 1.0), 6.0),
            "then accumulates again"
        );

        // A start time of zero means "not reported" and must not reset, even
        // though it differs from the recorded one.
        check!(
            is(acc.accumulate_sum(&labels, 0, 1.0), 7.0),
            "zero does not reset"
        );

        // A second series under different labels keeps its own total.
        let mut other = Labels::default();
        other.insert("__name__", "errors");
        check!(
            is(acc.accumulate_sum(&other, 100, 9.0), 9.0),
            "separate key"
        );
        check!(
            is(acc.accumulate_sum(&labels, 200, 1.0), 8.0),
            "the first is untouched"
        );

        // A series whose recorded start is still zero accumulates rather than
        // resetting, then records the start it was given.
        let mut fresh = super::DeltaAccumulator::default();
        let mut third = Labels::default();
        third.insert("__name__", "latency");
        check!(
            is(fresh.accumulate_sum(&third, 0, 2.0), 2.0),
            "no start recorded yet"
        );
        check!(
            is(fresh.accumulate_sum(&third, 300, 3.0), 5.0),
            "the first real start does not reset what came before it"
        );
        check!(is(fresh.accumulate_sum(&third, 300, 1.0), 6.0));
        check!(
            is(fresh.accumulate_sum(&third, 400, 1.0), 1.0),
            "but a later change does"
        );
    }

    /// `add_compatible_native_histogram` refuses to fold a delta into a
    /// cumulative whose layout differs, and there are five ways it can differ.
    /// Each is checked with that field alone changed: the conditions are
    /// joined by `or`, so with two fields differing at once, joining them by
    /// `and` instead would still reject and the mutant would live.
    #[test]
    fn a_delta_histogram_must_match_the_cumulative_layout_in_every_respect() {
        use crate::{ResetHint, histogram::NativeHistogram};

        let base = || NativeHistogram {
            schema: 2,
            is_float: false,
            reset_hint: ResetHint::Unknown,
            zero_threshold: 1e-9,
            zero_count: 0.0,
            count: 1.0,
            sum: 1.0,
            positive_spans: Vec::new(),
            positive_counts: Vec::new(),
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: None,
            start_timestamp_ms: None,
        };

        // Identical layouts fold without complaint.
        let mut cumulative = base();
        check!(super::add_compatible_native_histogram("m", &mut cumulative, &base()).is_ok());

        // Each field alone is enough to refuse.
        let differs = |mutate: &dyn Fn(&mut NativeHistogram)| {
            let mut cumulative = base();
            let mut delta = base();
            mutate(&mut delta);
            super::add_compatible_native_histogram("m", &mut cumulative, &delta).is_err()
        };
        check!(differs(&|h| h.schema = 3), "schema");
        check!(differs(&|h| h.is_float = true), "is_float");
        check!(differs(&|h| h.reset_hint = ResetHint::Gauge), "reset_hint");
        check!(differs(&|h| h.zero_threshold = 2e-9), "zero_threshold");
        check!(
            differs(&|h| h.custom_values = Some(vec![1.0])),
            "custom_values"
        );

        // The zero threshold is compared by bits, not by value. Positive and
        // negative zero are equal under `==` and differ in their bits, so this
        // pair is the only one that separates the two comparisons.
        let mut cumulative = NativeHistogram {
            zero_threshold: 0.0,
            ..base()
        };
        let delta = NativeHistogram {
            zero_threshold: -0.0,
            ..base()
        };
        check!(
            super::add_compatible_native_histogram("m", &mut cumulative, &delta).is_err(),
            "negative zero is a different layout from zero"
        );

        // Fields outside the layout do not make a delta incompatible; they are
        // what the fold is for.
        let mut cumulative = base();
        let mut delta = base();
        delta.count = 5.0;
        delta.sum = 9.0;
        check!(super::add_compatible_native_histogram("m", &mut cumulative, &delta).is_ok());
    }

    fn span(offset: i32, length: u32) -> crate::histogram::BucketSpan {
        crate::histogram::BucketSpan { offset, length }
    }

    fn bucket_map(pairs: &[(i32, f64)]) -> std::collections::BTreeMap<i32, f64> {
        pairs.iter().copied().collect()
    }

    /// Sparse histogram buckets are stored as spans of consecutive indexes.
    /// A span's offset is a *delta* from where the previous span ended, not an
    /// absolute index, which is the part that is easy to get wrong and
    /// invisible whenever there is only one span.
    #[test]
    fn bucket_spans_encode_gaps_as_deltas_from_the_previous_span() {
        let compact = super::compact_spanned_histogram_counts;

        // One run starting at zero.
        check!(compact(bucket_map(&[(0, 1.0), (1, 2.0)])) == (vec![span(0, 2)], vec![1.0, 2.0]));

        // One run starting away from zero: the first offset is absolute.
        check!(compact(bucket_map(&[(5, 1.0)])) == (vec![span(5, 1)], vec![1.0]));

        // Two runs. The second offset is measured from the end of the first,
        // so it is 1 rather than 3.
        check!(
            compact(bucket_map(&[(0, 1.0), (1, 2.0), (3, 4.0)]))
                == (vec![span(0, 2), span(1, 1)], vec![1.0, 2.0, 4.0])
        );

        // Three runs. Only from the second gap onwards is the previous span's
        // end non-zero, so this is the first case where subtracting it and
        // adding it give different answers.
        check!(
            compact(bucket_map(&[(0, 1.0), (2, 2.0), (4, 3.0)]))
                == (
                    vec![span(0, 1), span(1, 1), span(1, 1)],
                    vec![1.0, 2.0, 3.0]
                )
        );

        // Negative indexes are ordinary indexes.
        check!(compact(bucket_map(&[(-2, 1.0), (-1, 2.0)])) == (vec![span(-2, 2)], vec![1.0, 2.0]));

        // Empty buckets are dropped, which is what creates the gap here.
        check!(
            compact(bucket_map(&[(0, 1.0), (1, 0.0), (2, 3.0)]))
                == (vec![span(0, 1), span(1, 1)], vec![1.0, 3.0])
        );

        check!(
            compact(bucket_map(&[])) == (vec![], vec![]),
            "no buckets, no spans"
        );
        check!(
            compact(bucket_map(&[(0, 0.0)])) == (vec![], vec![]),
            "only empty buckets is the same as none"
        );
    }

    /// Downscaling merges neighbouring buckets and re-encodes them as spans.
    /// The spans it produces are read back by the same delta-offset decoder
    /// the merge path uses, so the two have to agree: a run that starts three
    /// buckets after the previous one ended must carry an offset of three,
    /// not its absolute index.
    ///
    /// The input here is deliberately sparse. With a single run the two
    /// conventions coincide, and the disagreement only shows from the second
    /// run onwards.
    #[test]
    fn downscaled_spans_are_decoded_back_to_the_buckets_they_came_from() {
        // Source buckets at offset 0: indexes 0..6 with gaps, halved by the
        // downscale to schema-1, so indexes 0, 1, 2, 3 carry the merged
        // counts of pairs (0,1), (2,3), (4,5), (6,7).
        let buckets = exponential_histogram_data_point::Buckets {
            offset: 0,
            bucket_counts: vec![1, 0, 0, 0, 2, 0, 0, 3],
        };

        let (spans, counts) =
            super::downscaled_spans(Some(&buckets), 0, -1).expect("downscale succeeds");

        // The three populated source buckets merge onto indexes 1, 3 and 4,
        // and decoding the spans has to land back on exactly those.
        let decoded = super::spanned_histogram_counts(&spans, &counts);
        check!(
            decoded == bucket_map(&[(1, 1.0), (3, 2.0), (4, 3.0)]),
            "spans {spans:?} counts {counts:?} decoded to {decoded:?}"
        );

        // The second run begins one index after the first ended, so its
        // offset is 1. Encoded as an absolute index it would be 3, and the
        // decoder would place its counts at 5 and 6 instead.
        check!(spans == vec![span(1, 1), span(1, 2)], "got {spans:?}");
        check!(counts == vec![1.0, 2.0, 3.0], "got {counts:?}");
    }

    /// Decoding spans back into buckets has to undo the delta encoding, so
    /// the two are checked as a round trip over shapes that exercise each
    /// part: a single run, several runs, negative indexes, and a gap.
    #[test]
    fn bucket_spans_survive_a_round_trip() {
        for pairs in [
            vec![],
            vec![(0, 1.0)],
            vec![(5, 1.0)],
            vec![(-3, 1.0)],
            vec![(0, 1.0), (1, 2.0)],
            vec![(0, 1.0), (1, 2.0), (3, 4.0)],
            vec![(-2, 1.0), (-1, 2.0), (4, 3.0), (9, 5.0)],
        ] {
            let original = bucket_map(&pairs);
            let (spans, counts) = super::compact_spanned_histogram_counts(original.clone());
            let decoded = super::spanned_histogram_counts(&spans, &counts);
            check!(decoded == original, "round trip of {pairs:?} via {spans:?}");
        }
    }

    /// A span may claim more buckets than there are counts to fill them. The
    /// decoder stops rather than inventing values or panicking, so a truncated
    /// payload yields the prefix it can actually account for.
    #[test]
    fn decoding_stops_when_the_counts_run_out() {
        let decoded = super::spanned_histogram_counts(&[span(0, 4)], &[1.0, 2.0]);
        check!(decoded == bucket_map(&[(0, 1.0), (1, 2.0)]));

        let decoded = super::spanned_histogram_counts(&[span(0, 2), span(3, 2)], &[1.0, 2.0, 3.0]);
        check!(
            decoded == bucket_map(&[(0, 1.0), (1, 2.0), (5, 3.0)]),
            "the second span still starts where its offset says"
        );

        check!(
            super::spanned_histogram_counts(&[], &[1.0]).is_empty(),
            "no spans, no buckets"
        );
    }

    /// Every OTLP unit the table maps, checked one by one.
    ///
    /// A deleted arm falls through to `None`, and the caller then emits the
    /// metric with no unit suffix at all -- `http_request_duration` instead of
    /// `http_request_duration_milliseconds`. Prometheus treats those as
    /// different series, so the sample lands somewhere nothing queries. The
    /// whole table is listed because a per-arm gap is invisible from any one
    /// unit: seven of these were uncovered.
    #[test]
    fn every_otlp_unit_maps_to_its_prometheus_suffix() {
        for (unit, want) in [
            ("d", "days"),
            ("h", "hours"),
            ("min", "minutes"),
            ("s", "seconds"),
            ("ms", "milliseconds"),
            ("us", "microseconds"),
            ("ns", "nanoseconds"),
            ("m", "meters"),
            ("By", "bytes"),
            ("KiBy", "kibibytes"),
            ("MiBy", "mebibytes"),
            ("GiBy", "gibibytes"),
            ("TiBy", "tebibytes"),
            ("kBy", "kilobytes"),
            ("MBy", "megabytes"),
            ("GBy", "gigabytes"),
            ("TBy", "terabytes"),
            ("bit", "bits"),
            ("V", "volts"),
            ("A", "amperes"),
            ("J", "joules"),
            ("W", "watts"),
            ("g", "grams"),
            ("Cel", "celsius"),
            ("Hz", "hertz"),
            ("%", "percent"),
        ] {
            assert2::check!(
                super::prometheus_base_unit_suffix(unit) == Some(want),
                "unit {unit:?}"
            );
        }

        // Not a unit the table knows, and the empty string: both carry no
        // suffix rather than a wrong one.
        assert2::check!(super::prometheus_base_unit_suffix("furlong") == None);
        assert2::check!(super::prometheus_base_unit_suffix("") == None);
    }

    use super::{DeltaAccumulator, TranslationStrategy, decode_otlp, decode_otlp_stateful};
    use crate::{
        BucketSpan,
        wire::{DecodedMetadata, DecodedSample, DecodedSeries},
    };

    fn kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.to_string())),
            }),
            key_strindex: 0,
        }
    }

    fn number_point(value: f64, timestamp: u64, attributes: Vec<KeyValue>) -> NumberDataPoint {
        NumberDataPoint {
            attributes,
            time_unix_nano: timestamp,
            value: Some(number_data_point::Value::AsDouble(value)),
            ..Default::default()
        }
    }

    fn metrics_data(metric: Metric) -> MetricsData {
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![metric],
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn metrics_data_with_resource(
        metric: Metric,
        resource_attributes: Vec<KeyValue>,
    ) -> MetricsData {
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: resource_attributes,
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![metric],
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        let mut labels = Labels::new();
        for (name, value) in pairs {
            labels.insert((*name).to_string(), (*value).to_string());
        }
        labels
    }

    fn sample_value(
        series: &[crate::wire::DecodedSeries],
        name: &str,
        le: Option<&str>,
    ) -> Option<f64> {
        series
            .iter()
            .find(|series| {
                series.labels.get("__name__") == Some(name) && series.labels.get("le") == le
            })
            .and_then(|series| series.samples.first().map(|sample| sample.value))
    }

    #[test]
    fn gauge_datapoint_decodes_to_float_series() {
        let data = metrics_data(Metric {
            name: "system.cpu.utilization".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![number_point(
                    0.42,
                    1_000_000,
                    vec![kv("host.name", "api-1")],
                )],
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        check!(
            series
                == vec![DecodedSeries {
                    labels: labels(&[
                        ("__name__", "system_cpu_utilization"),
                        ("host_name", "api-1")
                    ]),
                    samples: vec![DecodedSample {
                        timestamp_ms: 1,
                        value: 0.42,
                        start_timestamp_ms: None,
                    }],
                    histograms: Vec::new(),
                    exemplars: Vec::new(),
                    metadata: Some(DecodedMetadata {
                        metric_family_name: "system_cpu_utilization".into(),
                        metric_type: "gauge".into(),
                        help: String::new(),
                        unit: String::new(),
                    }),
                }]
        );
    }

    #[test]
    fn far_future_datapoint_is_rejected_not_clamped() {
        let data = metrics_data(Metric {
            name: "system.cpu.utilization".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![number_point(0.42, u64::MAX, Vec::new())],
            })),
            ..Default::default()
        });

        let err = decode_otlp(&data, TranslationStrategy::default()).unwrap_err();

        assert!(matches!(err, super::OtlpError::Invalid(_, _)));
        assert!(format!("{err}").contains("too far in the future"));
    }

    #[test]
    fn gauge_metric_decodes_metadata() {
        let data = metrics_data(Metric {
            name: "system.cpu.utilization".into(),
            description: "CPU utilization ratio.".into(),
            unit: "1".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![number_point(0.42, 1_000_000, Vec::new())],
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        let metadata = series[0].metadata.as_ref().expect("metric metadata");
        check!(
            *metadata
                == DecodedMetadata {
                    metric_family_name: "system_cpu_utilization".into(),
                    metric_type: "gauge".into(),
                    help: "CPU utilization ratio.".into(),
                    unit: "1".into(),
                }
        );
    }

    #[test]
    fn gauge_datapoint_drops_exemplars() {
        let data = metrics_data(Metric {
            name: "system.cpu.utilization".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    time_unix_nano: 2_000_000,
                    value: Some(number_data_point::Value::AsDouble(0.42)),
                    exemplars: vec![Exemplar {
                        filtered_attributes: vec![kv("user.id", "alice")],
                        time_unix_nano: 1_500_000,
                        value: Some(otlp_exemplar::Value::AsDouble(0.9)),
                        span_id: vec![0xab, 0xcd],
                        trace_id: vec![0x01, 0x23, 0x45, 0x67],
                    }],
                    ..Default::default()
                }],
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        assert!(series.len() == 1);
        assert!(series[0].exemplars.is_empty());
    }

    /// A monotonic sum keeps its exemplars; a non-monotonic one drops them,
    /// the rule `gauge_datapoint_drops_exemplars` already pins for gauges.
    /// Every other exemplar assertion in this file runs through a histogram
    /// path, so without a case that *keeps* one here, number datapoints could
    /// discard every exemplar they carry and the whole suite would still pass.
    #[test]
    fn only_a_monotonic_sum_keeps_its_number_datapoint_exemplars() {
        let sum_carrying_an_exemplar = |is_monotonic| {
            metrics_data(Metric {
                name: "http.server.requests".into(),
                data: Some(metric::Data::Sum(Sum {
                    data_points: vec![NumberDataPoint {
                        time_unix_nano: 3_000_000,
                        value: Some(number_data_point::Value::AsDouble(7.0)),
                        exemplars: vec![Exemplar {
                            filtered_attributes: vec![kv("user.id", "alice")],
                            time_unix_nano: 2_500_000,
                            value: Some(otlp_exemplar::Value::AsDouble(0.9)),
                            span_id: vec![0xab, 0xcd],
                            trace_id: vec![0x01, 0x23, 0x45, 0x67],
                        }],
                        ..Default::default()
                    }],
                    aggregation_temporality: AggregationTemporality::Cumulative as i32,
                    is_monotonic,
                })),
                ..Default::default()
            })
        };

        let kept = decode_otlp(
            &sum_carrying_an_exemplar(true),
            TranslationStrategy::default(),
        )
        .unwrap();

        assert!(kept.len() == 1);
        assert!(kept[0].exemplars.len() == 1);
        let mut labels = Labels::new();
        labels.insert("user_id", "alice");
        labels.insert("trace_id", "01234567");
        labels.insert("span_id", "abcd");
        check!(
            kept[0].exemplars[0]
                == DecodedExemplar {
                    labels,
                    timestamp_ms: 2,
                    value: 0.9,
                }
        );

        let dropped = decode_otlp(
            &sum_carrying_an_exemplar(false),
            TranslationStrategy::default(),
        )
        .unwrap();

        assert!(dropped.len() == 1);
        check!(dropped[0].exemplars.is_empty());
    }

    #[test]
    fn monotonic_sum_gets_total_suffix() {
        let data = metrics_data(Metric {
            name: "http.server.requests".into(),
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![number_point(7.0, 2_000_000, Vec::new())],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        assert!(series[0].labels.get("__name__") == Some("http_server_requests_total"));
        assert!(series[0].samples == vec![(2, 7.0)]);
    }

    #[test]
    fn default_translation_collapses_repeated_replacement_underscores() {
        let data = metrics_data(Metric {
            name: "http--server..requests".into(),
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![number_point(7.0, 2_000_000, Vec::new())],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        assert!(series[0].labels.get("__name__") == Some("http_server_requests_total"));
    }

    #[test]
    fn default_translation_adds_unit_suffix_before_total_suffix() {
        let data = metrics_data(Metric {
            name: "k8s.pod.cpu.time".into(),
            unit: "s".into(),
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![number_point(7.0, 2_000_000, Vec::new())],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        assert!(series[0].labels.get("__name__") == Some("k8s_pod_cpu_time_seconds_total"));
        assert!(series[0].metadata.as_ref().is_some_and(
            |metadata| metadata.metric_family_name == "k8s_pod_cpu_time_seconds_total"
        ));
    }

    #[test]
    fn default_translation_converts_rate_units_to_prometheus_suffixes() {
        let data = metrics_data(Metric {
            name: "network.io".into(),
            unit: "By/s".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![number_point(1024.0, 2_000_000, Vec::new())],
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        assert!(series[0].labels.get("__name__") == Some("network_io_bytes_per_second"));
    }

    #[test]
    fn default_translation_converts_meter_rate_unit_to_prometheus_suffix() {
        let data = metrics_data(Metric {
            name: "vehicle.speed".into(),
            unit: "m/s".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![number_point(12.5, 2_000_000, Vec::new())],
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        assert!(series[0].labels.get("__name__") == Some("vehicle_speed_meters_per_second"));
    }

    #[test]
    fn default_translation_drops_ucum_unit_annotations_before_suffix_conversion() {
        let data = metrics_data(Metric {
            name: "network.io".into(),
            unit: "By{packet}/s".into(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![number_point(1024.0, 2_000_000, Vec::new())],
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        assert!(series[0].labels.get("__name__") == Some("network_io_bytes_per_second"));
    }

    #[test]
    fn default_translation_converts_common_ucum_units_to_prometheus_suffixes() {
        for (metric_name, unit, expected_name) in [
            ("process.uptime", "min", "process_uptime_minutes"),
            ("process.uptime", "h", "process_uptime_hours"),
            ("process.uptime", "d", "process_uptime_days"),
            ("cache.size", "KiBy", "cache_size_kibibytes"),
            ("cache.size", "MiBy", "cache_size_mebibytes"),
            ("cache.size", "GiBy", "cache_size_gibibytes"),
            ("cache.size", "TiBy", "cache_size_tebibytes"),
            ("cache.size", "kBy", "cache_size_kilobytes"),
            ("cache.size", "MBy", "cache_size_megabytes"),
            ("cache.size", "GBy", "cache_size_gigabytes"),
            ("cache.size", "TBy", "cache_size_terabytes"),
            ("sensor.reading", "V", "sensor_reading_volts"),
            ("sensor.reading", "A", "sensor_reading_amperes"),
            ("sensor.reading", "J", "sensor_reading_joules"),
            ("sensor.reading", "W", "sensor_reading_watts"),
            ("sensor.reading", "g", "sensor_reading_grams"),
            ("cache.write", "MiBy/s", "cache_write_mebibytes_per_second"),
        ] {
            let data = metrics_data(Metric {
                name: metric_name.into(),
                unit: unit.into(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![number_point(1.0, 2_000_000, Vec::new())],
                })),
                ..Default::default()
            });

            let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

            assert!(series[0].labels.get("__name__") == Some(expected_name));
        }
    }

    #[test]
    fn delta_sum_accumulates_to_cumulative_samples() {
        let first = metrics_data(Metric {
            name: "http.server.requests".into(),
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![number_point(7.0, 2_000_000, vec![kv("route", "/v1")])],
                aggregation_temporality: AggregationTemporality::Delta as i32,
                is_monotonic: true,
            })),
            ..Default::default()
        });
        let second = metrics_data(Metric {
            name: "http.server.requests".into(),
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![number_point(5.0, 3_000_000, vec![kv("route", "/v1")])],
                aggregation_temporality: AggregationTemporality::Delta as i32,
                is_monotonic: true,
            })),
            ..Default::default()
        });
        let mut accumulator = DeltaAccumulator::default();

        let first_series =
            decode_otlp_stateful(&first, TranslationStrategy::default(), &mut accumulator).unwrap();
        let second_series =
            decode_otlp_stateful(&second, TranslationStrategy::default(), &mut accumulator)
                .unwrap();

        assert!(first_series[0].samples == vec![(2, 7.0)]);
        assert!(second_series[0].samples == vec![(3, 12.0)]);
    }

    #[test]
    fn stateless_decode_accumulates_delta_sums_within_one_payload() {
        let data = metrics_data(Metric {
            name: "http.server.requests".into(),
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![
                    NumberDataPoint {
                        attributes: vec![kv("route", "/v1")],
                        start_time_unix_nano: 1_000_000,
                        time_unix_nano: 2_000_000,
                        value: Some(number_data_point::Value::AsDouble(7.0)),
                        ..Default::default()
                    },
                    NumberDataPoint {
                        attributes: vec![kv("route", "/v1")],
                        start_time_unix_nano: 1_000_000,
                        time_unix_nano: 3_000_000,
                        value: Some(number_data_point::Value::AsDouble(5.0)),
                        ..Default::default()
                    },
                ],
                aggregation_temporality: AggregationTemporality::Delta as i32,
                is_monotonic: true,
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        let expected_labels =
            labels(&[("__name__", "http_server_requests_total"), ("route", "/v1")]);
        let expected_metadata = Some(DecodedMetadata {
            metric_family_name: "http_server_requests_total".into(),
            metric_type: "counter".into(),
            help: String::new(),
            unit: String::new(),
        });
        check!(
            series
                == vec![
                    DecodedSeries {
                        labels: expected_labels.clone(),
                        samples: vec![DecodedSample {
                            timestamp_ms: 2,
                            value: 7.0,
                            start_timestamp_ms: Some(1),
                        }],
                        histograms: Vec::new(),
                        exemplars: Vec::new(),
                        metadata: expected_metadata.clone(),
                    },
                    DecodedSeries {
                        labels: expected_labels,
                        samples: vec![DecodedSample {
                            timestamp_ms: 3,
                            value: 12.0,
                            start_timestamp_ms: Some(1),
                        }],
                        histograms: Vec::new(),
                        exemplars: Vec::new(),
                        metadata: expected_metadata,
                    },
                ]
        );
    }

    #[test]
    fn histogram_decodes_exemplar_to_matching_bucket_series() {
        let data = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    time_unix_nano: 2_000_000,
                    count: 3,
                    sum: Some(1.7),
                    bucket_counts: vec![1, 1, 1],
                    explicit_bounds: vec![0.5, 1.0],
                    exemplars: vec![Exemplar {
                        filtered_attributes: vec![kv("http.route", "/v1")],
                        time_unix_nano: 1_500_000,
                        value: Some(otlp_exemplar::Value::AsDouble(0.9)),
                        span_id: vec![0xab, 0xcd],
                        trace_id: vec![0x01, 0x23, 0x45, 0x67],
                    }],
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        let matching_bucket = series
            .iter()
            .find(|series| {
                series.labels.get("__name__") == Some("rpc_server_duration_bucket")
                    && series.labels.get("le") == Some("1")
            })
            .expect("matching bucket series");
        assert!(matching_bucket.exemplars.len() == 1);
        let exemplar = &matching_bucket.exemplars[0];
        check!(exemplar.timestamp_ms == 1);
        check!((exemplar.value - 0.9).abs() < f64::EPSILON);
        check!(exemplar.labels.get("trace_id") == Some("01234567"));
        check!(exemplar.labels.get("span_id") == Some("abcd"));
        check!(exemplar.labels.get("http_route") == Some("/v1"));

        for series in &series {
            if series.labels.get("le") != Some("1") {
                assert!(series.exemplars.is_empty());
            }
        }
    }

    #[test]
    fn delta_histogram_accumulates_to_cumulative_classic_series() {
        let first = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    attributes: vec![kv("route", "/v1")],
                    time_unix_nano: 2_000_000,
                    start_time_unix_nano: 1_000_000,
                    count: 5,
                    sum: Some(7.0),
                    bucket_counts: vec![1, 4],
                    explicit_bounds: vec![0.5],
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            })),
            ..Default::default()
        });
        let second = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    attributes: vec![kv("route", "/v1")],
                    time_unix_nano: 3_000_000,
                    start_time_unix_nano: 1_000_000,
                    count: 3,
                    sum: Some(4.0),
                    bucket_counts: vec![2, 1],
                    explicit_bounds: vec![0.5],
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            })),
            ..Default::default()
        });
        let mut accumulator = DeltaAccumulator::default();

        let first_series =
            decode_otlp_stateful(&first, TranslationStrategy::default(), &mut accumulator).unwrap();
        let second_series =
            decode_otlp_stateful(&second, TranslationStrategy::default(), &mut accumulator)
                .unwrap();

        let cases = [
            (
                "first payload",
                &first_series,
                "rpc_server_duration_bucket",
                Some("0.5"),
                1.0,
            ),
            (
                "first payload",
                &first_series,
                "rpc_server_duration_bucket",
                Some("+Inf"),
                5.0,
            ),
            (
                "first payload",
                &first_series,
                "rpc_server_duration_count",
                None,
                5.0,
            ),
            (
                "first payload",
                &first_series,
                "rpc_server_duration_sum",
                None,
                7.0,
            ),
            (
                "second payload",
                &second_series,
                "rpc_server_duration_bucket",
                Some("0.5"),
                3.0,
            ),
            (
                "second payload",
                &second_series,
                "rpc_server_duration_bucket",
                Some("+Inf"),
                8.0,
            ),
            (
                "second payload",
                &second_series,
                "rpc_server_duration_count",
                None,
                8.0,
            ),
            (
                "second payload",
                &second_series,
                "rpc_server_duration_sum",
                None,
                11.0,
            ),
        ];
        for (case, series, name, le, expected) in cases {
            assert!(
                sample_value(series, name, le) == Some(expected),
                "case: {case} name={name} le={le:?}"
            );
        }
    }

    #[test]
    fn resource_attributes_emit_target_info_series() {
        let data = metrics_data_with_resource(
            Metric {
                name: "system.cpu.utilization".into(),
                data: Some(metric::Data::Gauge(Gauge {
                    data_points: vec![number_point(
                        0.42,
                        1_000_000,
                        vec![kv("host.name", "api-1")],
                    )],
                })),
                ..Default::default()
            },
            vec![
                kv("service.name", "checkout"),
                kv("telemetry.sdk.language", "rust"),
            ],
        );

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        let target = series
            .iter()
            .find(|series| series.labels.get("__name__") == Some("target_info"))
            .expect("target_info series");
        assert!(
            target.labels
                == labels(&[
                    ("__name__", "target_info"),
                    ("service_name", "checkout"),
                    ("telemetry_sdk_language", "rust")
                ])
        );
        assert!(target.samples == vec![(1, 1.0)]);
    }

    #[test]
    fn scope_metadata_is_added_to_metric_series_labels() {
        let data = MetricsData {
            resource_metrics: vec![ResourceMetrics {
                resource: Some(Resource {
                    attributes: vec![kv("service.name", "checkout")],
                    dropped_attributes_count: 0,
                    entity_refs: Vec::new(),
                }),
                scope_metrics: vec![ScopeMetrics {
                    scope: Some(InstrumentationScope {
                        name: "io.opentelemetry.http".into(),
                        version: "1.2.3".into(),
                        attributes: vec![kv("library.language", "rust")],
                        dropped_attributes_count: 0,
                    }),
                    metrics: vec![Metric {
                        name: "http.server.active_requests".into(),
                        data: Some(metric::Data::Gauge(Gauge {
                            data_points: vec![number_point(3.0, 2_000_000, Vec::new())],
                        })),
                        ..Default::default()
                    }],
                    schema_url: "https://opentelemetry.io/schemas/1.24.0".into(),
                }],
                schema_url: String::new(),
            }],
        };

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        let metric = series
            .iter()
            .find(|series| series.labels.get("__name__") == Some("http_server_active_requests"))
            .expect("metric series");
        check!(
            metric.labels
                == labels(&[
                    ("__name__", "http_server_active_requests"),
                    ("otel_scope_library_language", "rust"),
                    ("otel_scope_name", "io.opentelemetry.http"),
                    (
                        "otel_scope_schema_url",
                        "https://opentelemetry.io/schemas/1.24.0"
                    ),
                    ("otel_scope_version", "1.2.3"),
                    ("service_name", "checkout"),
                ])
        );

        let target = series
            .iter()
            .find(|series| series.labels.get("__name__") == Some("target_info"))
            .expect("target_info series");
        assert!(target.labels.get("otel_scope_name").is_none());
    }

    #[test]
    fn summary_decodes_to_quantile_sum_and_count_series() {
        let data = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::Summary(Summary {
                data_points: vec![SummaryDataPoint {
                    attributes: vec![kv("route", "/v1")],
                    time_unix_nano: 4_000_000,
                    count: 9,
                    sum: 12.5,
                    quantile_values: vec![
                        summary_data_point::ValueAtQuantile {
                            quantile: 0.5,
                            value: 2.0,
                        },
                        summary_data_point::ValueAtQuantile {
                            quantile: 0.9,
                            value: 4.0,
                        },
                    ],
                    ..Default::default()
                }],
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        let expected_metadata = Some(DecodedMetadata {
            metric_family_name: "rpc_server_duration".into(),
            metric_type: "summary".into(),
            help: String::new(),
            unit: String::new(),
        });
        check!(
            series
                == vec![
                    DecodedSeries {
                        labels: labels(&[
                            ("__name__", "rpc_server_duration"),
                            ("quantile", "0.5"),
                            ("route", "/v1")
                        ]),
                        samples: vec![DecodedSample {
                            timestamp_ms: 4,
                            value: 2.0,
                            start_timestamp_ms: None,
                        }],
                        histograms: Vec::new(),
                        exemplars: Vec::new(),
                        metadata: expected_metadata.clone(),
                    },
                    DecodedSeries {
                        labels: labels(&[
                            ("__name__", "rpc_server_duration"),
                            ("quantile", "0.9"),
                            ("route", "/v1")
                        ]),
                        samples: vec![DecodedSample {
                            timestamp_ms: 4,
                            value: 4.0,
                            start_timestamp_ms: None,
                        }],
                        histograms: Vec::new(),
                        exemplars: Vec::new(),
                        metadata: expected_metadata.clone(),
                    },
                    DecodedSeries {
                        labels: labels(&[
                            ("__name__", "rpc_server_duration_count"),
                            ("route", "/v1")
                        ]),
                        samples: vec![DecodedSample {
                            timestamp_ms: 4,
                            value: 9.0,
                            start_timestamp_ms: None,
                        }],
                        histograms: Vec::new(),
                        exemplars: Vec::new(),
                        metadata: expected_metadata.clone(),
                    },
                    DecodedSeries {
                        labels: labels(&[
                            ("__name__", "rpc_server_duration_sum"),
                            ("route", "/v1")
                        ]),
                        samples: vec![DecodedSample {
                            timestamp_ms: 4,
                            value: 12.5,
                            start_timestamp_ms: None,
                        }],
                        histograms: Vec::new(),
                        exemplars: Vec::new(),
                        metadata: expected_metadata,
                    },
                ]
        );
    }

    #[test]
    fn exponential_histogram_decodes_to_native_histogram() {
        let data = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 3_000_000,
                    start_time_unix_nano: 1_000_000,
                    count: 6,
                    sum: Some(12.0),
                    scale: 3,
                    zero_count: 1,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: -1,
                        bucket_counts: vec![2, 3],
                    }),
                    negative: Some(exponential_histogram_data_point::Buckets {
                        offset: 4,
                        bucket_counts: vec![1],
                    }),
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        check!(series.len() == 1);
        check!(series[0].labels.get("__name__") == Some("rpc_server_duration"));
        check!(series[0].histograms.len() == 1);
        let (timestamp_ms, hist) = &series[0].histograms[0];
        check!(*timestamp_ms == 3);
        check!(hist.schema == 3);
        check!((hist.count - 6.0).abs() < f64::EPSILON);
        check!((hist.sum - 12.0).abs() < f64::EPSILON);
        check!((hist.zero_count - 1.0).abs() < f64::EPSILON);
        check!(hist.positive_spans[0].offset == 0);
        check!(hist.positive_counts == vec![2.0, 3.0]);
        check!(hist.negative_spans[0].offset == 5);
        check!(hist.negative_counts == vec![1.0]);
        check!(hist.start_timestamp_ms == Some(1));
    }

    #[test]
    fn exponential_histogram_decodes_exemplar_trace_context_and_filtered_attributes() {
        let data = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 3_000_000,
                    count: 2,
                    sum: Some(5.0),
                    scale: 1,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: 0,
                        bucket_counts: vec![2],
                    }),
                    exemplars: vec![Exemplar {
                        filtered_attributes: vec![kv("span.kind", "server")],
                        time_unix_nano: 2_500_000,
                        value: Some(otlp_exemplar::Value::AsDouble(2.5)),
                        span_id: vec![0xab, 0xcd],
                        trace_id: vec![0x01, 0x23, 0x45, 0x67],
                    }],
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();

        assert!(series.len() == 1);
        assert!(series[0].exemplars.len() == 1);
        let exemplar = &series[0].exemplars[0];
        check!(exemplar.timestamp_ms == 2);
        check!((exemplar.value - 2.5).abs() < f64::EPSILON);
        check!(exemplar.labels.get("trace_id") == Some("01234567"));
        check!(exemplar.labels.get("span_id") == Some("abcd"));
        check!(exemplar.labels.get("span_kind") == Some("server"));
    }

    /// Both histogram flavours report `"histogram"` as their metadata type.
    /// Gauge, counter and summary each have a test pinning the whole metadata
    /// struct; histogram had none, so the type string could read "gauge" on
    /// every bucket series of both decoders and nothing would notice.
    #[test]
    fn both_histogram_flavours_report_the_histogram_metadata_type() {
        let classic = metric::Data::Histogram(Histogram {
            data_points: vec![HistogramDataPoint {
                time_unix_nano: 2_000_000,
                count: 1,
                sum: Some(0.5),
                bucket_counts: vec![1, 0],
                explicit_bounds: vec![1.0],
                ..Default::default()
            }],
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
        });
        let exponential = metric::Data::ExponentialHistogram(ExponentialHistogram {
            data_points: vec![ExponentialHistogramDataPoint {
                time_unix_nano: 2_000_000,
                count: 1,
                sum: Some(0.5),
                scale: 0,
                positive: Some(exponential_histogram_data_point::Buckets {
                    offset: 0,
                    bucket_counts: vec![1],
                }),
                ..Default::default()
            }],
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
        });

        for data in [classic, exponential] {
            let series = decode_otlp(
                &metrics_data(Metric {
                    name: "rpc.server.duration".into(),
                    description: "Server call latency.".into(),
                    unit: "s".into(),
                    data: Some(data),
                    ..Default::default()
                }),
                TranslationStrategy::default(),
            )
            .unwrap();

            assert!(!series.is_empty());
            for one in &series {
                check!(
                    one.metadata
                        == Some(DecodedMetadata {
                            metric_family_name: "rpc_server_duration_seconds".into(),
                            metric_type: "histogram".into(),
                            help: "Server call latency.".into(),
                            unit: "s".into(),
                        })
                );
            }
        }
    }

    #[test]
    fn delta_exponential_histogram_accumulates_to_cumulative_native_histogram() {
        let first = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 2_000_000,
                    start_time_unix_nano: 1_000_000,
                    count: 4,
                    sum: Some(6.0),
                    scale: 1,
                    zero_count: 1,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: 0,
                        bucket_counts: vec![2, 1],
                    }),
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            })),
            ..Default::default()
        });
        let second = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 3_000_000,
                    start_time_unix_nano: 1_000_000,
                    count: 3,
                    sum: Some(5.0),
                    scale: 1,
                    zero_count: 2,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: 0,
                        bucket_counts: vec![1, 2],
                    }),
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            })),
            ..Default::default()
        });
        let mut accumulator = DeltaAccumulator::default();

        let first_series =
            decode_otlp_stateful(&first, TranslationStrategy::default(), &mut accumulator).unwrap();
        let second_series =
            decode_otlp_stateful(&second, TranslationStrategy::default(), &mut accumulator)
                .unwrap();

        let first_hist = &first_series[0].histograms[0].1;
        check!((first_hist.count - 4.0).abs() < f64::EPSILON);
        check!((first_hist.sum - 6.0).abs() < f64::EPSILON);
        check!((first_hist.zero_count - 1.0).abs() < f64::EPSILON);
        check!(first_hist.positive_counts == vec![2.0, 1.0]);

        let second_hist = &second_series[0].histograms[0].1;
        check!((second_hist.count - 7.0).abs() < f64::EPSILON);
        check!((second_hist.sum - 11.0).abs() < f64::EPSILON);
        check!((second_hist.zero_count - 3.0).abs() < f64::EPSILON);
        check!(second_hist.positive_counts == vec![3.0, 3.0]);
        check!(second_hist.start_timestamp_ms == Some(1));
    }

    #[test]
    fn delta_exponential_histogram_accumulates_different_span_layouts() {
        let first = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 2_000_000,
                    start_time_unix_nano: 1_000_000,
                    count: 2,
                    sum: Some(3.0),
                    scale: 1,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: 0,
                        bucket_counts: vec![2],
                    }),
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            })),
            ..Default::default()
        });
        let second = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 3_000_000,
                    start_time_unix_nano: 1_000_000,
                    count: 3,
                    sum: Some(5.0),
                    scale: 1,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: 1,
                        bucket_counts: vec![3],
                    }),
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Delta as i32,
            })),
            ..Default::default()
        });
        let mut accumulator = DeltaAccumulator::default();

        decode_otlp_stateful(&first, TranslationStrategy::default(), &mut accumulator).unwrap();
        let second_series =
            decode_otlp_stateful(&second, TranslationStrategy::default(), &mut accumulator)
                .unwrap();

        let second_hist = &second_series[0].histograms[0].1;
        check!((second_hist.count - 5.0).abs() < f64::EPSILON);
        check!((second_hist.sum - 8.0).abs() < f64::EPSILON);
        check!(
            second_hist.positive_spans
                == vec![BucketSpan {
                    offset: 1,
                    length: 2
                }]
        );
        check!(second_hist.positive_counts == vec![2.0, 3.0]);
    }

    #[test]
    fn exponential_histogram_shifts_otlp_lower_boundary_indexes_to_native_upper_boundary_indexes() {
        let data = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    count: 2,
                    scale: 2,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: 0,
                        bucket_counts: vec![1],
                    }),
                    negative: Some(exponential_histogram_data_point::Buckets {
                        offset: 3,
                        bucket_counts: vec![1],
                    }),
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        });

        let series = decode_otlp(&data, TranslationStrategy::default()).unwrap();
        let hist = &series[0].histograms[0].1;

        assert!(hist.positive_spans[0].offset == 1);
        assert!(hist.negative_spans[0].offset == 4);
    }

    #[test]
    fn exponential_histogram_rejects_scale_below_native_schema_range() {
        let data = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    count: 1,
                    scale: -5,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: 0,
                        bucket_counts: vec![1],
                    }),
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        });

        let err = decode_otlp(&data, TranslationStrategy::default()).unwrap_err();

        assert!(matches!(err, super::OtlpError::Invalid(_, _)));
        assert!(format!("{err}").contains("scale -5"));
    }

    #[test]
    fn exponential_histogram_rejects_unrepresentable_downscale() {
        let data = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    count: 1,
                    scale: 40,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: 0,
                        bucket_counts: vec![1],
                    }),
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        });

        let err = decode_otlp(&data, TranslationStrategy::default()).unwrap_err();

        assert!(matches!(err, super::OtlpError::Invalid(_, _)));
        assert!(format!("{err}").contains("scale 40"));
    }

    #[test]
    fn exponential_histogram_rejects_lossy_downscale() {
        let data = metrics_data(Metric {
            name: "rpc.server.duration".into(),
            data: Some(metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    count: 3,
                    scale: 9,
                    positive: Some(exponential_histogram_data_point::Buckets {
                        offset: 0,
                        bucket_counts: vec![1, 2],
                    }),
                    ..Default::default()
                }],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
            })),
            ..Default::default()
        });

        let err = decode_otlp(&data, TranslationStrategy::default()).unwrap_err();

        assert!(matches!(err, super::OtlpError::Invalid(_, _)));
        assert!(format!("{err}").contains("lossy downscale"));
    }
}

// === split-modules: generated submodules ===
mod accumulate_delta_float_series;
mod add_compatible_native_histogram;
mod add_spanned_histogram_counts;
mod attribute_value;
mod bytes_to_hex;
mod classic_histogram_series;
mod compact_spanned_histogram_counts;
mod decode_otlp;
mod decode_otlp_bytes;
mod decode_otlp_inner;
mod decode_otlp_stateful;
mod decode_otlp_stateful_bytes;
mod delta_accumulator;
mod delta_histogram_state;
mod delta_key;
mod delta_state;
mod delta_sum_series;
mod downscaled_spans;
mod exemplar;
mod exemplar_belongs_to_bucket;
mod exemplar_policy;
mod exemplars_for_bucket;
mod exemplars_from_exponential_histogram_point;
mod exemplars_from_histogram_point;
mod exemplars_from_number_point;
mod exemplars_from_otlp;
mod exponential_histogram_series;
mod exponential_histogram_to_native;
mod gauge_series;
mod histogram_series;
mod insert_attributes;
mod instrumentation_scope_attributes;
mod labels;
mod max_native_histogram_schema;
mod max_sample_timestamp_ms;
mod metric_attributes;
mod metric_metadata;
mod metric_series;
mod min_native_histogram_schema;
mod nanos_to_millis;
mod normalize_name;
mod number_value;
mod otlp_error;
mod prometheus_base_unit_suffix;
mod prometheus_unit_suffix;
mod reject_far_future_points;
mod resource_metrics_timestamp_ms;
mod scalar_series;
mod scope_attributes;
mod spanned_histogram_counts;
mod string_attribute;
mod strip_ucum_annotations;
mod sum_metadata_type;
mod sum_series;
mod summary_point_series;
mod summary_series;
mod translated_metric_name;
mod translation_strategy;

use accumulate_delta_float_series::accumulate_delta_float_series;
use add_compatible_native_histogram::add_compatible_native_histogram;
use add_spanned_histogram_counts::add_spanned_histogram_counts;
use attribute_value::attribute_value;
use bytes_to_hex::bytes_to_hex;
use classic_histogram_series::classic_histogram_series;
use compact_spanned_histogram_counts::compact_spanned_histogram_counts;
pub use decode_otlp::decode_otlp;
pub use decode_otlp_bytes::decode_otlp_bytes;
use decode_otlp_inner::decode_otlp_inner;
pub use decode_otlp_stateful::decode_otlp_stateful;
pub use decode_otlp_stateful_bytes::decode_otlp_stateful_bytes;
pub use delta_accumulator::DeltaAccumulator;
use delta_histogram_state::DeltaHistogramState;
use delta_key::DeltaKey;
use delta_key::delta_key;
use delta_state::DeltaState;
use delta_sum_series::delta_sum_series;
use downscaled_spans::downscaled_spans;
use exemplar::exemplar;
use exemplar_belongs_to_bucket::exemplar_belongs_to_bucket;
use exemplar_policy::ExemplarPolicy;
use exemplars_for_bucket::exemplars_for_bucket;
use exemplars_from_exponential_histogram_point::exemplars_from_exponential_histogram_point;
use exemplars_from_histogram_point::exemplars_from_histogram_point;
use exemplars_from_number_point::exemplars_from_number_point;
use exemplars_from_otlp::exemplars_from_otlp;
use exponential_histogram_series::exponential_histogram_series;
pub use exponential_histogram_to_native::exponential_histogram_to_native;
use gauge_series::gauge_series;
use histogram_series::histogram_series;
use insert_attributes::insert_attributes;
use instrumentation_scope_attributes::instrumentation_scope_attributes;
use labels::labels;
use max_native_histogram_schema::MAX_NATIVE_HISTOGRAM_SCHEMA;
pub (crate) use max_sample_timestamp_ms::MAX_SAMPLE_TIMESTAMP_MS;
use metric_attributes::metric_attributes;
use metric_metadata::metric_metadata;
use metric_series::metric_series;
use min_native_histogram_schema::MIN_NATIVE_HISTOGRAM_SCHEMA;
use nanos_to_millis::nanos_to_millis;
pub use normalize_name::normalize_name;
use number_value::number_value;
pub use otlp_error::OtlpError;
use prometheus_base_unit_suffix::prometheus_base_unit_suffix;
use prometheus_unit_suffix::prometheus_unit_suffix;
use reject_far_future_points::reject_far_future_points;
use resource_metrics_timestamp_ms::resource_metrics_timestamp_ms;
use scalar_series::scalar_series;
use scope_attributes::scope_attributes;
use spanned_histogram_counts::spanned_histogram_counts;
use string_attribute::string_attribute;
use strip_ucum_annotations::strip_ucum_annotations;
use sum_metadata_type::sum_metadata_type;
use sum_series::sum_series;
use summary_point_series::summary_point_series;
use summary_series::summary_series;
use translated_metric_name::translated_metric_name;
pub use translation_strategy::TranslationStrategy;
