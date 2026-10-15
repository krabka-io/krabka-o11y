use std::collections::{BTreeMap, BTreeSet};

use krabka_metrics::{BucketSpan, NativeHistogram, ResetHint};

use super::{
    annotations::{
        bad_bucket_label_warning, emit_histogram_quantile_forced_monotonicity_info, emit_info,
        emit_warning, invalid_quantile_warning, is_valid_quantile,
        native_histogram_fraction_nans_info, native_histogram_quantile_nan_result_info,
        native_histogram_quantile_nan_skew_info, warn_mixed_histograms,
    },
    labels::{
        float_sample_value, labels_key, labels_without_label, labels_without_metric_name,
        record_metric_name,
    },
};
use crate::{
    PromqlLabels as Labels,
    error::{PromqlError, Result},
    result::{InstantSample, SampleValue},
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::native_histogram_fixture;

    fn histogram(schema: i8, reset_hint: ResetHint) -> NativeHistogram {
        native_histogram_fixture(
            schema,
            true,
            reset_hint,
            (schema == -53).then(Vec::new),
            None,
        )
    }

    /// Two histograms are range-compatible only when *every* shape field
    /// agrees. Each case below differs in exactly one of them, and each is on
    /// its own enough to make the pair incompatible.
    #[test]
    fn range_compatibility_needs_every_shape_field_to_agree() {
        struct Difference {
            field: &'static str,
            change: fn(&mut NativeHistogram),
        }
        let span = |offset, length| BucketSpan { offset, length };
        let base = || {
            let mut histogram = histogram(0, ResetHint::Unknown);
            histogram.positive_spans = vec![span(1, 2)];
            histogram.positive_counts = vec![1.0, 2.0];
            histogram.negative_spans = vec![span(0, 1)];
            histogram.negative_counts = vec![3.0];
            histogram
        };
        assert2::check!(native_histograms_are_range_compatible(&base(), &base()));

        let differing: [Difference; 8] = [
            Difference {
                field: "schema",
                change: |h| h.schema = 1,
            },
            Difference {
                field: "is_float",
                change: |h| h.is_float = !h.is_float,
            },
            Difference {
                field: "zero_threshold",
                change: |h| h.zero_threshold = 1.0,
            },
            Difference {
                field: "custom_values",
                change: |h| h.custom_values = Some(vec![1.0]),
            },
            Difference {
                field: "positive_spans",
                change: |h| {
                    h.positive_spans = vec![BucketSpan {
                        offset: 2,
                        length: 2,
                    }];
                },
            },
            Difference {
                field: "negative_spans",
                change: |h| {
                    h.negative_spans = vec![BucketSpan {
                        offset: 1,
                        length: 1,
                    }];
                },
            },
            Difference {
                field: "positive_counts length",
                change: |h| h.positive_counts.push(9.0),
            },
            Difference {
                field: "negative_counts length",
                change: |h| h.negative_counts.push(9.0),
            },
        ];
        for difference in differing {
            let mut right = base();
            (difference.change)(&mut right);
            assert2::check!(
                !native_histograms_are_range_compatible(&base(), &right),
                "{} alone must make the pair incompatible",
                difference.field
            );
        }
    }

    /// `DetectReset` compares bucket populations BY INDEX, over both
    /// histograms resolved to the current one's schema and zero threshold --
    /// what `floatBucketIterator` hands `detectReset` upstream. A positional
    /// walk of the two count vectors instead compares whichever buckets happen
    /// to share an offset in the vector, which asks the same question only
    /// when the two span layouts already agree.
    #[test]
    fn detect_reset_aligns_buckets_by_index_not_by_position() {
        struct ResetCase {
            name: &'static str,
            previous: NativeHistogram,
            current: NativeHistogram,
            expected_reset: bool,
        }
        macro_rules! reset_case {
            ($name:expr, $previous:expr, $current:expr, $expected_reset:expr $(,)?) => {
                ResetCase {
                    name: $name,
                    previous: $previous,
                    current: $current,
                    expected_reset: $expected_reset,
                }
            };
        }
        let span = |offset, length| BucketSpan { offset, length };
        let positive = |schema, count, spans: Vec<BucketSpan>, counts: Vec<f64>| {
            let mut hist = histogram(schema, ResetHint::Unknown);
            hist.count = count;
            hist.positive_spans = spans;
            hist.positive_counts = counts;
            hist
        };
        let negative = |count, spans: Vec<BucketSpan>, counts: Vec<f64>| {
            let mut hist = histogram(0, ResetHint::Unknown);
            hist.count = count;
            hist.negative_spans = spans;
            hist.negative_counts = counts;
            hist
        };
        let zero_region = |threshold: f64, zero_count: f64, counts: Vec<f64>| {
            let mut hist = histogram(0, ResetHint::Unknown);
            hist.count = 100.0;
            hist.zero_threshold = threshold;
            hist.zero_count = zero_count;
            hist.positive_spans = if counts.is_empty() {
                Vec::new()
            } else {
                vec![span(0, u32::try_from(counts.len()).unwrap())]
            };
            hist.positive_counts = counts;
            hist
        };

        let cases = [
            // The count of 5 moved from bucket 0 to bucket 1. A positional walk
            // compares [5] against [5] and sees nothing.
            reset_case!(
                "a count that moved to the next bucket",
                positive(0, 5.0, vec![span(0, 1)], vec![5.0]),
                positive(0, 5.0, vec![span(1, 1)], vec![5.0]),
                true,
            ),
            // Buckets 0 and 2, spelled once as two spans and once as one span
            // with a hole in it. Nothing moved.
            reset_case!(
                "the same buckets described by different spans",
                positive(0, 3.0, vec![span(0, 1), span(1, 1)], vec![1.0, 2.0]),
                positive(0, 3.0, vec![span(0, 3)], vec![1.0, 0.0, 2.0]),
                false,
            ),
            // Bucket 2 fell from 2 to 1, but it is the third count in the
            // current vector and the second in the previous one.
            reset_case!(
                "a bucket that shrank at an index the vectors do not share",
                positive(0, 3.0, vec![span(0, 1), span(1, 1)], vec![1.0, 2.0]),
                positive(0, 7.0, vec![span(0, 3)], vec![1.0, 5.0, 1.0]),
                true,
            ),
            reset_case!(
                "a populated bucket the current histogram no longer carries",
                positive(0, 3.0, vec![span(0, 2)], vec![1.0, 2.0]),
                positive(0, 3.0, vec![span(0, 1)], vec![1.0]),
                true,
            ),
            reset_case!(
                "an empty bucket the current histogram no longer carries",
                positive(0, 1.0, vec![span(0, 2)], vec![1.0, 0.0]),
                positive(0, 1.0, vec![span(0, 1)], vec![1.0]),
                false,
            ),
            // Schema 1 buckets 1 and 2 are both halves of schema 0 bucket 1, so
            // they merge to 7 before the comparison.
            reset_case!(
                "a finer previous schema merged short of the coarser current one",
                positive(1, 7.0, vec![span(1, 2)], vec![3.0, 4.0]),
                positive(0, 10.0, vec![span(1, 1)], vec![6.0]),
                true,
            ),
            reset_case!(
                "a finer previous schema merged level with the coarser current one",
                positive(1, 7.0, vec![span(1, 2)], vec![3.0, 4.0]),
                positive(0, 10.0, vec![span(1, 1)], vec![7.0]),
                false,
            ),
            reset_case!(
                "a negative count that moved to the next bucket",
                negative(5.0, vec![span(0, 1)], vec![5.0]),
                negative(5.0, vec![span(1, 1)], vec![5.0]),
                true,
            ),
            // Widening the zero threshold to 1 folds the previous histogram's
            // bucket 0, which spans (0.5, 1], into its zero count: 3 + 5 = 8.
            reset_case!(
                "a zero count short of the buckets the wider threshold swallowed",
                zero_region(0.0, 3.0, vec![5.0]),
                zero_region(1.0, 3.0, Vec::new()),
                true,
            ),
            reset_case!(
                "a zero count level with the buckets the wider threshold swallowed",
                zero_region(0.0, 3.0, vec![5.0]),
                zero_region(1.0, 8.0, Vec::new()),
                false,
            ),
        ];

        for case in cases {
            assert2::check!(
                native_histogram_detect_reset(&case.previous, &case.current) == case.expected_reset,
                "{}",
                case.name
            );
        }
    }

    #[test]
    fn add_downscales_exponential_buckets() {
        let mut left = histogram(1, ResetHint::No);
        left.positive_spans = vec![BucketSpan {
            offset: -1,
            length: 4,
        }];
        left.positive_counts = vec![1.0, 2.0, 4.0, 8.0];
        let mut right = histogram(0, ResetHint::No);
        right.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 2,
        }];
        right.positive_counts = vec![10.0, 20.0];

        add_compatible_native_histogram(&mut left, &right).unwrap();

        assert2::assert!(left.schema == 0);
        assert2::assert!(
            left.positive_spans
                == vec![BucketSpan {
                    offset: 0,
                    length: 2
                }]
        );
        assert2::assert!(left.positive_counts == vec![13.0, 32.0]);
    }

    #[test]
    fn add_expands_zero_bucket_to_populated_bucket_boundary() {
        let mut left = histogram(0, ResetHint::No);
        left.zero_count = 1.0;
        left.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 1,
        }];
        left.positive_counts = vec![2.0];
        let mut right = histogram(0, ResetHint::No);
        right.zero_threshold = 0.75;
        right.zero_count = 3.0;

        add_compatible_native_histogram(&mut left, &right).unwrap();

        assert2::assert!((left.zero_threshold - 1.0).abs() < f64::EPSILON);
        assert2::assert!((left.zero_count - 6.0).abs() < f64::EPSILON);
        assert2::assert!(left.positive_spans.is_empty());
        assert2::assert!(left.positive_counts.is_empty());
    }

    /// A bucket whose lower bound is *exactly* the zero threshold sits outside
    /// the zero region, so `>= threshold` stops there and the bucket survives
    /// the merge intact. Every other zero-bucket test puts the bucket strictly
    /// below the threshold, where `>` and `>=` agree and it is folded in
    /// either way -- which also leaves the matching keep test in
    /// `reduced_counts_outside_zero` free to drop the boundary bucket.
    #[test]
    fn add_keeps_a_bucket_whose_lower_bound_is_exactly_the_zero_threshold() {
        let mut left = histogram(0, ResetHint::No);
        left.zero_threshold = 2.0;
        left.zero_count = 1.0;
        left.positive_spans = vec![BucketSpan {
            offset: 2,
            length: 1,
        }];
        left.positive_counts = vec![5.0];
        let mut right = histogram(0, ResetHint::No);
        right.zero_threshold = 2.0;
        right.zero_count = 3.0;

        add_compatible_native_histogram(&mut left, &right).unwrap();

        assert2::assert!((left.zero_threshold - 2.0).abs() < f64::EPSILON);
        assert2::assert!((left.zero_count - 4.0).abs() < f64::EPSILON);
        assert2::assert!(
            left.positive_spans
                == vec![BucketSpan {
                    offset: 2,
                    length: 1
                }]
        );
        assert2::assert!(left.positive_counts == vec![5.0]);
    }

    /// An *empty* bucket below the zero threshold must not widen the zero
    /// region: only a bucket that actually holds observations forces the
    /// threshold out to its upper bound.
    #[test]
    fn add_does_not_widen_the_zero_region_for_an_empty_bucket() {
        let mut left = histogram(0, ResetHint::No);
        left.zero_threshold = 0.75;
        left.zero_count = 1.0;
        left.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 1,
        }];
        left.positive_counts = vec![0.0];
        let mut right = histogram(0, ResetHint::No);
        right.zero_threshold = 0.75;
        right.zero_count = 2.0;

        add_compatible_native_histogram(&mut left, &right).unwrap();

        assert2::assert!((left.zero_threshold - 0.75).abs() < f64::EPSILON);
        assert2::assert!((left.zero_count - 3.0).abs() < f64::EPSILON);
    }

    /// Adding two histograms recompacts the merged buckets back into spans.
    /// Three runs separated by gaps of different widths pin the offset
    /// arithmetic: with a single run there is none, and with two the first
    /// emitted span still has a zero `previous_span_end` to subtract, where
    /// subtracting and adding it agree.
    #[test]
    fn add_recompacts_merged_buckets_into_separate_runs() {
        let mut left = histogram(0, ResetHint::No);
        left.positive_spans = vec![
            BucketSpan {
                offset: 1,
                length: 2,
            },
            BucketSpan {
                offset: 7,
                length: 2,
            },
        ];
        left.positive_counts = vec![1.0, 2.0, 3.0, 4.0];
        let mut right = histogram(0, ResetHint::No);
        right.positive_spans = vec![BucketSpan {
            offset: 5,
            length: 2,
        }];
        right.positive_counts = vec![5.0, 6.0];

        add_compatible_native_histogram(&mut left, &right).unwrap();

        // Left holds 1,2 and 10,11; right holds 5,6. The merge runs
        // 1..=2, 5..=6, 10..=11 -- gaps of two and of three.
        assert2::assert!(
            left.positive_spans
                == vec![
                    BucketSpan {
                        offset: 1,
                        length: 2
                    },
                    BucketSpan {
                        offset: 2,
                        length: 2
                    },
                    BucketSpan {
                        offset: 3,
                        length: 2
                    },
                ]
        );
        assert2::assert!(left.positive_counts == vec![1.0, 2.0, 5.0, 6.0, 3.0, 4.0]);
    }

    #[test]
    fn add_reconciles_custom_bucket_bounds() {
        let mut left = histogram(-53, ResetHint::No);
        left.custom_values = Some(vec![1.0, 2.0, 4.0]);
        left.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 4,
        }];
        left.positive_counts = vec![1.0, 2.0, 3.0, 4.0];
        let mut right = histogram(-53, ResetHint::No);
        right.custom_values = Some(vec![1.0, 3.0, 4.0]);
        right.positive_spans = vec![BucketSpan {
            offset: 0,
            length: 4,
        }];
        right.positive_counts = vec![10.0, 20.0, 30.0, 40.0];

        add_compatible_native_histogram(&mut left, &right).unwrap();

        assert2::assert!(left.custom_values == Some(vec![1.0, 4.0]));
        assert2::assert!(
            left.positive_spans
                == vec![BucketSpan {
                    offset: 0,
                    length: 3
                }]
        );
        assert2::assert!(left.positive_counts == vec![11.0, 55.0, 44.0]);
    }

    #[yare::parameterized(
        no_hints = { ResetHint::No, ResetHint::No, ResetHint::No },
        explicit_reset = { ResetHint::Yes, ResetHint::No, ResetHint::Unknown },
        unknown_hint = { ResetHint::Unknown, ResetHint::No, ResetHint::Unknown },
        gauge_hint = { ResetHint::No, ResetHint::Gauge, ResetHint::Gauge },
    )]
    fn add_reconciles_counter_reset_hints(
        left_hint: ResetHint,
        right_hint: ResetHint,
        expected: ResetHint,
    ) {
        let mut left = histogram(0, left_hint);
        let right = histogram(0, right_hint);
        add_compatible_native_histogram(&mut left, &right).unwrap();
        assert2::assert!(left.reset_hint == expected);
    }

    #[test]
    fn add_rejects_exponential_and_custom_bucket_mix() {
        let mut exponential = histogram(0, ResetHint::No);
        let custom = histogram(-53, ResetHint::No);

        let error = add_compatible_native_histogram(&mut exponential, &custom).unwrap_err();

        assert2::assert!(format!("{error}").contains("exponential and custom-bucket"));
    }
}

mod add_compatible_native_histogram;
mod add_custom_histogram;
mod add_exponential_histogram;
mod almost_equal;
mod apply_histogram_accessor;
mod apply_histogram_reduction;
mod bucket_helpers;
mod detect_reset_bucket_counts;
mod fractions;
mod histogram_accumulator;
mod native_histogram_detect_reset;
mod native_histogram_stdvar;
mod quantiles;
mod reconcile_native_histogram_layouts;
mod template_histogram;
mod trim_native_histogram;
pub(crate) use add_compatible_native_histogram::add_compatible_native_histogram;
use add_custom_histogram::add_custom_histogram;
use add_exponential_histogram::add_exponential_histogram;
use almost_equal::almost_equal;
pub(super) use apply_histogram_accessor::apply_histogram_accessor;
pub(super) use apply_histogram_reduction::{HistogramReducers, apply_histogram_reduction};
pub(super) use bucket_helpers::{
    NormalizedClassicBuckets, ZeroCountAtThreshold, find_mixed_histogram_keys,
    group_classic_bucket_sample, normalized_classic_histogram_buckets, zero_count_at_threshold,
};
use bucket_helpers::{add_bucket_maps, append_native_spanned_buckets, native_histogram_buckets};
pub(crate) use bucket_helpers::{
    for_each_spanned_histogram_count, spanned_histogram_counts, standard_histogram_bound,
};
use detect_reset_bucket_counts::detect_reset_bucket_counts;
pub(super) use fractions::apply_histogram_fraction;
use fractions::{classic_histogram_fraction, native_histogram_fraction};
pub(super) use histogram_accumulator::HistogramAccumulator;
pub(crate) use krabka_metrics::compact_spanned_histogram_counts;
pub(super) use native_histogram_detect_reset::native_histogram_detect_reset;
use native_histogram_stdvar::native_histogram_stdvar;
pub(super) use quantiles::apply_histogram_quantile;
#[cfg(feature = "experimental-functions")]
pub(super) use quantiles::apply_histogram_quantiles;
use quantiles::{classic_histogram_quantile, format_quantile_label, native_histogram_quantile};
pub(crate) use template_histogram::template_histogram_value;

fn observed_histogram_count(
    walked_count: f64,
    buckets: &[NativeQuantileBucket],
    walked: usize,
) -> f64 {
    walked_count
        + buckets
            .iter()
            .skip(walked + 1)
            .map(|bucket| bucket.count)
            .sum::<f64>()
}
pub(super) use reconcile_native_histogram_layouts::reconcile_native_histogram_layouts;
pub(super) use trim_native_histogram::trim_native_histogram;

#[derive(Clone, Copy, Debug)]
pub(crate) struct ClassicBucket {
    pub(crate) upper_bound: f64,
    pub(crate) count: f64,
}

/// The observation-value bounds of `histogram_fraction(lower, upper, v)`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FractionBounds {
    pub(crate) lower: f64,
    pub(crate) upper: f64,
}

#[derive(Clone, Copy)]
pub(crate) struct NativeQuantileBucket {
    pub(crate) lower: f64,
    pub(crate) upper: f64,
    pub(crate) count: f64,
}

#[derive(Clone, Copy)]
pub(crate) enum HistogramAccessor {
    Count,
    Sum,
    Avg,
    Stddev,
    Stdvar,
}

impl HistogramAccessor {
    pub(crate) fn value(self, hist: &NativeHistogram) -> f64 {
        match self {
            Self::Count => hist.count,
            Self::Sum => hist.sum,
            Self::Avg => hist.sum / hist.count,
            Self::Stddev => native_histogram_stdvar(hist).sqrt(),
            Self::Stdvar => native_histogram_stdvar(hist),
        }
    }
}

pub(crate) fn combined_reset_hint(left: ResetHint, right: ResetHint) -> ResetHint {
    if left == right {
        left
    } else if left == ResetHint::Gauge || right == ResetHint::Gauge {
        ResetHint::Gauge
    } else {
        ResetHint::Unknown
    }
}

pub(crate) fn custom_histogram_bound(index: i32, custom_values: &[f64]) -> f64 {
    match index {
        -1 if custom_values.first().is_some_and(|value| *value > 0.0) => 0.0,
        -1 => f64::NEG_INFINITY,
        _ => usize::try_from(index)
            .ok()
            .and_then(|index| custom_values.get(index).copied())
            .unwrap_or(f64::INFINITY),
    }
}

pub(crate) fn scaled_native_histogram(histogram: &NativeHistogram, factor: f64) -> NativeHistogram {
    let mut out = histogram.clone();
    scale_native_histogram_values(&mut out, factor);
    if factor.is_sign_negative() {
        out.reset_hint = ResetHint::Gauge;
    }
    out
}

pub(crate) fn bucket_overlap_fraction(bucket: NativeQuantileBucket, lower: f64, upper: f64) -> f64 {
    let overlap_lower = bucket.lower.max(lower);
    let overlap_upper = bucket.upper.min(upper);
    // `>=` against `>` is a permanent mutation survivor: the two differ only on
    // a zero-width overlap, and there the fall-through divides that zero width
    // by the bucket's own and returns the same 0.0.
    if overlap_lower >= overlap_upper {
        return 0.0;
    }
    if bucket.lower.is_infinite() || bucket.upper.is_infinite() {
        // An open-ended bucket counts in full, or not at all: a finite query
        // bound never reaches into one, however far out it goes.
        //
        // Only two open bounds get this far. A `+Inf` lower or a `-Inf` upper
        // leaves the overlap empty against any range -- `max(+Inf, lower)` is
        // never below `upper`, and `min(-Inf, upper)` is never above `lower` --
        // so the test above has already returned for them.
        if bucket.lower.is_infinite() {
            return f64::from(lower.is_infinite() && lower.is_sign_negative());
        }
        return f64::from(upper.is_infinite() && upper.is_sign_positive());
    }
    (overlap_upper - overlap_lower) / (bucket.upper - bucket.lower)
}

/// Reads the `le` bound of one classic bucket series.
///
/// A series with no `le`, or with an `le` that is not a float, is not a bucket
/// at all. `resetHistograms` warns and drops it rather than failing the query,
/// because one malformed series should not take the rest of the vector with it.
pub(crate) fn classic_bucket_bound(labels: &Labels) -> Option<f64> {
    let label = labels.get("le").unwrap_or("");
    let Ok(bound) = parse_classic_bucket_bound(label) else {
        emit_warning(bad_bucket_label_warning(
            label,
            labels.get("__name__").unwrap_or(""),
        ));
        return None;
    };
    Some(bound)
}

pub(crate) fn classic_histogram_buckets(buckets: &[ClassicBucket]) -> Vec<NativeQuantileBucket> {
    let mut out = Vec::with_capacity(buckets.len());
    let mut lower = if buckets
        .first()
        .is_some_and(|bucket| bucket.upper_bound <= 0.0)
    {
        f64::NEG_INFINITY
    } else {
        0.0
    };
    let mut previous_count = 0.0;
    for bucket in buckets {
        let count = bucket.count - previous_count;
        previous_count = bucket.count;
        out.push(NativeQuantileBucket {
            lower,
            upper: bucket.upper_bound,
            count,
        });
        lower = bucket.upper_bound;
    }
    out
}

pub(crate) fn divide_native_histogram_values(histogram: &mut NativeHistogram, divisor: f64) {
    histogram.count /= divisor;
    histogram.sum /= divisor;
    histogram.zero_count /= divisor;
    for count in histogram
        .positive_counts
        .iter_mut()
        .chain(&mut histogram.negative_counts)
    {
        *count /= divisor;
    }
}

/// Maps a native-histogram accessor function name to its [`HistogramAccessor`] variant.
///
/// This function mirrors the accessor arms of `PromqlEngine::eval_instant_call`.
/// It returns `None` for any other function, so the planner dispatch falls
/// through.
pub(crate) fn histogram_accessor_from_function_name(name: &str) -> Option<HistogramAccessor> {
    Some(match name {
        "histogram_count" => HistogramAccessor::Count,
        "histogram_sum" => HistogramAccessor::Sum,
        "histogram_avg" => HistogramAccessor::Avg,
        "histogram_stddev" => HistogramAccessor::Stddev,
        "histogram_stdvar" => HistogramAccessor::Stdvar,
        _ => return None,
    })
}

/// Every populated bucket of `hist`, in ascending order of value.
///
/// This is `FloatHistogram.AllBucketIterator`: the negative buckets from the
/// most negative upwards, then the zero bucket where it holds anything, then
/// the positive buckets. Both `histogram_quantile` and `histogram_fraction`
/// walk the buckets in that order and accumulate a rank as they go, so the
/// order is part of the answer.
pub(crate) fn native_histogram_all_buckets(hist: &NativeHistogram) -> Vec<NativeQuantileBucket> {
    let mut buckets = native_histogram_buckets(hist);
    buckets.sort_by(|left, right| left.lower.total_cmp(&right.lower));
    buckets
}

/// The value that `histogram_stdvar` gives to the observations of `bucket`.
///
/// A custom bucket and the zero bucket take the arithmetic midpoint, so an
/// open custom bucket takes an infinite value. An exponential bucket takes the
/// geometric midpoint, negated below zero.
pub(crate) fn native_histogram_bucket_mean(
    hist: &NativeHistogram,
    bucket: NativeQuantileBucket,
) -> f64 {
    if hist.is_nhcb() || (bucket.lower <= 0.0 && bucket.upper >= 0.0) {
        return f64::midpoint(bucket.lower, bucket.upper);
    }
    if bucket.upper <= 0.0 {
        return -(bucket.lower * bucket.upper).sqrt();
    }
    (bucket.lower * bucket.upper).sqrt()
}

/// Interpolates `fraction` of the way through one bucket.
///
/// Custom buckets and the zero bucket interpolate LINEARLY, matching the
/// classic-histogram convention. An exponential bucket interpolates on a
/// logarithmic scale, where every bucket of a schema has the same width, so the
/// same fraction applies to the logarithms of the bounds and the result comes
/// back through `exp2`. A negative bucket mirrors that.
pub(crate) fn native_histogram_bucket_quantile(
    hist: &NativeHistogram,
    bucket: NativeQuantileBucket,
    fraction: f64,
) -> f64 {
    if hist.is_nhcb() || (bucket.lower <= 0.0 && bucket.upper >= 0.0) {
        return bucket.lower + (bucket.upper - bucket.lower) * fraction;
    }
    let log_lower = bucket.lower.abs().log2();
    let log_upper = bucket.upper.abs().log2();
    if bucket.lower > 0.0 {
        return (log_lower + (log_upper - log_lower) * fraction).exp2();
    }
    -(log_upper + (log_lower - log_upper) * (1.0 - fraction)).exp2()
}

pub(crate) fn native_histograms_are_range_compatible(
    left: &NativeHistogram,
    right: &NativeHistogram,
) -> bool {
    left.schema == right.schema
        && left.is_float == right.is_float
        && left.zero_threshold.to_bits() == right.zero_threshold.to_bits()
        && left.custom_values == right.custom_values
        && left.positive_spans == right.positive_spans
        && left.negative_spans == right.negative_spans
        && left.positive_counts.len() == right.positive_counts.len()
        && left.negative_counts.len() == right.negative_counts.len()
}

pub(crate) fn parse_classic_bucket_bound(value: &str) -> Result<f64> {
    // Both infinity arms are permanent mutation survivors, and both are
    // redundant: Rust's own float parser accepts "+Inf", "-Inf" and "Inf", so
    // deleting either falls through to the same value. They stay because the
    // `le` spellings are Prometheus wire format, and leaning on the standard
    // library happening to accept them is not something to discover later.
    match value {
        "+Inf" | "Inf" => Ok(f64::INFINITY),
        "-Inf" => Ok(f64::NEG_INFINITY),
        _ => value.parse::<f64>().map_err(|error| {
            PromqlError::Plan(format!(
                "invalid classic histogram bucket `{value}`: {error}"
            ))
        }),
    }
}

pub(crate) fn reduced_counts_outside_zero(
    histogram: &NativeHistogram,
    spans: &[BucketSpan],
    counts: &[f64],
    zero_threshold: f64,
    target_schema: i8,
) -> BTreeMap<i32, f64> {
    let schema_delta =
        u32::try_from(i16::from(histogram.schema) - i16::from(target_schema)).unwrap_or_default();
    let divisor = 1_i32.checked_shl(schema_delta).unwrap_or(i32::MAX);
    let mut out = BTreeMap::new();
    for entry in spanned_histogram_counts(spans, counts) {
        let index = entry.0;
        let count = entry.1;
        if standard_histogram_bound(index - 1, histogram.schema) < zero_threshold {
            continue;
        }
        let quotient = index.div_euclid(divisor);
        let target_index = quotient + i32::from(index.rem_euclid(divisor) != 0);
        *out.entry(target_index).or_insert(0.0) += count;
    }
    out
}

pub(crate) fn remap_custom_counts(
    spans: &[BucketSpan],
    counts: &[f64],
    source_values: &[f64],
    target_values: &[f64],
) -> BTreeMap<i32, f64> {
    let mut out = BTreeMap::new();
    for entry in spanned_histogram_counts(spans, counts) {
        let index = entry.0;
        let count = entry.1;
        let upper = custom_histogram_bound(index, source_values);
        let target_index = target_values
            .iter()
            .position(|bound| bound.total_cmp(&upper).is_ge())
            .unwrap_or(target_values.len());
        let target_index = i32::try_from(target_index).unwrap_or(i32::MAX);
        *out.entry(target_index).or_insert(0.0) += count;
    }
    out
}

pub(crate) fn scale_native_histogram_values(histogram: &mut NativeHistogram, factor: f64) {
    histogram.zero_count *= factor;
    histogram.count *= factor;
    histogram.sum *= factor;
    for count in &mut histogram.positive_counts {
        *count *= factor;
    }
    for count in &mut histogram.negative_counts {
        *count *= factor;
    }
}

/// Narrows the zero bucket's bounds to the side the histogram actually observed.
///
/// Prometheus reads a zero bucket in a histogram that has only positive buckets
/// as starting at zero, and one in a histogram that has only negative buckets as
/// ending at zero, so that an interpolation inside it does not reach across a
/// side where nothing was observed. A histogram with buckets on both sides, or
/// on neither, keeps the threshold on both sides.
pub(crate) fn zero_bucket_bounds(
    hist: &NativeHistogram,
    bucket: NativeQuantileBucket,
) -> NativeQuantileBucket {
    let mut bucket = bucket;
    if hist.negative_counts.is_empty() && !hist.positive_counts.is_empty() {
        bucket.lower = 0.0;
    } else if hist.positive_counts.is_empty() && !hist.negative_counts.is_empty() {
        bucket.upper = 0.0;
    }
    bucket
}
