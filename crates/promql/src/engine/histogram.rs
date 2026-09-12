use std::collections::{BTreeMap, BTreeSet};

use krabka_blockstore::Labels;
use krabka_metrics::{BucketSpan, NativeHistogram, ResetHint};

use super::{
    annotations::{
        bad_bucket_label_warning, emit_info, emit_warning,
        histogram_quantile_forced_monotonicity_info, invalid_quantile_warning, is_valid_quantile,
        native_histogram_fraction_nans_info, native_histogram_quantile_nan_result_info,
        native_histogram_quantile_nan_skew_info, warn_mixed_histograms,
    },
    labels::{
        float_sample_value, labels_key, labels_without_label, labels_without_metric_name,
        record_metric_name,
    },
};
use crate::{
    error::{PromqlError, Result},
    result::{InstantSample, SampleValue},
};

#[cfg(test)]
mod tests {
    use super::*;

    fn histogram(schema: i8, reset_hint: ResetHint) -> NativeHistogram {
        NativeHistogram {
            schema,
            is_float: true,
            reset_hint,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 0.0,
            sum: 0.0,
            positive_spans: Vec::new(),
            positive_counts: Vec::new(),
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: (schema == -53).then(Vec::new),
            start_timestamp_ms: None,
        }
    }

    /// Two histograms are range-compatible only when *every* shape field
    /// agrees. Each case below differs in exactly one of them, and each is on
    /// its own enough to make the pair incompatible.
    #[test]
    fn range_compatibility_needs_every_shape_field_to_agree() {
        type Difference = (&'static str, fn(&mut NativeHistogram));
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
            ("schema", |h| h.schema = 1),
            ("is_float", |h| h.is_float = !h.is_float),
            ("zero_threshold", |h| h.zero_threshold = 1.0),
            ("custom_values", |h| h.custom_values = Some(vec![1.0])),
            ("positive_spans", |h| {
                h.positive_spans = vec![BucketSpan {
                    offset: 2,
                    length: 2,
                }];
            }),
            ("negative_spans", |h| {
                h.negative_spans = vec![BucketSpan {
                    offset: 1,
                    length: 1,
                }];
            }),
            ("positive_counts length", |h| h.positive_counts.push(9.0)),
            ("negative_counts length", |h| h.negative_counts.push(9.0)),
        ];
        for (name, differ) in differing {
            let mut right = base();
            differ(&mut right);
            assert2::check!(
                !native_histograms_are_range_compatible(&base(), &right),
                "{name} alone must make the pair incompatible"
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
        type Case = (&'static str, NativeHistogram, NativeHistogram, bool);
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

        let cases: [Case; 10] = [
            // The count of 5 moved from bucket 0 to bucket 1. A positional walk
            // compares [5] against [5] and sees nothing.
            (
                "a count that moved to the next bucket",
                positive(0, 5.0, vec![span(0, 1)], vec![5.0]),
                positive(0, 5.0, vec![span(1, 1)], vec![5.0]),
                true,
            ),
            // Buckets 0 and 2, spelled once as two spans and once as one span
            // with a hole in it. Nothing moved.
            (
                "the same buckets described by different spans",
                positive(0, 3.0, vec![span(0, 1), span(1, 1)], vec![1.0, 2.0]),
                positive(0, 3.0, vec![span(0, 3)], vec![1.0, 0.0, 2.0]),
                false,
            ),
            // Bucket 2 fell from 2 to 1, but it is the third count in the
            // current vector and the second in the previous one.
            (
                "a bucket that shrank at an index the vectors do not share",
                positive(0, 3.0, vec![span(0, 1), span(1, 1)], vec![1.0, 2.0]),
                positive(0, 7.0, vec![span(0, 3)], vec![1.0, 5.0, 1.0]),
                true,
            ),
            (
                "a populated bucket the current histogram no longer carries",
                positive(0, 3.0, vec![span(0, 2)], vec![1.0, 2.0]),
                positive(0, 3.0, vec![span(0, 1)], vec![1.0]),
                true,
            ),
            (
                "an empty bucket the current histogram no longer carries",
                positive(0, 1.0, vec![span(0, 2)], vec![1.0, 0.0]),
                positive(0, 1.0, vec![span(0, 1)], vec![1.0]),
                false,
            ),
            // Schema 1 buckets 1 and 2 are both halves of schema 0 bucket 1, so
            // they merge to 7 before the comparison.
            (
                "a finer previous schema merged short of the coarser current one",
                positive(1, 7.0, vec![span(1, 2)], vec![3.0, 4.0]),
                positive(0, 10.0, vec![span(1, 1)], vec![6.0]),
                true,
            ),
            (
                "a finer previous schema merged level with the coarser current one",
                positive(1, 7.0, vec![span(1, 2)], vec![3.0, 4.0]),
                positive(0, 10.0, vec![span(1, 1)], vec![7.0]),
                false,
            ),
            (
                "a negative count that moved to the next bucket",
                negative(5.0, vec![span(0, 1)], vec![5.0]),
                negative(5.0, vec![span(1, 1)], vec![5.0]),
                true,
            ),
            // Widening the zero threshold to 1 folds the previous histogram's
            // bucket 0, which spans (0.5, 1], into its zero count: 3 + 5 = 8.
            (
                "a zero count short of the buckets the wider threshold swallowed",
                zero_region(0.0, 3.0, vec![5.0]),
                zero_region(1.0, 3.0, Vec::new()),
                true,
            ),
            (
                "a zero count level with the buckets the wider threshold swallowed",
                zero_region(0.0, 3.0, vec![5.0]),
                zero_region(1.0, 8.0, Vec::new()),
                false,
            ),
        ];

        for (name, previous, current, expected) in cases {
            assert2::check!(
                native_histogram_detect_reset(&previous, &current) == expected,
                "{name}"
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

    #[test]
    fn add_reconciles_counter_reset_hints() {
        let cases = [
            (ResetHint::No, ResetHint::No, ResetHint::No),
            (ResetHint::Yes, ResetHint::No, ResetHint::Unknown),
            (ResetHint::Unknown, ResetHint::No, ResetHint::Unknown),
            (ResetHint::No, ResetHint::Gauge, ResetHint::Gauge),
        ];
        for (left_hint, right_hint, expected) in cases {
            let mut left = histogram(0, left_hint);
            let right = histogram(0, right_hint);
            add_compatible_native_histogram(&mut left, &right).unwrap();
            assert2::assert!(left.reset_hint == expected);
        }
    }

    #[test]
    fn add_rejects_exponential_and_custom_bucket_mix() {
        let mut exponential = histogram(0, ResetHint::No);
        let custom = histogram(-53, ResetHint::No);

        let error = add_compatible_native_histogram(&mut exponential, &custom).unwrap_err();

        assert2::assert!(format!("{error}").contains("exponential and custom-bucket"));
    }
}

mod add_bucket_maps;
mod add_compatible_native_histogram;
mod add_custom_histogram;
mod add_exponential_histogram;
mod almost_equal;
mod append_native_spanned_buckets;
mod apply_histogram_accessor;
mod apply_histogram_fraction;
mod apply_histogram_quantile;
mod apply_histogram_quantiles;
mod bucket_overlap_fraction;
mod classic_bucket;
mod classic_bucket_bound;
mod classic_histogram_buckets;
mod classic_histogram_fraction;
mod classic_histogram_quantile;
mod combined_reset_hint;
mod compact_spanned_histogram_counts;
mod custom_histogram_bound;
mod detect_reset_bucket_counts;
mod histogram_accessor;
mod histogram_accessor_from_function_name;
mod native_histogram_all_buckets;
mod native_histogram_bucket_mean;
mod native_histogram_bucket_quantile;
mod native_histogram_buckets;
mod native_histogram_detect_reset;
mod native_histogram_fraction;
mod native_histogram_quantile;
mod native_histogram_stdvar;
mod native_histograms_are_range_compatible;
mod native_quantile_bucket;
mod normalized_classic_histogram_buckets;
mod parse_classic_bucket_bound;
mod reduced_counts_outside_zero;
mod remap_custom_counts;
mod scale_native_histogram_values;
mod scaled_native_histogram;
mod spanned_histogram_counts;
mod standard_histogram_bound;
mod zero_bucket_bounds;
mod zero_count_at_threshold;

use add_bucket_maps::add_bucket_maps;
pub(crate) use add_compatible_native_histogram::add_compatible_native_histogram;
use add_custom_histogram::add_custom_histogram;
use add_exponential_histogram::add_exponential_histogram;
use almost_equal::almost_equal;
use append_native_spanned_buckets::append_native_spanned_buckets;
pub(super) use apply_histogram_accessor::apply_histogram_accessor;
pub(super) use apply_histogram_fraction::apply_histogram_fraction;
pub(super) use apply_histogram_quantile::apply_histogram_quantile;
#[cfg(feature = "experimental-functions")]
pub(super) use apply_histogram_quantiles::apply_histogram_quantiles;
use bucket_overlap_fraction::bucket_overlap_fraction;
use classic_bucket::ClassicBucket;
use classic_bucket_bound::classic_bucket_bound;
use classic_histogram_buckets::classic_histogram_buckets;
use classic_histogram_fraction::classic_histogram_fraction;
use classic_histogram_quantile::classic_histogram_quantile;
use combined_reset_hint::combined_reset_hint;
use compact_spanned_histogram_counts::compact_spanned_histogram_counts;
use custom_histogram_bound::custom_histogram_bound;
use detect_reset_bucket_counts::detect_reset_bucket_counts;
pub(super) use histogram_accessor::HistogramAccessor;
pub(super) use histogram_accessor_from_function_name::histogram_accessor_from_function_name;
use native_histogram_all_buckets::native_histogram_all_buckets;
use native_histogram_bucket_mean::native_histogram_bucket_mean;
use native_histogram_bucket_quantile::native_histogram_bucket_quantile;
use native_histogram_buckets::native_histogram_buckets;
pub(super) use native_histogram_detect_reset::native_histogram_detect_reset;
use native_histogram_fraction::native_histogram_fraction;
use native_histogram_quantile::native_histogram_quantile;
use native_histogram_stdvar::native_histogram_stdvar;
pub(super) use native_histograms_are_range_compatible::native_histograms_are_range_compatible;
use native_quantile_bucket::NativeQuantileBucket;
use normalized_classic_histogram_buckets::normalized_classic_histogram_buckets;
use parse_classic_bucket_bound::parse_classic_bucket_bound;
use reduced_counts_outside_zero::reduced_counts_outside_zero;
use remap_custom_counts::remap_custom_counts;
pub(super) use scale_native_histogram_values::scale_native_histogram_values;
pub(super) use scaled_native_histogram::scaled_native_histogram;
use spanned_histogram_counts::spanned_histogram_counts;
use standard_histogram_bound::standard_histogram_bound;
use zero_bucket_bounds::zero_bucket_bounds;
use zero_count_at_threshold::zero_count_at_threshold;
