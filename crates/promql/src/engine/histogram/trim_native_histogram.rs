use super::{
    NativeHistogram, compact_spanned_histogram_counts, spanned_histogram_counts,
    standard_histogram_bound,
};

struct BucketBounds {
    lower: f64,
    higher: f64,
}

pub(crate) fn trim_native_histogram(
    histogram: &NativeHistogram,
    bound: f64,
    upper: bool,
) -> NativeHistogram {
    let mut out = histogram.clone();
    let mut count = 0.0;
    let mut sum = 0.0;
    let mut changed = false;
    let custom = histogram.is_nhcb();
    for positive in [true, false] {
        let mut buckets = if positive {
            spanned_histogram_counts(&mut out.positive_spans, &mut out.positive_counts)
        } else {
            spanned_histogram_counts(&mut out.negative_spans, &mut out.negative_counts)
        };
        let bucket_indexes = buckets.keys().copied().collect::<Vec<_>>();
        for index in bucket_indexes {
            let observations = buckets[&index];
            if observations == 0.0 {
                continue;
            }
            let bounds = if custom {
                BucketBounds {
                    lower: trim_custom_bound(
                        index - 1,
                        histogram.custom_values.as_deref().unwrap_or_default(),
                    ),
                    higher: trim_custom_bound(
                        index,
                        histogram.custom_values.as_deref().unwrap_or_default(),
                    ),
                }
            } else if positive {
                BucketBounds {
                    lower: standard_histogram_bound(index - 1, histogram.schema),
                    higher: standard_histogram_bound(index, histogram.schema),
                }
            } else {
                BucketBounds {
                    lower: -standard_histogram_bound(index, histogram.schema),
                    higher: -standard_histogram_bound(index - 1, histogram.schema),
                }
            };
            let bucket_trim =
                if (upper && bounds.higher <= bound) || (!upper && bounds.lower >= bound) {
                    BucketTrim {
                        keep: observations,
                        midpoint: midpoint(bounds.lower, bounds.higher, positive, custom),
                    }
                } else if (upper && bounds.lower < bound) || (!upper && bounds.higher > bound) {
                    partial_bucket(
                        bounds.lower,
                        bounds.higher,
                        observations,
                        bound,
                        upper,
                        positive,
                        custom,
                    )
                } else {
                    BucketTrim {
                        keep: 0.0,
                        midpoint: 0.0,
                    }
                };
            changed |=
                bucket_trim.keep.partial_cmp(&observations) != Some(std::cmp::Ordering::Equal);
            buckets.insert(index, bucket_trim.keep);
            count += bucket_trim.keep;
            sum += bucket_trim.keep * bucket_trim.midpoint;
        }
        let compacted = compact_spanned_histogram_counts(buckets);
        if positive {
            out.positive_spans = compacted.spans;
            out.positive_counts = compacted.counts;
        } else {
            out.negative_spans = compacted.spans;
            out.negative_counts = compacted.counts;
        }
    }
    if histogram.zero_count > 0.0 {
        let positive = histogram.positive_counts.iter().any(|count| *count != 0.0);
        let negative = histogram.negative_counts.iter().any(|count| *count != 0.0);
        let lower = if positive && !negative {
            0.0
        } else {
            -histogram.zero_threshold
        };
        let higher = if negative && !positive {
            0.0
        } else {
            histogram.zero_threshold
        };
        let bucket_trim = if (upper && bound <= lower) || (!upper && bound >= higher) {
            BucketTrim {
                keep: 0.0,
                midpoint: 0.0,
            }
        } else if (upper && bound >= higher) || (!upper && bound <= lower) {
            BucketTrim {
                keep: histogram.zero_count,
                midpoint: linear_midpoint(lower, higher),
            }
        } else if upper {
            BucketTrim {
                keep: histogram.zero_count * (bound - lower) / (higher - lower),
                midpoint: linear_midpoint(lower, bound),
            }
        } else {
            BucketTrim {
                keep: histogram.zero_count * (higher - bound) / (higher - lower),
                midpoint: linear_midpoint(bound, higher),
            }
        };
        changed |=
            bucket_trim.keep.partial_cmp(&histogram.zero_count) != Some(std::cmp::Ordering::Equal);
        out.zero_count = bucket_trim.keep;
        count += bucket_trim.keep;
        sum += bucket_trim.keep * bucket_trim.midpoint;
    }
    if !changed {
        return histogram.clone();
    }
    out.count = count;
    out.sum = sum;
    out
}

pub(super) fn midpoint(lower: f64, upper: f64, positive: bool, linear: bool) -> f64 {
    if lower.is_infinite() {
        if upper.is_infinite() {
            0.0
        } else if upper > 0.0 {
            upper / 2.0
        } else {
            upper
        }
    } else if upper.is_infinite() {
        lower
    } else if linear {
        linear_midpoint(lower, upper)
    } else if positive {
        (lower * upper).abs().sqrt()
    } else {
        -(lower * upper).abs().sqrt()
    }
}

pub(super) struct BucketTrim {
    pub(super) keep: f64,
    pub(super) midpoint: f64,
}

pub(super) fn partial_bucket(
    lower: f64,
    higher: f64,
    count: f64,
    bound: f64,
    upper: bool,
    positive: bool,
    linear: bool,
) -> BucketTrim {
    if lower.is_infinite() {
        if upper {
            if bound >= higher {
                return BucketTrim {
                    keep: count,
                    midpoint: 0.0,
                };
            }
            if bound > 0.0 && higher > 0.0 && higher.is_finite() {
                return BucketTrim {
                    keep: count * bound / higher,
                    midpoint: bound / 2.0,
                };
            }
            if higher <= 0.0 {
                return BucketTrim {
                    keep: count,
                    midpoint: bound,
                };
            }
        } else {
            if bound <= lower {
                return BucketTrim {
                    keep: count,
                    midpoint: 0.0,
                };
            }
            if bound >= 0.0 && higher > bound && higher.is_finite() {
                return BucketTrim {
                    keep: count * (1.0 - bound / higher),
                    midpoint: linear_midpoint(bound, higher),
                };
            }
        }
        return BucketTrim {
            keep: 0.0,
            midpoint: if higher.is_finite() { higher } else { 0.0 },
        };
    }
    if higher.is_infinite() {
        if !upper && bound >= lower {
            return BucketTrim {
                keep: count,
                midpoint: bound,
            };
        }
        return BucketTrim {
            keep: 0.0,
            midpoint: if lower.is_finite() { lower } else { 0.0 },
        };
    }
    let below = if bound <= lower {
        0.0
    } else if bound >= higher {
        count
    } else if linear {
        count * (bound - lower) / (higher - lower)
    } else if bound > 0.0 {
        count * (bound.log2() - lower.abs().log2()) / (higher.abs().log2() - lower.abs().log2())
    } else {
        count
            * (1.0
                - (bound.abs().log2() - higher.abs().log2())
                    / (lower.abs().log2() - higher.abs().log2()))
    };
    if upper {
        BucketTrim {
            keep: below,
            midpoint: midpoint(lower, bound, positive, linear),
        }
    } else {
        BucketTrim {
            keep: count - below,
            midpoint: midpoint(bound, higher, positive, linear),
        }
    }
}

fn trim_custom_bound(index: i32, bounds: &[f64]) -> f64 {
    if index < 0 {
        f64::NEG_INFINITY
    } else {
        usize::try_from(index)
            .ok()
            .and_then(|index| bounds.get(index))
            .copied()
            .unwrap_or(f64::INFINITY)
    }
}

fn linear_midpoint(lower: f64, upper: f64) -> f64 {
    // Preserve the pinned Go sum-then-divide arithmetic, including overflow.
    let sum = lower + upper;
    sum / 2.0
}

#[cfg(test)]
mod tests {
    use krabka_metrics::{BucketSpan, NativeHistogram, ResetHint};

    use super::trim_native_histogram;

    #[yare::parameterized(
        positive = { f64::INFINITY, true },
        negative = { f64::NEG_INFINITY, false },
    )]
    fn an_untrimmed_histogram_keeps_zero_gaps_and_signed_sum(bound: f64, upper: bool) {
        let input = NativeHistogram {
            schema: 0,
            is_float: true,
            reset_hint: ResetHint::Unknown,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 2.0,
            sum: -0.0,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 3,
            }],
            positive_counts: vec![1.0, 0.0, 1.0],
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: None,
            start_timestamp_ms: None,
        };
        let actual = trim_native_histogram(&input, bound, upper);
        assert2::assert!(actual == input);
        assert2::assert!(actual.sum.to_bits() == (-0.0_f64).to_bits());
    }
}
