use super::{
    NativeHistogram, compact_spanned_histogram_counts, spanned_histogram_counts,
    standard_histogram_bound,
};

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
        let (spans, counts) = if positive {
            (&mut out.positive_spans, &mut out.positive_counts)
        } else {
            (&mut out.negative_spans, &mut out.negative_counts)
        };
        let mut buckets = spanned_histogram_counts(spans, counts);
        for (index, observations) in &mut buckets {
            if *observations == 0.0 {
                continue;
            }
            let (lower, higher) = if custom {
                (
                    trim_custom_bound(
                        *index - 1,
                        histogram.custom_values.as_deref().unwrap_or_default(),
                    ),
                    trim_custom_bound(
                        *index,
                        histogram.custom_values.as_deref().unwrap_or_default(),
                    ),
                )
            } else if positive {
                (
                    standard_histogram_bound(*index - 1, histogram.schema),
                    standard_histogram_bound(*index, histogram.schema),
                )
            } else {
                (
                    -standard_histogram_bound(*index, histogram.schema),
                    -standard_histogram_bound(*index - 1, histogram.schema),
                )
            };
            let (keep, midpoint) = if (upper && higher <= bound) || (!upper && lower >= bound) {
                (*observations, midpoint(lower, higher, positive, custom))
            } else if (upper && lower < bound) || (!upper && higher > bound) {
                partial_bucket(lower, higher, *observations, bound, upper, positive, custom)
            } else {
                (0.0, 0.0)
            };
            changed |= keep.partial_cmp(observations) != Some(std::cmp::Ordering::Equal);
            *observations = keep;
            count += keep;
            sum += keep * midpoint;
        }
        (*spans, *counts) = compact_spanned_histogram_counts(buckets);
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
        let (keep, midpoint) = if (upper && bound <= lower) || (!upper && bound >= higher) {
            (0.0, 0.0)
        } else if (upper && bound >= higher) || (!upper && bound <= lower) {
            (histogram.zero_count, linear_midpoint(lower, higher))
        } else if upper {
            (
                histogram.zero_count * (bound - lower) / (higher - lower),
                linear_midpoint(lower, bound),
            )
        } else {
            (
                histogram.zero_count * (higher - bound) / (higher - lower),
                linear_midpoint(bound, higher),
            )
        };
        changed |= keep.partial_cmp(&histogram.zero_count) != Some(std::cmp::Ordering::Equal);
        out.zero_count = keep;
        count += keep;
        sum += keep * midpoint;
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

pub(super) fn partial_bucket(
    lower: f64,
    higher: f64,
    count: f64,
    bound: f64,
    upper: bool,
    positive: bool,
    linear: bool,
) -> (f64, f64) {
    if lower.is_infinite() {
        if upper {
            if bound >= higher {
                return (count, 0.0);
            }
            if bound > 0.0 && higher > 0.0 && higher.is_finite() {
                return (count * bound / higher, bound / 2.0);
            }
            if higher <= 0.0 {
                return (count, bound);
            }
        } else {
            if bound <= lower {
                return (count, 0.0);
            }
            if bound >= 0.0 && higher > bound && higher.is_finite() {
                return (
                    count * (1.0 - bound / higher),
                    linear_midpoint(bound, higher),
                );
            }
        }
        return (0.0, if higher.is_finite() { higher } else { 0.0 });
    }
    if higher.is_infinite() {
        if !upper && bound >= lower {
            return (count, bound);
        }
        return (0.0, if lower.is_finite() { lower } else { 0.0 });
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
        (below, midpoint(lower, bound, positive, linear))
    } else {
        (count - below, midpoint(bound, higher, positive, linear))
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

    #[test]
    fn an_untrimmed_histogram_keeps_zero_gaps_and_signed_sum() {
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
        for (bound, upper) in [(f64::INFINITY, true), (f64::NEG_INFINITY, false)] {
            let actual = trim_native_histogram(&input, bound, upper);
            assert2::assert!(actual == input);
            assert2::assert!(actual.sum.to_bits() == (-0.0_f64).to_bits());
        }
    }
}
