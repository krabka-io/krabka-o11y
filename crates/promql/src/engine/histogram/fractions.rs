use std::num::FpCategory;

use super::{
    ClassicBucket, FractionBounds, HistogramReducers, InstantSample, NativeHistogram,
    NativeQuantileBucket, Result, apply_histogram_reduction, bucket_overlap_fraction,
    classic_histogram_buckets, emit_info, native_histogram_all_buckets,
    native_histogram_fraction_nans_info, normalized_classic_histogram_buckets, zero_bucket_bounds,
};

/// Applies `histogram_fraction(lower, upper, v)` to an instant vector `v`,
/// with `bounds` holding `lower` and `upper`.
///
/// This function mirrors `PromqlEngine::eval_histogram_fraction_call` exactly.
/// Native-histogram rows fold through [`native_histogram_fraction`] and keep the
/// source timestamp. This function groups classic `<metric>_bucket{le}` float
/// rows by labelset and drops `__name__` and `le` from the group. Each group
/// then folds through [`classic_histogram_fraction`] and carries `time_ms`.
///
/// This function drops a labelset that carries both a classic and a native
/// histogram from the output. It raises the
/// `MixedClassicNativeHistogramsWarning` through the in-scope annotation sink,
/// exactly as the interpreter does. The interpreter and the operator path share
/// this function, so the two are parity-exact.
///
/// # Errors
///
/// Returns [`PromqlError`] for an unparseable `le` bound. Returns
/// [`PromqlError`] for a non-float classic bucket count. These are exactly the
/// errors the interpreter raised inline.
pub(crate) fn apply_histogram_fraction(
    bounds: FractionBounds,
    samples: Vec<InstantSample>,
    time_ms: i64,
) -> Result<Vec<InstantSample>> {
    let FractionBounds { lower, upper } = bounds;
    apply_histogram_reduction(
        samples,
        time_ms,
        HistogramReducers {
            native: |histogram: &NativeHistogram, metric: &str| {
                native_histogram_fraction(lower, upper, histogram, metric)
            },
            classic: |_: &str, buckets: &mut Vec<ClassicBucket>| {
                classic_histogram_fraction(lower, upper, buckets)
            },
        },
    )
}

pub(crate) fn classic_histogram_fraction(
    lower: f64,
    upper: f64,
    buckets: &mut [ClassicBucket],
) -> f64 {
    if lower.is_nan() || upper.is_nan() {
        return f64::NAN;
    }
    if lower >= upper {
        return 0.0;
    }

    let normalized = normalized_classic_histogram_buckets(buckets);
    let buckets = normalized.buckets;
    if !buckets.last().is_some_and(|bucket| {
        bucket.upper_bound.is_infinite() && bucket.upper_bound.is_sign_positive()
    }) {
        return f64::NAN;
    }

    // `||` here is a permanent survivor against `&&`: normalization runs a
    // maximum from zero over the counts, so the total is never negative, and
    // at exactly zero every bucket is empty and the division below reaches NaN
    // on its own.
    let total = buckets.last().map_or(0.0, |bucket| bucket.count);
    if total <= 0.0 || total.is_nan() {
        return f64::NAN;
    }

    classic_histogram_buckets(&buckets)
        .into_iter()
        .map(|bucket| bucket.count * bucket_overlap_fraction(bucket, lower, upper))
        .sum::<f64>()
        / total
}

/// The fraction of `hist`'s observations that fall between `lower` and `upper`.
///
/// This is the inverse of `histogram_quantile`, and it interpolates the same
/// way: the rank of each bound is found by walking the buckets and, where a
/// bound falls inside one, interpolating within it -- LOGARITHMICALLY for an
/// exponential bucket, linearly for a custom bucket or the zero bucket. A
/// bucket of infinite width contributes nothing to a finite bound, since there
/// is no meaningful place inside it to interpolate to.
///
/// A histogram whose `sum` is NaN observed NaNs, which raise `count` without
/// landing in a bucket, so its fractions cannot reach 1 and it says so.
pub(crate) fn native_histogram_fraction(
    lower: f64,
    upper: f64,
    hist: &NativeHistogram,
    metric: &str,
) -> f64 {
    if matches!(hist.count.classify(), FpCategory::Zero) || lower.is_nan() || upper.is_nan() {
        return f64::NAN;
    }
    if lower >= upper {
        return 0.0;
    }

    let buckets = native_histogram_all_buckets(hist);
    let nhcb = hist.is_nhcb();
    let mut rank = 0.0_f64;
    let mut count = 0.0_f64;
    let mut lower_rank = 0.0_f64;
    let mut upper_rank = 0.0_f64;
    let mut lower_set = false;
    let mut upper_set = false;
    let mut walked = 0_usize;

    for (position, bucket) in buckets.iter().copied().enumerate() {
        walked = position;
        count += bucket.count;
        let zero_bucket = bucket.lower <= 0.0 && bucket.upper >= 0.0;
        let bucket = if zero_bucket {
            zero_bucket_bounds(hist, bucket)
        } else {
            bucket
        };
        let interpolate = |bound: f64| {
            if nhcb || zero_bucket {
                return interpolate_linearly(bucket, rank, bound);
            }
            interpolate_exponentially(bucket, rank, bound)
        };

        if !lower_set && bucket.lower >= lower {
            lower_rank = rank;
            lower_set = true;
        }
        if !upper_set && bucket.lower >= upper {
            upper_rank = rank;
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        if !lower_set && bucket.lower < lower && bucket.upper > lower {
            lower_rank = interpolate(lower);
            lower_set = true;
        }
        if !upper_set && bucket.lower < upper && bucket.upper > upper {
            upper_rank = interpolate(upper);
            upper_set = true;
        }
        if lower_set && upper_set {
            break;
        }
        rank += bucket.count;
    }

    if hist.sum.is_nan() {
        let observed = super::observed_histogram_count(count, &buckets, walked);
        if observed < hist.count {
            emit_info(native_histogram_fraction_nans_info(metric));
        }
        count = observed;
    } else {
        count = hist.count;
    }

    if !lower_set || lower_rank > count {
        lower_rank = count;
    }
    if !upper_set || upper_rank > count {
        upper_rank = count;
    }
    (upper_rank - lower_rank) / hist.count
}

/// The rank of `bound` inside a linearly-interpolated bucket.
///
/// A bucket that runs to an infinity has no meaningful inside, so the whole
/// bucket is skipped: an upper bound of `+Inf` makes the second term zero, and
/// a lower bound of `-Inf` is handled by hand to the same effect.
fn interpolate_linearly(bucket: NativeQuantileBucket, rank: f64, bound: f64) -> f64 {
    if bucket.lower.is_infinite() && bucket.lower.is_sign_negative() {
        return bucket.count;
    }
    rank + bucket.count * (bound - bucket.lower) / (bucket.upper - bucket.lower)
}

/// The rank of `bound` inside a logarithmically-interpolated exponential bucket.
fn interpolate_exponentially(bucket: NativeQuantileBucket, rank: f64, bound: f64) -> f64 {
    let log_lower = bucket.lower.abs().log2();
    let log_upper = bucket.upper.abs().log2();
    let log_bound = bound.abs().log2();
    let fraction = if bound > 0.0 {
        (log_bound - log_lower) / (log_upper - log_lower)
    } else {
        1.0 - ((log_bound - log_upper) / (log_lower - log_upper))
    };
    rank + bucket.count * fraction
}
