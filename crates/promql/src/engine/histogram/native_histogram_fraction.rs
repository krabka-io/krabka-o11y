use std::num::FpCategory;

use super::{
    NativeHistogram, NativeQuantileBucket, emit_info, native_histogram_all_buckets,
    native_histogram_fraction_nans_info, zero_bucket_bounds,
};

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
        let observed: f64 = count
            + buckets
                .iter()
                .skip(walked + 1)
                .map(|bucket| bucket.count)
                .sum::<f64>();
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
