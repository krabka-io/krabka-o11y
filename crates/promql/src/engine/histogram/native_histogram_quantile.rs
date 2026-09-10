use std::num::FpCategory;

use super::{
    NativeHistogram, NativeQuantileBucket, emit_info, native_histogram_all_buckets,
    native_histogram_bucket_quantile, native_histogram_quantile_nan_result_info,
    native_histogram_quantile_nan_skew_info, zero_bucket_bounds,
};

/// Estimates `quantile` from one native histogram.
///
/// This walks the buckets accumulating a rank until it passes `quantile *
/// count`, then interpolates inside the bucket it stopped in. A quantile at or
/// above the median walks from the TOP instead, which is what Prometheus does
/// and which changes the answer in the last bits.
///
/// A histogram whose `sum` is NaN observed a NaN, and those observations raise
/// `count` without landing in any bucket. Such a histogram is always walked
/// forwards, and it reports either an unreachable rank (the result is NaN) or a
/// rank the buckets do not fully account for (the result is skewed high).
pub(crate) fn native_histogram_quantile(
    quantile: f64,
    hist: &NativeHistogram,
    metric: &str,
) -> f64 {
    if quantile < 0.0 {
        return f64::NEG_INFINITY;
    }
    if quantile > 1.0 {
        return f64::INFINITY;
    }
    if matches!(hist.count.classify(), FpCategory::Zero) || quantile.is_nan() {
        return f64::NAN;
    }

    let buckets = native_histogram_all_buckets(hist);
    let forwards = hist.sum.is_nan() || quantile < 0.5;
    let mut rank = if forwards {
        quantile * hist.count
    } else {
        (1.0 - quantile) * hist.count
    };

    let mut walked = 0_usize;
    let mut count = 0.0_f64;
    let mut bucket = NativeQuantileBucket {
        lower: 0.0,
        upper: 0.0,
        count: 0.0,
    };
    for (position, candidate) in bucket_walk(&buckets, forwards) {
        walked = position;
        if candidate.count == 0.0 {
            continue;
        }
        bucket = candidate;
        count += candidate.count;
        if count >= rank {
            break;
        }
    }

    if hist.is_nhcb() {
        if bucket.lower.is_infinite() && bucket.lower.is_sign_negative() {
            if bucket.upper <= 0.0 {
                return bucket.upper;
            }
            bucket.lower = 0.0;
        } else if bucket.upper.is_infinite() && bucket.upper.is_sign_positive() {
            return bucket.lower;
        }
    } else if bucket.lower < 0.0 && bucket.upper > 0.0 {
        bucket = zero_bucket_bounds(hist, bucket);
    }

    // Rounding can push the running count past the histogram's own.
    count = count.min(hist.count);
    if count < rank {
        if hist.sum.is_nan() {
            emit_info(native_histogram_quantile_nan_result_info(metric));
            return f64::NAN;
        }
        return bucket.upper;
    }

    rank = if forwards {
        rank - (count - bucket.count)
    } else {
        count - rank
    };

    if hist.sum.is_nan() {
        let observed: f64 = count
            + buckets
                .iter()
                .skip(walked + 1)
                .map(|bucket| bucket.count)
                .sum::<f64>();
        if observed < hist.count {
            emit_info(native_histogram_quantile_nan_skew_info(metric));
        }
    }

    native_histogram_bucket_quantile(hist, bucket, rank / bucket.count)
}

/// The buckets to walk, paired with their index, in the direction the quantile
/// asks for.
fn bucket_walk(
    buckets: &[NativeQuantileBucket],
    forwards: bool,
) -> Box<dyn Iterator<Item = (usize, NativeQuantileBucket)> + '_> {
    if forwards {
        return Box::new(buckets.iter().copied().enumerate());
    }
    Box::new(buckets.iter().copied().enumerate().rev())
}
