use super::*;

pub(crate) fn classic_histogram_fraction(lower: f64, upper: f64, buckets: &mut [ClassicBucket]) -> f64 {
    if lower.is_nan() || upper.is_nan() {
        return f64::NAN;
    }
    if lower >= upper {
        return 0.0;
    }

    let buckets = normalized_classic_histogram_buckets(buckets);
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
