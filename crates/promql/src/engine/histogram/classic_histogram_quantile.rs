use super::{ClassicBucket, normalized_classic_histogram_buckets};

pub(crate) fn classic_histogram_quantile(quantile: f64, buckets: &mut [ClassicBucket]) -> f64 {
    if quantile.is_nan() {
        return f64::NAN;
    }
    if quantile < 0.0 {
        return f64::NEG_INFINITY;
    }
    if quantile > 1.0 {
        return f64::INFINITY;
    }

    let buckets = normalized_classic_histogram_buckets(buckets);
    if buckets.len() < 2
        || !buckets.last().is_some_and(|bucket| {
            bucket.upper_bound.is_infinite() && bucket.upper_bound.is_sign_positive()
        })
    {
        return f64::NAN;
    }

    let total = buckets.last().map_or(0.0, |bucket| bucket.count);
    if total <= 0.0 || total.is_nan() {
        return f64::NAN;
    }
    let rank = quantile * total;
    // The fallback is a permanent mutation survivor because it is never
    // taken: counts are made monotonic above, the last one is the total, and
    // `rank` is at most that total, so the search always finds a bucket.
    let bucket_index = buckets
        .iter()
        .position(|bucket| bucket.count >= rank)
        .unwrap_or(buckets.len() - 1);

    if bucket_index == buckets.len() - 1 {
        return buckets[bucket_index - 1].upper_bound;
    }

    let bucket = buckets[bucket_index];
    let (lower_bound, previous_count) = if bucket_index == 0 {
        if bucket.upper_bound <= 0.0 {
            return bucket.upper_bound;
        }
        (0.0, 0.0)
    } else {
        let previous = buckets[bucket_index - 1];
        (previous.upper_bound, previous.count)
    };

    let bucket_count = bucket.count - previous_count;
    if bucket_count <= 0.0 {
        return bucket.upper_bound;
    }
    lower_bound + (bucket.upper_bound - lower_bound) * ((rank - previous_count) / bucket_count)
}
