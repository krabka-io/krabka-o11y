use super::{ClassicBucket, almost_equal};

/// Threshold for a relative delta between two classic buckets that counts as a
/// rounding error rather than a real decrease. This is Prometheus'
/// `smallDeltaTolerance`.
const SMALL_DELTA_TOLERANCE: f64 = 1e-12;

/// Sorts, merges, and makes monotonic the classic buckets of one series.
///
/// The second return value is whether monotonicity had to be FORCED: a bucket
/// count fell by more than a rounding error, and `ensureMonotonicAndIgnoreSmallDeltas`
/// clamped it back up. Prometheus reports that as an info annotation, because
/// the quantile it then computes is a guess. A fall within the tolerance is
/// corrected silently.
pub(crate) fn normalized_classic_histogram_buckets(
    buckets: &mut [ClassicBucket],
) -> (Vec<ClassicBucket>, bool, [f64; 3]) {
    buckets.sort_by(|left, right| left.upper_bound.total_cmp(&right.upper_bound));

    let mut out: Vec<ClassicBucket> = Vec::with_capacity(buckets.len());
    for bucket in buckets.iter().copied() {
        if let Some(previous) = out.last_mut()
            && previous.upper_bound.total_cmp(&bucket.upper_bound).is_eq()
        {
            previous.count += bucket.count;
            continue;
        }
        out.push(bucket);
    }

    let mut forced = false;
    let mut repairs = [f64::INFINITY, f64::NEG_INFINITY, 0.0_f64];
    let Some(mut previous) = out.first().map(|bucket| bucket.count) else {
        return (out, forced, repairs);
    };
    for bucket in out.iter_mut().skip(1) {
        let count = bucket.count;
        if almost_equal(count, previous, SMALL_DELTA_TOLERANCE) {
            bucket.count = previous;
            continue;
        }
        if count < previous {
            repairs[0] = repairs[0].min(bucket.upper_bound);
            repairs[1] = repairs[1].max(bucket.upper_bound);
            repairs[2] = repairs[2].max(previous - count);
            bucket.count = previous;
            forced = true;
            continue;
        }
        previous = count;
    }
    (out, forced, repairs)
}
