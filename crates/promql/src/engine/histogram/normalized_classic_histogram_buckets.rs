use super::*;

pub(crate) fn normalized_classic_histogram_buckets(buckets: &mut [ClassicBucket]) -> Vec<ClassicBucket> {
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

    let mut max_count = 0.0_f64;
    for bucket in &mut out {
        max_count = max_count.max(bucket.count);
        bucket.count = max_count;
    }
    out
}
