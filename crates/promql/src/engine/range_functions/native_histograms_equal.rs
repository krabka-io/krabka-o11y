use std::cmp::Ordering;

use super::NativeHistogram;

/// Whether two native histograms are the same observation.
///
/// This is `FloatHistogram.Equals`, which `changes` uses to decide whether one
/// histogram sample differs from the one before it. The counter-reset hint is
/// not part of the comparison: it says where the sample sat in a chunk, not
/// what it observed.
///
/// Prometheus compares every COUNT bitwise, through `math.Float64bits` for the
/// observation count, the sum and the zero count, and through
/// `floatBucketsMatch` for the buckets. That is deliberate rather than
/// incidental: a sample whose sum is NaN has to equal the same sample read back
/// out of a chunk, and a count that flips from `+0` to `-0` really did change.
/// The zero THRESHOLD is the one float it compares numerically, with `!=`, so
/// a `+0` and a `-0` threshold are the same threshold.
pub(crate) fn native_histograms_equal(left: &NativeHistogram, right: &NativeHistogram) -> bool {
    left.schema == right.schema
        && left.count.to_bits() == right.count.to_bits()
        && left.sum.to_bits() == right.sum.to_bits()
        && left.zero_threshold.partial_cmp(&right.zero_threshold) == Some(Ordering::Equal)
        && left.zero_count.to_bits() == right.zero_count.to_bits()
        && left.custom_values == right.custom_values
        && left.positive_spans == right.positive_spans
        && left.negative_spans == right.negative_spans
        && bucket_counts_match(&left.positive_counts, &right.positive_counts)
        && bucket_counts_match(&left.negative_counts, &right.negative_counts)
}

/// `floatBucketsMatch`: bucket counts compared bitwise, as every count is.
fn bucket_counts_match(left: &[f64], right: &[f64]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| left.to_bits() == right.to_bits())
}
