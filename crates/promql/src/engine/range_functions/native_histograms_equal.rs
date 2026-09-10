use super::NativeHistogram;

/// Whether two native histograms are the same observation.
///
/// This is `FloatHistogram.Equals`, which `changes` uses to decide whether one
/// histogram sample differs from the one before it. The counter-reset hint is
/// not part of the comparison: it says where the sample sat in a chunk, not
/// what it observed.
pub(crate) fn native_histograms_equal(left: &NativeHistogram, right: &NativeHistogram) -> bool {
    left.schema == right.schema
        && left.count.to_bits() == right.count.to_bits()
        && left.sum.to_bits() == right.sum.to_bits()
        && left.zero_threshold.to_bits() == right.zero_threshold.to_bits()
        && left.zero_count.to_bits() == right.zero_count.to_bits()
        && left.custom_values == right.custom_values
        && left.positive_spans == right.positive_spans
        && left.negative_spans == right.negative_spans
        && left.positive_counts == right.positive_counts
        && left.negative_counts == right.negative_counts
}
