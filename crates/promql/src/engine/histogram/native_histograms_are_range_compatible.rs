use super::*;

pub(crate) fn native_histograms_are_range_compatible(
    left: &NativeHistogram,
    right: &NativeHistogram,
) -> bool {
    left.schema == right.schema
        && left.is_float == right.is_float
        && left.zero_threshold.to_bits() == right.zero_threshold.to_bits()
        && left.custom_values == right.custom_values
        && left.positive_spans == right.positive_spans
        && left.negative_spans == right.negative_spans
        && left.positive_counts.len() == right.positive_counts.len()
        && left.negative_counts.len() == right.negative_counts.len()
}
