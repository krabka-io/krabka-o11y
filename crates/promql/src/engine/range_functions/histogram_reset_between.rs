use super::*;

pub(crate) fn histogram_reset_between(previous: &NativeHistogram, current: &NativeHistogram) -> bool {
    current.count < previous.count
        || current.sum < previous.sum
        || current.zero_count < previous.zero_count
        || histogram_counts_reset(&previous.positive_counts, &current.positive_counts)
        || histogram_counts_reset(&previous.negative_counts, &current.negative_counts)
}
