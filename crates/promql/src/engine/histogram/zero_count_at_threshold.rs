use super::*;

pub(crate) fn zero_count_at_threshold(histogram: &NativeHistogram, mut threshold: f64) -> (f64, f64) {
    loop {
        let mut count = histogram.zero_count;
        let mut expanded = threshold;
        for buckets in [
            spanned_histogram_counts(&histogram.positive_spans, &histogram.positive_counts),
            spanned_histogram_counts(&histogram.negative_spans, &histogram.negative_counts),
        ] {
            for (index, bucket_count) in buckets {
                let lower = standard_histogram_bound(index - 1, histogram.schema);
                if lower >= threshold {
                    break;
                }
                count += bucket_count;
                let upper = standard_histogram_bound(index, histogram.schema);
                if bucket_count != 0.0 && upper > expanded {
                    expanded = upper;
                }
            }
        }
        if expanded.to_bits() == threshold.to_bits() {
            return (count, threshold);
        }
        threshold = expanded;
    }
}
