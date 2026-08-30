use super::NativeHistogram;

pub(crate) fn scale_native_histogram_values(histogram: &mut NativeHistogram, factor: f64) {
    histogram.zero_count *= factor;
    histogram.count *= factor;
    histogram.sum *= factor;
    for count in &mut histogram.positive_counts {
        *count *= factor;
    }
    for count in &mut histogram.negative_counts {
        *count *= factor;
    }
}
