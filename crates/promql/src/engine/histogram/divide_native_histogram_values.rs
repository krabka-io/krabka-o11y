use super::NativeHistogram;

pub(crate) fn divide_native_histogram_values(histogram: &mut NativeHistogram, divisor: f64) {
    histogram.count /= divisor;
    histogram.sum /= divisor;
    histogram.zero_count /= divisor;
    for count in histogram
        .positive_counts
        .iter_mut()
        .chain(&mut histogram.negative_counts)
    {
        *count /= divisor;
    }
}
