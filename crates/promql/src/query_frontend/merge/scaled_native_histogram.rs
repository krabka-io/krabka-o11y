use super::NativeHistogram;

pub(crate) fn scaled_native_histogram(histogram: &NativeHistogram, factor: f64) -> NativeHistogram {
    let mut out = histogram.clone();
    out.zero_count *= factor;
    out.count *= factor;
    out.sum *= factor;
    for count in &mut out.positive_counts {
        *count *= factor;
    }
    for count in &mut out.negative_counts {
        *count *= factor;
    }
    out
}
