use super::{NativeHistogram, native_histograms_are_range_compatible, histogram_reset_indices};

pub(crate) fn count_histogram_resets(histograms: &[NativeHistogram]) -> Option<f64> {
    if histograms.len() < 2
        || !histograms
            .windows(2)
            .all(|window| native_histograms_are_range_compatible(&window[0], &window[1]))
    {
        return None;
    }
    Some(
        histogram_reset_indices(histograms)
            .iter()
            .map(|_| 1.0)
            .sum(),
    )
}
