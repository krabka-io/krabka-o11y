use super::*;

pub(crate) fn histogram_reset_indices(histograms: &[NativeHistogram]) -> Vec<usize> {
    histograms
        .windows(2)
        .enumerate()
        .filter_map(|(index, window)| {
            histogram_reset_between(&window[0], &window[1]).then_some(index + 1)
        })
        .collect()
}
