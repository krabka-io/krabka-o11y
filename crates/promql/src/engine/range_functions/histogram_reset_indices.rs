use super::{NativeHistogram, histogram_reset_between};

pub(crate) fn histogram_reset_indices(histograms: &[NativeHistogram]) -> Vec<usize> {
    let mut indices = Vec::new();
    for index in 0..histograms.len().saturating_sub(1) {
        if histogram_reset_between(&histograms[index], &histograms[index + 1]) {
            indices.push(index + 1);
        }
    }
    indices
}
