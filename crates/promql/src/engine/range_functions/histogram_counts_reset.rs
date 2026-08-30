
pub(crate) fn histogram_counts_reset(previous: &[f64], current: &[f64]) -> bool {
    previous
        .iter()
        .zip(current.iter())
        .any(|(previous, current)| current < previous)
}
