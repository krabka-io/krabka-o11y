pub(crate) fn histogram_counts_reset(previous: &[f64], current: &[f64]) -> bool {
    let shared_len = previous.len().min(current.len());
    for index in 0..shared_len {
        if current[index] < previous[index] {
            return true;
        }
    }
    false
}
