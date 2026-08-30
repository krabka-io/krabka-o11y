/// Returns the value of the sample with the greatest timestamp.
///
/// A tie selects the later element, the same as `max_by_key(timestamp)` over a
/// time-sorted window. This function returns `None` only for an empty window.
pub(crate) fn last_value_by_timestamp(timestamps: &[i64], values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    // The window is time-sorted ascending (SeriesNormalize), so the engine's
    // `max_by_key(timestamp)` is the last element. Fall back to a scan if the
    // timestamps and values disagree in length (defensive; should not happen).
    if timestamps.len() == values.len() {
        let mut best_idx = 0;
        for (idx, ts) in timestamps.iter().enumerate() {
            // `max_by_key` keeps the *last* maximum on ties.
            if *ts >= timestamps[best_idx] {
                best_idx = idx;
            }
        }
        return Some(values[best_idx]);
    }
    values.last().copied()
}
