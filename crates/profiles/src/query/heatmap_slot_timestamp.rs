pub(crate) fn heatmap_slot_timestamp(
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    timestamp: i64,
) -> Option<i64> {
    if start_ms >= end_ms
        || step_ms <= 0
        || timestamp < start_ms.saturating_sub(step_ms)
        || timestamp > end_ms
    {
        return None;
    }
    let delta = (i128::from(timestamp) - i128::from(start_ms)).max(0);
    let rounded = i128::from(start_ms)
        + (delta + i128::from(step_ms) - 1) / i128::from(step_ms) * i128::from(step_ms);
    i64::try_from(rounded).ok()
}
