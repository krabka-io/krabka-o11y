pub(crate) fn heatmap_slot_timestamp(
    start_ms: i64,
    end_ms: i64,
    time_buckets: usize,
    timestamp: i64,
) -> Option<i64> {
    if timestamp < start_ms || timestamp >= end_ms || time_buckets == 0 {
        return None;
    }
    let time_span = i128::from(end_ms - start_ms);
    let raw = i128::from(timestamp - start_ms) * i128::try_from(time_buckets).ok()? / time_span;
    let bucket = i64::try_from(raw).ok()?;
    let step_ms = (end_ms - start_ms) / i64::try_from(time_buckets).ok()?;
    Some(start_ms + (bucket + 1) * step_ms)
}
