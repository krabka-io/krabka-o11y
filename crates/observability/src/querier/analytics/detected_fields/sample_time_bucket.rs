pub(crate) fn sample_time_bucket(sample_time: i64, start: i64, step: i64) -> i64 {
    if sample_time <= start {
        return start;
    }
    let offset = sample_time - start;
    start + (offset / step) * step
}
