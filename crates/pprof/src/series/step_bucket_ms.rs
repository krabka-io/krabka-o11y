use super::*;

/// Returns the epoch-millisecond start of the bucket that holds `ts_ms`.
///
/// The bucket size is the step of the query. The timestamp is an instant, and
/// the bucket start is an instant too, so both stay epoch milliseconds. Only
/// the step is an extent. This function floors with Euclidean division, so a
/// timestamp before the epoch goes into the bucket below it. Euclidean division
/// does not truncate toward zero.
#[must_use]
pub fn step_bucket_ms(ts_ms: i64, step: Time) -> i64 {
    let step_ms = step.millis_i64();
    ts_ms.div_euclid(step_ms) * step_ms
}
