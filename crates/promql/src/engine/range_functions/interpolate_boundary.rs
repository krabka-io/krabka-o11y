use super::ToPrimitive;

pub(crate) fn interpolate_boundary(
    left_ts: i64,
    left_value: f64,
    right_ts: i64,
    right_value: f64,
    target_ms: i64,
) -> Option<f64> {
    let interval = (right_ts - left_ts).to_f64()?;
    if interval <= 0.0 {
        return None;
    }
    let ratio = (target_ms - left_ts).to_f64()? / interval;
    Some(left_value + (right_value - left_value) * ratio)
}
