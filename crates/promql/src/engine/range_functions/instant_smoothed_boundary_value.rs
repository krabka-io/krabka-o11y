use super::*;

pub(crate) fn instant_smoothed_boundary_value(
    timestamps: &[i64],
    values: &[f64],
    target_ms: i64,
) -> Option<f64> {
    if timestamps.len() != values.len() || timestamps.is_empty() {
        return None;
    }
    if target_ms <= *timestamps.first()? {
        return values.first().copied();
    }
    if target_ms >= *timestamps.last()? {
        return values.last().copied();
    }
    boundary_value(timestamps, values, target_ms)
}
