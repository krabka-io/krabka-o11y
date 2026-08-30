use super::{RangeFn, Time, TimeExt, boundary_value, counter_corrected_values};

pub(crate) fn smoothed_float_range_value(
    timestamps: &[i64],
    values: &[f64],
    range_start_ms: i64,
    range_end_ms: i64,
    range: Time,
    kind: RangeFn,
) -> Option<f64> {
    if !matches!(kind, RangeFn::Delta | RangeFn::Increase | RangeFn::Rate) {
        return None;
    }
    // Permanent survivor against `&&`: the caller builds both slices from one
    // series, so the lengths always agree, and an empty window returns `None`
    // from `boundary_value` below anyway.
    if timestamps.len() != values.len() || timestamps.is_empty() {
        return None;
    }

    let smoothed_values = if matches!(kind, RangeFn::Increase | RangeFn::Rate) {
        counter_corrected_values(values)?
    } else {
        values.to_vec()
    };
    let start = boundary_value(timestamps, &smoothed_values, range_start_ms)?;
    let end = boundary_value(timestamps, &smoothed_values, range_end_ms)?;
    let mut result = end - start;
    // `< 0.0` is a permanent survivor against `== 0.0` and `<= 0.0`:
    // `counter_corrected_values` returns a non-decreasing series, and
    // interpolating or extrapolating one never puts the start above the end,
    // so `result` is never negative and at zero the clamp is a no-op.
    if matches!(kind, RangeFn::Increase | RangeFn::Rate) && result < 0.0 {
        result = 0.0;
    }
    if kind == RangeFn::Rate {
        let range_seconds = range.secs_f64();
        if range_seconds <= 0.0 {
            return None;
        }
        result /= range_seconds;
    }
    Some(result)
}
