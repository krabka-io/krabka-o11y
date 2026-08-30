use super::{interpolate_boundary, ToPrimitive};

pub(crate) fn boundary_value(timestamps: &[i64], values: &[f64], target_ms: i64) -> Option<f64> {
    // `||` against `&&` is a permanent mutation survivor. Both callers pair the
    // timestamps with values derived from them, so the lengths always agree and
    // the second arm alone decides -- and both callers have already returned
    // `None` for an empty series before they get here.
    if timestamps.len() != values.len() || timestamps.is_empty() {
        return None;
    }
    if timestamps.len() == 1 {
        return values.first().copied();
    }
    if let Some(index) = timestamps
        .iter()
        .position(|timestamp| *timestamp == target_ms)
    {
        return values.get(index).copied();
    }
    // `>` against `>=` is a permanent mutation survivor: a timestamp equal to
    // the target was already returned by the exact-match search above, so by
    // here no timestamp can equal it and the two spellings select the same one.
    if let Some(after_index) = timestamps
        .iter()
        .position(|timestamp| *timestamp > target_ms)
    {
        if after_index == 0 {
            return values.first().copied();
        }
        return interpolate_boundary(
            timestamps[after_index - 1],
            values[after_index - 1],
            timestamps[after_index],
            values[after_index],
            target_ms,
        );
    }
    let last_index = timestamps.len() - 1;
    let interval = timestamps[last_index].saturating_sub(timestamps[last_index - 1]);
    if target_ms.saturating_sub(timestamps[last_index]).to_f64()? > interval.to_f64()? * 1.1 {
        return values.last().copied();
    }
    interpolate_boundary(
        timestamps[last_index - 1],
        values[last_index - 1],
        timestamps[last_index],
        values[last_index],
        target_ms,
    )
}
