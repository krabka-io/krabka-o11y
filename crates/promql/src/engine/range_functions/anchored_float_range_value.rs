use super::{RangeFn, Time, TimeExt, count_changes, count_resets, counter_delta};

pub(crate) fn anchored_float_range_value(
    timestamps: &[i64],
    values: &[f64],
    range_start_ms: i64,
    range: Time,
    kind: RangeFn,
) -> Option<f64> {
    let mut selected = Vec::new();
    if matches!(kind, RangeFn::Changes | RangeFn::Resets) {
        let has_after_start = timestamps
            .iter()
            .any(|timestamp| *timestamp > range_start_ms);
        if has_after_start {
            if let Some(index) = timestamps
                .iter()
                .rposition(|timestamp| *timestamp <= range_start_ms)
            {
                selected.push((*timestamps.get(index)?, values.get(index).copied()?));
            }
            // `>` is a permanent survivor against `>=`: a sample sitting
            // exactly on `range_start_ms` is already pushed just above, and
            // this arm only serves `changes` and `resets`, which see no change
            // and no reset between two copies of the same value.
            selected.extend(timestamps.iter().zip(values.iter()).filter_map(
                |(timestamp, value)| (*timestamp > range_start_ms).then_some((*timestamp, *value)),
            ));
        } else if let Some(start_index) = timestamps
            .iter()
            .position(|timestamp| *timestamp == range_start_ms)
        {
            // `<` is a permanent survivor against `<=`: `start_index` is the
            // first timestamp equal to `range_start_ms`, so everything before
            // it is strictly below and the two spellings pick the same sample.
            if let Some(previous_index) = timestamps[..start_index]
                .iter()
                .rposition(|timestamp| *timestamp < range_start_ms)
            {
                selected.push((
                    *timestamps.get(previous_index)?,
                    values.get(previous_index).copied()?,
                ));
            }
            selected.push((
                *timestamps.get(start_index)?,
                values.get(start_index).copied()?,
            ));
        }
    } else {
        if let Some(index) = timestamps
            .iter()
            .rposition(|timestamp| *timestamp <= range_start_ms)
        {
            selected.push((*timestamps.get(index)?, values.get(index).copied()?));
        }
        selected.extend(
            timestamps
                .iter()
                .zip(values.iter())
                .filter_map(|(timestamp, value)| {
                    (*timestamp > range_start_ms).then_some((*timestamp, *value))
                }),
        );
    }
    if selected.is_empty() {
        return None;
    }
    if selected.len() == 1 && selected[0].0 <= range_start_ms {
        return None;
    }
    let selected_values = selected.iter().map(|(_, value)| *value).collect::<Vec<_>>();

    match kind {
        RangeFn::Changes => count_changes(&selected_values),
        RangeFn::Resets => count_resets(&selected_values),
        RangeFn::Delta => Some(selected_values.last()? - selected_values.first()?),
        RangeFn::Increase | RangeFn::Rate => {
            let result = counter_delta(&selected_values)?;
            if kind == RangeFn::Rate {
                let range_seconds = range.secs_f64();
                if range_seconds <= 0.0 {
                    return None;
                }
                Some(result / range_seconds)
            } else {
                let _ = timestamps;
                Some(result)
            }
        }
    }
}
