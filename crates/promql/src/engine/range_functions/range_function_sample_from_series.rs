use super::{
    ExtendedSelectorModifier, RangeFn, RangeSeries, SampleValue, Time, TimeExt, count_changes,
    count_resets, count_step_transitions, emit_warning, extended_float_range_value,
    extended_histogram_range_value, extrapolated_rate_with_starts, mixed_floats_histograms_warning,
    range_histogram_sample,
};

pub(crate) fn range_function_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    kind: RangeFn,
    modifier: Option<ExtendedSelectorModifier>,
    enable_type_and_unit_labels: bool,
) -> Option<SampleValue> {
    let range_start_ms = range_end_ms.saturating_sub(range.millis_i64());
    // `changes` and `resets` count over the whole plain window, floats and
    // histograms together, so they never meet the float-or-histogram split
    // below. The extended `anchored`/`smoothed` windows keep their own float
    // handling.
    if matches!(kind, RangeFn::Changes | RangeFn::Resets) {
        if modifier == Some(ExtendedSelectorModifier::Anchored) {
            if !series
                .samples
                .iter()
                .any(|(timestamp, _)| *timestamp > range_start_ms && *timestamp <= range_end_ms)
            {
                return None;
            }
            let anchor = series
                .samples
                .iter()
                .rfind(|(timestamp, _)| *timestamp <= range_start_ms)
                .map_or(range_start_ms, |(timestamp, _)| timestamp.saturating_sub(1));
            return count_step_transitions(series, anchor, range_end_ms, kind)
                .map(SampleValue::Float);
        }
        return count_step_transitions(series, range_start_ms, range_end_ms, kind)
            .map(SampleValue::Float);
    }
    let mut timestamps = Vec::new();
    let mut values = Vec::new();
    let mut histograms = Vec::new();
    for (timestamp, value) in &series.samples {
        let in_range = match modifier {
            Some(ExtendedSelectorModifier::Anchored) => *timestamp <= range_end_ms,
            Some(ExtendedSelectorModifier::Smoothed) => true,
            None => *timestamp > range_start_ms && *timestamp <= range_end_ms,
        };
        if !in_range {
            continue;
        }
        match value {
            SampleValue::Float(value) => {
                if !histograms.is_empty() {
                    return warn_mixed_window(series);
                }
                timestamps.push(*timestamp);
                values.push(*value);
            }
            SampleValue::Histogram(histogram) => {
                if !values.is_empty() {
                    return warn_mixed_window(series);
                }
                timestamps.push(*timestamp);
                histograms.push(histogram.clone());
            }
        }
    }

    if let Some(modifier) = modifier {
        let smoothed = modifier == ExtendedSelectorModifier::Smoothed;
        if !histograms.is_empty() {
            let points = timestamps.into_iter().zip(histograms).collect::<Vec<_>>();
            return extended_histogram_range_value(
                &points,
                range_start_ms,
                range_end_ms,
                range,
                kind,
                smoothed,
                series.labels.get("__name__").unwrap_or(""),
            )
            .map(SampleValue::Histogram);
        }
        let value = extended_float_range_value(
            &timestamps,
            &values,
            range_start_ms,
            range_end_ms,
            range,
            kind,
            smoothed,
        )?;
        emit_non_counter_info(series, kind, enable_type_and_unit_labels);
        return Some(SampleValue::Float(value));
    }

    if matches!(kind, RangeFn::Rate | RangeFn::Increase)
        && timestamps.windows(2).any(|pair| {
            let previous = series
                .start_timestamps_ms
                .get(&pair[0])
                .copied()
                .unwrap_or(0);
            let current = series
                .start_timestamps_ms
                .get(&pair[1])
                .copied()
                .unwrap_or(0);
            current != 0 && current < pair[0] && current != previous
        })
    {
        emit_warning(format!(
            "PromQL warning: sample has start time that overlaps with previous sample timestamp for metric {:?}",
            series.labels.get("__name__").unwrap_or("")
        ));
    }
    if !histograms.is_empty() {
        return range_histogram_sample(
            &timestamps,
            &histograms,
            range_start_ms,
            range_end_ms,
            range,
            kind,
            series.labels.get("__name__").unwrap_or(""),
        )
        .map(SampleValue::Histogram);
    }
    let value = match kind {
        RangeFn::Changes => count_changes(&values),
        RangeFn::Resets => count_resets(&values),
        RangeFn::Rate | RangeFn::Increase | RangeFn::Delta => extrapolated_rate_with_starts(
            &timestamps,
            &values,
            &series.start_timestamps_ms,
            range_start_ms,
            range_end_ms,
            range,
            kind,
        ),
    }?;
    emit_non_counter_info(series, kind, enable_type_and_unit_labels);
    Some(SampleValue::Float(value))
}

fn emit_non_counter_info(series: &RangeSeries, kind: RangeFn, enabled: bool) {
    if matches!(kind, RangeFn::Rate | RangeFn::Increase) {
        super::super::annotations::emit_metric_might_not_be_counter_info(&series.labels, enabled);
    }
}

/// Warns that a rate-family window holds both floats and histograms, and drops
/// the series -- `extrapolatedRate` needs one kind or the other.
fn warn_mixed_window(series: &RangeSeries) -> Option<SampleValue> {
    emit_warning(mixed_floats_histograms_warning(
        series.labels.get("__name__").unwrap_or(""),
    ));
    None
}
