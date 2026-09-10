use super::{
    ExtendedSelectorModifier, RangeFn, RangeSeries, SampleValue, Time, TimeExt,
    anchored_float_range_value, count_changes, count_resets, count_step_transitions, emit_warning,
    extrapolated_rate, mixed_floats_histograms_warning, range_histogram_sample,
    smoothed_float_range_value,
};

pub(crate) fn range_function_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    kind: RangeFn,
    modifier: Option<ExtendedSelectorModifier>,
) -> Option<SampleValue> {
    let range_start_ms = range_end_ms.saturating_sub(range.millis_i64());
    // `changes` and `resets` count over the whole plain window, floats and
    // histograms together, so they never meet the float-or-histogram split
    // below. The extended `anchored`/`smoothed` windows keep their own float
    // handling.
    if modifier.is_none() && matches!(kind, RangeFn::Changes | RangeFn::Resets) {
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

    if matches!(modifier, Some(ExtendedSelectorModifier::Anchored)) && !values.is_empty() {
        let value = anchored_float_range_value(&timestamps, &values, range_start_ms, range, kind)?;
        return Some(SampleValue::Float(value));
    }
    if matches!(modifier, Some(ExtendedSelectorModifier::Smoothed)) && !values.is_empty() {
        let value = smoothed_float_range_value(
            &timestamps,
            &values,
            range_start_ms,
            range_end_ms,
            range,
            kind,
        )?;
        return Some(SampleValue::Float(value));
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
        RangeFn::Rate | RangeFn::Increase | RangeFn::Delta => extrapolated_rate(
            &timestamps,
            &values,
            range_start_ms,
            range_end_ms,
            range,
            kind,
        ),
    }?;
    Some(SampleValue::Float(value))
}

/// Warns that a rate-family window holds both floats and histograms, and drops
/// the series -- `extrapolatedRate` needs one kind or the other.
fn warn_mixed_window(series: &RangeSeries) -> Option<SampleValue> {
    emit_warning(mixed_floats_histograms_warning(
        series.labels.get("__name__").unwrap_or(""),
    ));
    None
}
