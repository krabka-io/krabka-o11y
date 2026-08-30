use super::*;

pub(crate) fn range_function_sample_from_series(
    series: &RangeSeries,
    range_end_ms: i64,
    range: Time,
    kind: RangeFn,
    modifier: Option<ExtendedSelectorModifier>,
) -> Option<SampleValue> {
    let range_start_ms = range_end_ms.saturating_sub(range.millis_i64());
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
                    return None;
                }
                timestamps.push(*timestamp);
                values.push(*value);
            }
            SampleValue::Histogram(histogram) => {
                if !values.is_empty() {
                    return None;
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
        if matches!(kind, RangeFn::Resets) {
            return count_histogram_resets(&histograms).map(SampleValue::Float);
        }
        return range_histogram_sample(
            &timestamps,
            &histograms,
            range_start_ms,
            range_end_ms,
            range,
            kind,
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
