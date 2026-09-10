use super::{
    HistogramExtrapolation, NativeHistogram, RangeFn, ResetHint, Time, compact_histogram_spans,
    emit_warning, extrapolated_histogram_component, extrapolated_histogram_counts,
    histogram_reset_indices, native_histogram_not_counter_warning,
    native_histogram_not_gauge_warning, native_histograms_are_range_compatible,
};

pub(crate) fn range_histogram_sample(
    timestamps: &[i64],
    histograms: &[NativeHistogram],
    range_start_ms: i64,
    range_end_ms: i64,
    range: Time,
    kind: RangeFn,
    metric: &str,
) -> Option<NativeHistogram> {
    if !matches!(kind, RangeFn::Rate | RangeFn::Increase | RangeFn::Delta) || histograms.len() < 2 {
        return None;
    }
    // `histogramRate` says so when it is handed the sample kind the function
    // was not written for: a gauge histogram to `rate`/`increase`, or a counter
    // histogram to `delta`. Both still compute a result.
    if matches!(kind, RangeFn::Delta) {
        let ends_are_gauges = histograms
            .first()
            .is_some_and(|histogram| histogram.reset_hint == ResetHint::Gauge)
            && histograms
                .last()
                .is_some_and(|histogram| histogram.reset_hint == ResetHint::Gauge);
        if !ends_are_gauges {
            emit_warning(native_histogram_not_gauge_warning(metric));
        }
    } else if histograms
        .iter()
        .any(|histogram| histogram.reset_hint == ResetHint::Gauge)
    {
        emit_warning(native_histogram_not_counter_warning(metric));
    }
    let first = histograms.first()?;
    let last = histograms.last()?;
    if !histograms
        .windows(2)
        .all(|window| native_histograms_are_range_compatible(&window[0], &window[1]))
    {
        return None;
    }
    let resets = histogram_reset_indices(histograms);
    let extrapolation = HistogramExtrapolation {
        timestamps,
        reset_indices: &resets,
        range_start_ms,
        range_end_ms,
        range,
        kind,
    };

    let mut out = last.clone();
    out.count = extrapolated_histogram_component(
        &extrapolation,
        &histograms
            .iter()
            .map(|histogram| histogram.count)
            .collect::<Vec<_>>(),
    )?;
    out.sum = extrapolated_histogram_component(
        &extrapolation,
        &histograms
            .iter()
            .map(|histogram| histogram.sum)
            .collect::<Vec<_>>(),
    )?;
    out.zero_count = extrapolated_histogram_component(
        &extrapolation,
        &histograms
            .iter()
            .map(|histogram| histogram.zero_count)
            .collect::<Vec<_>>(),
    )?;
    out.positive_counts = extrapolated_histogram_counts(&extrapolation, histograms, |histogram| {
        &histogram.positive_counts
    })?;
    (out.positive_spans, out.positive_counts) =
        compact_histogram_spans(&out.positive_spans, &out.positive_counts);
    out.negative_counts = extrapolated_histogram_counts(&extrapolation, histograms, |histogram| {
        &histogram.negative_counts
    })?;
    (out.negative_spans, out.negative_counts) =
        compact_histogram_spans(&out.negative_spans, &out.negative_counts);
    if matches!(kind, RangeFn::Delta) || out.is_nhcb() && !resets.is_empty() {
        out.reset_hint = ResetHint::Gauge;
    }
    out.start_timestamp_ms = first.start_timestamp_ms.or(last.start_timestamp_ms);
    Some(out)
}
