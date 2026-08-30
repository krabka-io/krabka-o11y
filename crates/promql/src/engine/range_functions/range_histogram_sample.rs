use super::*;

pub(crate) fn range_histogram_sample(
    timestamps: &[i64],
    histograms: &[NativeHistogram],
    range_start_ms: i64,
    range_end_ms: i64,
    range: Time,
    kind: RangeFn,
) -> Option<NativeHistogram> {
    if !matches!(kind, RangeFn::Rate | RangeFn::Increase | RangeFn::Delta) || histograms.len() < 2 {
        return None;
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
