use super::{
    HistogramExtrapolation, NativeHistogram, RangeFn, ResetHint, Time, ToPrimitive,
    compact_histogram_spans, emit_info, emit_warning, extrapolated_histogram_component,
    extrapolated_histogram_counts, histogram_reset_indices, mismatched_custom_buckets_info,
    mixed_exponential_custom_warning, native_histogram_not_counter_warning,
    native_histogram_not_gauge_warning, native_histograms_are_range_compatible,
    reconcile_native_histogram_layouts,
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
    let reconciled_subtraction = histograms.windows(2).any(|pair| {
        pair[0].is_nhcb() && pair[1].is_nhcb() && pair[0].custom_values != pair[1].custom_values
    });
    let layout_changed = histograms
        .windows(2)
        .any(|pair| !native_histograms_are_range_compatible(&pair[0], &pair[1]));
    let mut histograms = histograms.to_vec();
    let mixed_pair = !reconcile_native_histogram_layouts(&mut histograms);
    if mixed_pair {
        if histograms.len() != 2 || matches!(kind, RangeFn::Delta) {
            emit_warning(mixed_exponential_custom_warning(metric));
            return None;
        }
        let previous_count = histograms[0].count;
        let previous_sum = histograms[0].sum;
        let mut previous = histograms[1].clone();
        previous.count = previous_count;
        previous.sum = previous_sum;
        previous.zero_count = 0.0;
        previous.positive_counts.fill(0.0);
        previous.negative_counts.fill(0.0);
        histograms[0] = previous;
    }
    if reconciled_subtraction {
        emit_info(mismatched_custom_buckets_info("subtraction"));
    }
    let first = histograms.first()?;
    let last = histograms.last()?;
    if !histograms
        .windows(2)
        .all(|window| native_histograms_are_range_compatible(&window[0], &window[1]))
    {
        return None;
    }
    let resets = if mixed_pair {
        vec![1]
    } else {
        histogram_reset_indices(&histograms)
    };
    let duration_to_zero = if resets.is_empty() || mixed_pair {
        let count_delta = if mixed_pair {
            last.count
        } else {
            last.count - first.count
        };
        let sampled_interval = (timestamps.last()? - timestamps.first()?).to_f64()? / 1000.0;
        (count_delta > 0.0 && first.count >= 0.0)
            .then(|| sampled_interval * (first.count / count_delta))
    } else {
        None
    };
    let extrapolation = HistogramExtrapolation {
        timestamps,
        reset_indices: &resets,
        range_start_ms,
        range_end_ms,
        range,
        kind,
        duration_to_zero,
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
    out.positive_counts =
        extrapolated_histogram_counts(&extrapolation, &histograms, |histogram| {
            &histogram.positive_counts
        })?;
    (out.positive_spans, out.positive_counts) =
        compact_histogram_spans(&out.positive_spans, &out.positive_counts);
    out.negative_counts =
        extrapolated_histogram_counts(&extrapolation, &histograms, |histogram| {
            &histogram.negative_counts
        })?;
    (out.negative_spans, out.negative_counts) =
        compact_histogram_spans(&out.negative_spans, &out.negative_counts);
    if matches!(kind, RangeFn::Delta)
        || extrapolation.duration_to_zero.is_some()
        || layout_changed
        || out.is_nhcb() && !resets.is_empty()
    {
        out.reset_hint = ResetHint::Gauge;
    }
    out.start_timestamp_ms = first.start_timestamp_ms.or(last.start_timestamp_ms);
    Some(out)
}
