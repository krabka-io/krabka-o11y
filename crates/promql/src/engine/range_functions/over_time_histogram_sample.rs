use super::{
    NativeHistogram, OverTimeFn, ResetHint, add_compatible_native_histogram, emit_info,
    emit_warning, histogram_counter_reset_collision_warning, mismatched_custom_buckets_info,
    scale_native_histogram_values,
};

/// Folds a window of histogram samples into `sum_over_time` or `avg_over_time`.
///
/// A window that mixes histograms stating a counter reset with histograms
/// stating there was none is summing samples that disagree about their own
/// history, and Prometheus warns about the result.
pub(crate) fn over_time_histogram_sample(
    histograms: &[NativeHistogram],
    kind: OverTimeFn,
) -> Option<NativeHistogram> {
    let reset_seen = histograms
        .iter()
        .any(|histogram| histogram.reset_hint == ResetHint::Yes);
    let not_reset_seen = histograms
        .iter()
        .any(|histogram| histogram.reset_hint == ResetHint::No);
    if reset_seen && not_reset_seen {
        emit_warning(histogram_counter_reset_collision_warning("aggregation"));
    }

    let mut out = histograms.first()?.clone();
    let mut reconciled_custom_buckets = false;
    for histogram in &histograms[1..] {
        if out.is_nhcb() && histogram.is_nhcb() && out.custom_values != histogram.custom_values {
            reconciled_custom_buckets = true;
            emit_info(mismatched_custom_buckets_info("aggregation"));
        }
        add_compatible_native_histogram(&mut out, histogram).ok()?;
    }
    if matches!(kind, OverTimeFn::Avg) {
        let count: f64 = histograms.iter().map(|_| 1.0).sum();
        scale_native_histogram_values(&mut out, 1.0 / count);
        if reconciled_custom_buckets {
            out.reset_hint = ResetHint::Gauge;
        }
    }
    Some(out)
}
