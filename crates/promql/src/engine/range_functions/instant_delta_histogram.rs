use super::{
    IrateFn, NativeHistogram, ResetHint, add_compatible_native_histogram, compact_histogram_spans,
    emit_info, emit_warning, mismatched_custom_buckets_info, mixed_exponential_custom_warning,
    native_histogram_detect_reset, native_histogram_not_counter_warning,
    native_histogram_not_gauge_warning, scale_native_histogram_values,
};

/// Folds the last two histogram samples of a window into `irate` or `idelta`.
///
/// `instantValue` subtracts the older sample from the newer one and calls the
/// result a gauge. `irate` skips the subtraction across a counter reset and
/// keeps the newer sample whole; `idelta` always subtracts, because it is meant
/// for gauges. Each warns about the sample kind it was not meant for.
///
/// Returns `None` where Prometheus drops the series: an exponential histogram
/// paired with a custom-bucket one, which cannot be subtracted.
pub(crate) fn instant_delta_histogram(
    previous: &NativeHistogram,
    current: &NativeHistogram,
    metric: &str,
    kind: IrateFn,
) -> Option<NativeHistogram> {
    let is_rate = matches!(kind, IrateFn::Irate);
    if is_rate
        && (previous.reset_hint == ResetHint::Gauge || current.reset_hint == ResetHint::Gauge)
    {
        emit_warning(native_histogram_not_counter_warning(metric));
    }
    if !is_rate
        && (previous.reset_hint != ResetHint::Gauge || current.reset_hint != ResetHint::Gauge)
    {
        emit_warning(native_histogram_not_gauge_warning(metric));
    }

    let mut out = current.clone();
    if !is_rate || !native_histogram_detect_reset(previous, current) {
        if previous.is_nhcb() != current.is_nhcb() {
            emit_warning(mixed_exponential_custom_warning(metric));
            return None;
        }
        if current.is_nhcb() && current.custom_values != previous.custom_values {
            emit_info(mismatched_custom_buckets_info("subtraction"));
        }
        let mut negated = previous.clone();
        scale_native_histogram_values(&mut negated, -1.0);
        add_compatible_native_histogram(&mut out, &negated).ok()?;
    }
    out.reset_hint = ResetHint::Gauge;
    (out.positive_spans, out.positive_counts) =
        compact_histogram_spans(&out.positive_spans, &out.positive_counts);
    (out.negative_spans, out.negative_counts) =
        compact_histogram_spans(&out.negative_spans, &out.negative_counts);
    Some(out)
}
