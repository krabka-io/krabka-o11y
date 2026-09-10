use super::{NativeHistogram, ResetHint, remap_custom_counts};

/// Whether `current` restarted the counter that `previous` was measuring.
///
/// This is `FloatHistogram.DetectReset`. A stated hint settles it outright;
/// otherwise a reset is anything that cannot be a monotonic increase: a fallen
/// observation count or zero-bucket count, a move from an exponential schema to
/// custom buckets, a finer schema, a lowered zero threshold, or a bucket that
/// lost population.
///
/// Two custom-bucket histograms whose bounds disagree are compared over the
/// bounds they share, which is the layout their subtraction would reconcile to.
pub(crate) fn native_histogram_detect_reset(
    previous: &NativeHistogram,
    current: &NativeHistogram,
) -> bool {
    match current.reset_hint {
        ResetHint::Yes => return true,
        ResetHint::No => return false,
        ResetHint::Unknown | ResetHint::Gauge => {}
    }
    if current.count < previous.count {
        return true;
    }
    if current.is_nhcb() {
        if !previous.is_nhcb() {
            return true;
        }
        if current.custom_values != previous.custom_values {
            return reconciled_buckets_shrank(previous, current);
        }
    }
    current.schema > previous.schema
        || current.zero_threshold < previous.zero_threshold
        || current.zero_count < previous.zero_count
        || bucket_shrank(&previous.positive_counts, &current.positive_counts)
        || bucket_shrank(&previous.negative_counts, &current.negative_counts)
}

fn bucket_shrank(previous: &[f64], current: &[f64]) -> bool {
    previous
        .iter()
        .zip(current.iter())
        .any(|(previous, current)| current < previous)
}

fn reconciled_buckets_shrank(previous: &NativeHistogram, current: &NativeHistogram) -> bool {
    let previous_values = previous.custom_values.as_deref().unwrap_or_default();
    let current_values = current.custom_values.as_deref().unwrap_or_default();
    let shared = current_values
        .iter()
        .copied()
        .filter(|value| previous_values.contains(value))
        .collect::<Vec<_>>();
    let previous = remap_custom_counts(
        &previous.positive_spans,
        &previous.positive_counts,
        previous_values,
        &shared,
    );
    let current = remap_custom_counts(
        &current.positive_spans,
        &current.positive_counts,
        current_values,
        &shared,
    );
    previous
        .into_iter()
        .any(|(index, count)| current.get(&index).copied().unwrap_or(0.0) < count)
}
