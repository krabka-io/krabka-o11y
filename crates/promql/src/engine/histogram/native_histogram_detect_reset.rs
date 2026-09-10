use std::cmp::Ordering;

use super::{
    BTreeMap, NativeHistogram, ResetHint, detect_reset_bucket_counts, remap_custom_counts,
    zero_count_at_threshold,
};

/// Whether `current` restarted the counter that `previous` was measuring.
///
/// This is `FloatHistogram.DetectReset`. A stated hint settles it outright;
/// otherwise a reset is anything that cannot be a monotonic increase: a fallen
/// observation count or zero-bucket count, a move from an exponential schema to
/// custom buckets, a finer schema, a lowered zero threshold, or a bucket that
/// lost population.
///
/// Bucket populations are compared BY INDEX, over the two histograms resolved
/// to the current histogram's schema and zero threshold, which is what
/// `floatBucketIterator` hands `detectReset` upstream. Two histograms that
/// describe the same buckets through different spans still line up, and a
/// bucket the previous histogram populated and the current one does not carry
/// at all is a reset in its own right.
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
    if current.schema > previous.schema || current.zero_threshold < previous.zero_threshold {
        return true;
    }
    if zero_bucket_shrank(previous, current) {
        return true;
    }
    [
        (
            &previous.positive_spans,
            &previous.positive_counts,
            &current.positive_spans,
            &current.positive_counts,
        ),
        (
            &previous.negative_spans,
            &previous.negative_counts,
            &current.negative_spans,
            &current.negative_counts,
        ),
    ]
    .into_iter()
    .any(
        |(previous_spans, previous_counts, current_spans, current_counts)| {
            bucket_shrank(
                &detect_reset_bucket_counts(
                    previous_spans,
                    previous_counts,
                    previous.schema,
                    current.schema,
                    current.zero_threshold,
                ),
                &detect_reset_bucket_counts(
                    current_spans,
                    current_counts,
                    current.schema,
                    current.schema,
                    current.zero_threshold,
                ),
            )
        },
    )
}

/// Whether the zero bucket lost population once the previous histogram is read
/// at the current histogram's threshold.
///
/// Widening the threshold folds the previous histogram's buckets below it into
/// its zero count, so the counts only compare once that fold has happened. A
/// threshold that lands INSIDE a populated bucket cannot be reached without a
/// reset, and `zero_count_at_threshold` reports exactly that by handing back a
/// threshold it had to push out past the one it was asked for.
fn zero_bucket_shrank(previous: &NativeHistogram, current: &NativeHistogram) -> bool {
    let (previous_zero_count, threshold) =
        if same_float(current.zero_threshold, previous.zero_threshold) {
            (previous.zero_count, current.zero_threshold)
        } else {
            zero_count_at_threshold(previous, current.zero_threshold)
        };
    !same_float(threshold, current.zero_threshold) || current.zero_count < previous_zero_count
}

fn same_float(left: f64, right: f64) -> bool {
    left.partial_cmp(&right) == Some(Ordering::Equal)
}

/// `detectReset`: a bucket the previous histogram populated that the current
/// one either lost outright or holds less in.
fn bucket_shrank(previous: &BTreeMap<i32, f64>, current: &BTreeMap<i32, f64>) -> bool {
    previous.iter().any(|(index, previous_count)| {
        current
            .get(index)
            .map_or(*previous_count != 0.0, |current_count| {
                current_count < previous_count
            })
    })
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
