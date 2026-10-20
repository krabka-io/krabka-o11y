use std::cmp::Ordering;

use super::{
    BTreeMap, NativeHistogram, ResetHint, detect_reset_bucket_counts, remap_custom_counts,
    zero_count_at_threshold,
};

struct BucketLayout<'a> {
    spans: &'a [krabka_metrics::BucketSpan],
    counts: &'a [f64],
    schema: i8,
}

struct BucketComparison<'a> {
    previous: BucketLayout<'a>,
    current: BucketLayout<'a>,
}

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
    let layouts = [
        BucketComparison {
            previous: BucketLayout {
                spans: &previous.positive_spans,
                counts: &previous.positive_counts,
                schema: previous.schema,
            },
            current: BucketLayout {
                spans: &current.positive_spans,
                counts: &current.positive_counts,
                schema: current.schema,
            },
        },
        BucketComparison {
            previous: BucketLayout {
                spans: &previous.negative_spans,
                counts: &previous.negative_counts,
                schema: previous.schema,
            },
            current: BucketLayout {
                spans: &current.negative_spans,
                counts: &current.negative_counts,
                schema: current.schema,
            },
        },
    ];
    layouts.iter().any(|pair| {
        bucket_shrank(
            &detect_reset_bucket_counts(
                pair.previous.spans,
                pair.previous.counts,
                pair.previous.schema,
                pair.current.schema,
                current.zero_threshold,
            ),
            &detect_reset_bucket_counts(
                pair.current.spans,
                pair.current.counts,
                pair.current.schema,
                pair.current.schema,
                current.zero_threshold,
            ),
        )
    })
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
    let zero_count = if same_float(current.zero_threshold, previous.zero_threshold) {
        super::ZeroCountAtThreshold {
            count: previous.zero_count,
            threshold: current.zero_threshold,
        }
    } else {
        zero_count_at_threshold(previous, current.zero_threshold)
    };
    !same_float(zero_count.threshold, current.zero_threshold)
        || current.zero_count < zero_count.count
}

fn same_float(left: f64, right: f64) -> bool {
    left.partial_cmp(&right) == Some(Ordering::Equal)
}

/// `detectReset`: a bucket the previous histogram populated that the current
/// one either lost outright or holds less in.
fn bucket_shrank(previous: &BTreeMap<i32, f64>, current: &BTreeMap<i32, f64>) -> bool {
    previous.keys().any(|index| {
        let previous_count = previous[index];
        current
            .get(index)
            .map_or(previous_count != 0.0, |current_count| {
                *current_count < previous_count
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
    for index in previous.keys() {
        if current.get(index).copied().unwrap_or(0.0) < previous[index] {
            return true;
        }
    }
    false
}
