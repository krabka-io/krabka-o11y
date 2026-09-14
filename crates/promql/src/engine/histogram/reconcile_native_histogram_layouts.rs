use super::{
    BTreeMap, BTreeSet, NativeHistogram, add_compatible_native_histogram,
    compact_spanned_histogram_counts, spanned_histogram_counts,
};

/// Rewrites compatible histograms to their shared coarsest bucket layout.
///
/// Returns false for an exponential/custom mix, which has no common layout.
pub(crate) fn reconcile_native_histogram_layouts(histograms: &mut [NativeHistogram]) -> bool {
    let Some(first) = histograms.first() else {
        return true;
    };
    if histograms
        .iter()
        .any(|histogram| histogram.is_nhcb() != first.is_nhcb())
    {
        return false;
    }

    let mut layout = first.clone();
    for histogram in &histograms[1..] {
        add_compatible_native_histogram(&mut layout, histogram)
            .expect("histogram kinds were checked");
    }
    layout.count = 0.0;
    layout.sum = 0.0;
    layout.zero_count = 0.0;
    layout.positive_counts.fill(0.0);
    layout.negative_counts.fill(0.0);

    for histogram in &mut *histograms {
        let reset_hint = histogram.reset_hint;
        let start_timestamp_ms = histogram.start_timestamp_ms;
        let mut reconciled = layout.clone();
        add_compatible_native_histogram(&mut reconciled, histogram)
            .expect("histogram kinds were checked");
        reconciled.reset_hint = reset_hint;
        reconciled.start_timestamp_ms = start_timestamp_ms;
        *histogram = reconciled;
    }
    align_bucket_counts(histograms, true);
    align_bucket_counts(histograms, false);
    true
}

fn align_bucket_counts(histograms: &mut [NativeHistogram], positive: bool) {
    let indices = histograms
        .iter()
        .flat_map(|histogram| {
            let (spans, counts) = if positive {
                (&histogram.positive_spans, &histogram.positive_counts)
            } else {
                (&histogram.negative_spans, &histogram.negative_counts)
            };
            spanned_histogram_counts(spans, counts).into_keys()
        })
        .collect::<BTreeSet<_>>();
    let spans = compact_spanned_histogram_counts(
        indices
            .iter()
            .map(|index| (*index, 1.0))
            .collect::<BTreeMap<_, _>>(),
    )
    .0;
    for histogram in &mut *histograms {
        let (source_spans, source_counts) = if positive {
            (&histogram.positive_spans, &histogram.positive_counts)
        } else {
            (&histogram.negative_spans, &histogram.negative_counts)
        };
        let counts = spanned_histogram_counts(source_spans, source_counts);
        let counts = indices
            .iter()
            .map(|index| counts.get(index).copied().unwrap_or_default())
            .collect();
        if positive {
            histogram.positive_spans.clone_from(&spans);
            histogram.positive_counts = counts;
        } else {
            histogram.negative_spans.clone_from(&spans);
            histogram.negative_counts = counts;
        }
    }
}
