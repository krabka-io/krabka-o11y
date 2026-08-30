use super::*;

pub(crate) fn add_histogram_step(
    start: &NativeHistogram,
    step: &NativeHistogram,
    offset: u32,
) -> NativeHistogram {
    let multiplier = f64::from(offset);
    let mut histogram = start.clone();
    histogram.sum += step.sum * multiplier;
    histogram.count += step.count * multiplier;
    histogram.zero_count += step.zero_count * multiplier;
    (histogram.positive_spans, histogram.positive_counts) = add_histogram_counts(
        &start.positive_spans,
        &start.positive_counts,
        &step.positive_spans,
        &step.positive_counts,
        multiplier,
    );
    (histogram.negative_spans, histogram.negative_counts) = add_histogram_counts(
        &start.negative_spans,
        &start.negative_counts,
        &step.negative_spans,
        &step.negative_counts,
        multiplier,
    );
    histogram
}
