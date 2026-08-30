use super::BucketSpan;

pub(crate) fn compact_histogram_spans(
    spans: &[BucketSpan],
    counts: &[f64],
) -> (Vec<BucketSpan>, Vec<f64>) {
    let mut index = 0;
    let mut count_index = 0;
    let mut buckets = Vec::new();
    for (span_index, span) in spans.iter().enumerate() {
        if span_index == 0 {
            index = span.offset;
        } else {
            index += span.offset;
        }
        for _ in 0..span.length {
            let Some(count) = counts.get(count_index).copied() else {
                break;
            };
            buckets.push((index, count));
            index += 1;
            count_index += 1;
        }
    }
    let Some(first_non_zero) = buckets.iter().position(|(_, count)| *count != 0.0) else {
        return (Vec::new(), Vec::new());
    };
    let last_non_zero = buckets
        .iter()
        .rposition(|(_, count)| *count != 0.0)
        .expect("first non-zero bucket exists");
    let buckets = &buckets[first_non_zero..=last_non_zero];

    let mut compacted_spans = Vec::new();
    let mut compacted_counts = Vec::with_capacity(buckets.len());
    let mut span_start = None;
    let mut previous_index = 0;
    let mut previous_span_end = 0;
    for &(index, count) in buckets {
        if span_start.is_none() {
            span_start = Some(index);
        } else if index != previous_index + 1 {
            let start = span_start.expect("checked is_some");
            compacted_spans.push(BucketSpan {
                offset: start - previous_span_end,
                length: u32::try_from(previous_index - start + 1).unwrap_or(u32::MAX),
            });
            previous_span_end = previous_index + 1;
            span_start = Some(index);
        }
        compacted_counts.push(count);
        previous_index = index;
    }
    if let Some(start) = span_start {
        compacted_spans.push(BucketSpan {
            offset: start - previous_span_end,
            length: u32::try_from(previous_index - start + 1).unwrap_or(u32::MAX),
        });
    }
    (compacted_spans, compacted_counts)
}
