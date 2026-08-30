use super::{BTreeMap, BucketSpan};

pub(crate) fn compact_spanned_histogram_counts(
    buckets: BTreeMap<i32, f64>,
) -> (Vec<BucketSpan>, Vec<f64>) {
    let buckets = buckets
        .into_iter()
        .filter(|(_, count)| *count != 0.0)
        .collect::<Vec<_>>();
    let mut spans = Vec::new();
    let mut counts = Vec::with_capacity(buckets.len());
    let mut span_start = None;
    let mut previous_index = 0_i32;
    let mut previous_span_end = 0_i32;
    for (index, count) in buckets {
        if span_start.is_none() {
            span_start = Some(index);
        } else if index != previous_index + 1 {
            let start = span_start.expect("checked is_some");
            spans.push(BucketSpan {
                offset: start - previous_span_end,
                length: u32::try_from(previous_index - start + 1).unwrap_or(u32::MAX),
            });
            previous_span_end = previous_index + 1;
            span_start = Some(index);
        }
        counts.push(count);
        previous_index = index;
    }
    if let Some(start) = span_start {
        spans.push(BucketSpan {
            offset: start - previous_span_end,
            length: u32::try_from(previous_index - start + 1).unwrap_or(u32::MAX),
        });
    }
    (spans, counts)
}
