use super::BucketSpan;
use crate::engine::for_each_spanned_histogram_count;

pub(crate) struct CompactedHistogramSpans {
    pub spans: Vec<BucketSpan>,
    pub counts: Vec<f64>,
}

struct BucketCount {
    index: i32,
    count: f64,
}

pub(crate) fn compact_histogram_spans(
    spans: &[BucketSpan],
    counts: &[f64],
) -> CompactedHistogramSpans {
    let mut buckets = Vec::new();
    for_each_spanned_histogram_count(spans, counts, |index, count| {
        buckets.push(BucketCount { index, count });
    });
    let Some(first_non_zero) = buckets.iter().position(|bucket| bucket.count != 0.0) else {
        return CompactedHistogramSpans {
            spans: Vec::new(),
            counts: Vec::new(),
        };
    };
    let last_non_zero = buckets
        .iter()
        .rposition(|bucket| bucket.count != 0.0)
        .expect("first non-zero bucket exists");
    let buckets = &buckets[first_non_zero..=last_non_zero];

    let mut compacted_spans = Vec::new();
    let mut compacted_counts = Vec::with_capacity(buckets.len());
    let mut span_start = None;
    let mut previous_index = 0;
    let mut previous_span_end = 0;
    for bucket in buckets {
        let index = bucket.index;
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
        compacted_counts.push(bucket.count);
        previous_index = index;
    }
    if let Some(start) = span_start {
        compacted_spans.push(BucketSpan {
            offset: start - previous_span_end,
            length: u32::try_from(previous_index - start + 1).unwrap_or(u32::MAX),
        });
    }
    CompactedHistogramSpans {
        spans: compacted_spans,
        counts: compacted_counts,
    }
}
