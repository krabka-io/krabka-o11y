use super::*;

#[derive(Clone, Copy, Debug)]
pub(crate) struct BucketCount {
    pub(crate) index: i32,
    pub(crate) count: f64,
}

pub(crate) struct SpansAndCounts {
    pub(crate) spans: Vec<BucketSpan>,
    pub(crate) counts: Vec<f64>,
}

pub(crate) fn spans_and_counts(buckets: &[BucketCount]) -> SpansAndCounts {
    let mut spans = Vec::new();
    let mut counts = Vec::new();
    let Some(first_bucket) = buckets.first() else {
        return SpansAndCounts { spans, counts };
    };

    let mut current_offset = first_bucket.index;
    let mut current_len = 1_u32;
    let mut previous_index = first_bucket.index;
    counts.push(first_bucket.count);

    for bucket in buckets.iter().skip(1) {
        if bucket.index == previous_index + 1 {
            current_len += 1;
        } else {
            spans.push(BucketSpan {
                offset: current_offset,
                length: current_len,
            });
            current_offset = bucket.index - previous_index - 1;
            current_len = 1;
        }
        previous_index = bucket.index;
        counts.push(bucket.count);
    }
    spans.push(BucketSpan {
        offset: current_offset,
        length: current_len,
    });

    SpansAndCounts { spans, counts }
}
