use std::collections::BTreeMap;

use super::BucketSpan;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SpannedHistogramCounts {
    pub spans: Vec<BucketSpan>,
    pub counts: Vec<f64>,
}

/// Run-length-encodes the non-zero buckets of a sparse `index -> count` map
/// into native-histogram spans and their dense counts.
///
/// Zero counts are dropped, and a run breaks wherever bucket indexes stop
/// being consecutive.
#[must_use]
pub fn compact_spanned_histogram_counts(buckets: BTreeMap<i32, f64>) -> SpannedHistogramCounts {
    let buckets = buckets
        .into_iter()
        .filter(|entry| entry.1 != 0.0)
        .collect::<Vec<_>>();
    let mut spans = Vec::new();
    let mut counts = Vec::with_capacity(buckets.len());
    let mut span_start = None;
    let mut previous_index = 0_i32;
    let mut previous_span_end = 0_i32;
    for bucket in buckets {
        let index = bucket.0;
        let count = bucket.1;
        match span_start {
            None => span_start = Some(index),
            Some(start) if index != previous_index + 1 => {
                spans.push(BucketSpan {
                    offset: start - previous_span_end,
                    length: u32::try_from(previous_index - start + 1).unwrap_or(u32::MAX),
                });
                previous_span_end = previous_index + 1;
                span_start = Some(index);
            }
            Some(_) => {}
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
    SpannedHistogramCounts { spans, counts }
}
