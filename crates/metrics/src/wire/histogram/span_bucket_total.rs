use super::*;

pub(crate) fn span_bucket_total(spans: &[BucketSpan]) -> usize {
    spans.iter().map(|span| span.length as usize).sum()
}
