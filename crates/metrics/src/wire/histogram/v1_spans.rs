use super::*;

pub(crate) fn v1_spans(spans: &[pb::v1::BucketSpan]) -> Vec<BucketSpan> {
    spans
        .iter()
        .map(|span| BucketSpan {
            offset: span.offset,
            length: span.length,
        })
        .collect()
}
