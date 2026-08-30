use super::{pb, BucketSpan};

pub(crate) fn v2_spans(spans: &[pb::v2::BucketSpan]) -> Vec<BucketSpan> {
    spans
        .iter()
        .map(|span| BucketSpan {
            offset: span.offset,
            length: span.length,
        })
        .collect()
}
