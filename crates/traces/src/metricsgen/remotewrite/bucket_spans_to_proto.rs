use super::{BucketSpan, RemoteWriteBucketSpan};

pub(crate) fn bucket_spans_to_proto(spans: &[BucketSpan]) -> Vec<RemoteWriteBucketSpan> {
    spans
        .iter()
        .map(|span| RemoteWriteBucketSpan {
            offset: span.offset,
            length: span.length,
        })
        .collect()
}
