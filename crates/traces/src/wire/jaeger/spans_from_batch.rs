use super::*;

pub(crate) fn spans_from_batch(batch: &JaegerBatch) -> Vec<Span> {
    batch
        .spans
        .iter()
        .map(|span| jaeger_span_to_internal(span, &batch.process))
        .collect()
}
