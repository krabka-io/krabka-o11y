use super::{
    JaegerBatch, Span, WireError, api_v2, process_from_proto, span_from_proto, spans_from_batch,
};

///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn decode_jaeger_grpc_batch(batch: api_v2::Batch) -> Result<Vec<Span>, WireError> {
    let batch_process = batch
        .process
        .as_ref()
        .map(process_from_proto)
        .unwrap_or_default();
    let mut spans = Vec::with_capacity(batch.spans.len());
    for span in batch.spans {
        let process = span
            .process
            .as_ref()
            .map_or_else(|| batch_process.clone(), process_from_proto);
        spans.extend(spans_from_batch(&JaegerBatch {
            process,
            spans: vec![span_from_proto(span)?],
        }));
    }
    Ok(spans)
}
