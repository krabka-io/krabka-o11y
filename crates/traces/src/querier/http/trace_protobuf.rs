use super::*;

/// OTLP `TracesData` protobuf, the Tempo v1 `/api/traces/{id}` body.
///
/// Grafana's Tempo datasource decodes it as `tempopb.Trace`, which is
/// wire-identical to `TracesData`. Both are field 1 = repeated
/// `ResourceSpans`.
pub(crate) fn trace_protobuf(
    trace: &TraceSpans,
    max_trace_spans: usize,
) -> Result<Vec<u8>, prost::EncodeError> {
    let data = trace_traces_data(trace, max_trace_spans);
    let mut bytes = Vec::with_capacity(data.encoded_len());
    data.encode(&mut bytes)?;
    Ok(bytes)
}
