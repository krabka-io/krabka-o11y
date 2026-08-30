use super::*;

pub(crate) fn trace_by_id_response_protobuf(
    trace: &TraceSpans,
    max_trace_spans: usize,
) -> Result<Vec<u8>, prost::EncodeError> {
    let response = TraceByIdResponse {
        trace: Some(trace_traces_data(trace, max_trace_spans)),
    };
    let mut bytes = Vec::with_capacity(response.encoded_len());
    response.encode(&mut bytes)?;
    Ok(bytes)
}
