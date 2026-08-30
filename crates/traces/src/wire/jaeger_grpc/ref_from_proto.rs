use super::{api_v2, JaegerRef, WireError, trace_id_parts, span_id_part};

pub(crate) fn ref_from_proto(reference: &api_v2::SpanRef) -> Result<JaegerRef, WireError> {
    let (trace_id_high, trace_id_low) = trace_id_parts(&reference.trace_id)?;
    Ok(JaegerRef {
        ref_type: reference.ref_type,
        trace_id_low,
        trace_id_high,
        span_id: span_id_part(&reference.span_id)?,
    })
}
