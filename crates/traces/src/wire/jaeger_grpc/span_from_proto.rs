use super::{
    JaegerSpan, WireError, api_v2, duration_micros, key_value_from_proto, log_from_proto,
    ref_from_proto, span_id_part, timestamp_micros, trace_id_parts,
};

pub(crate) fn span_from_proto(span: api_v2::Span) -> Result<JaegerSpan, WireError> {
    let (trace_id_high, trace_id_low) = trace_id_parts(&span.trace_id)?;
    let span_id = span_id_part(&span.span_id)?;
    Ok(JaegerSpan {
        trace_id_low,
        trace_id_high,
        span_id,
        parent_span_id: 0,
        operation_name: span.operation_name,
        references: span
            .references
            .iter()
            .map(ref_from_proto)
            .collect::<Result<Vec<_>, _>>()?,
        start_time_micros: timestamp_micros(span.start_time.as_ref()),
        duration_micros: duration_micros(span.duration.as_ref()),
        tags: span.tags.iter().map(key_value_from_proto).collect(),
        logs: span.logs.iter().map(log_from_proto).collect(),
    })
}
