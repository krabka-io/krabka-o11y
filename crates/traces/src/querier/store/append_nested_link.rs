use super::*;

pub(crate) fn append_nested_link(
    link: Option<&LinkRef>,
    link_trace_id: &mut FixedSizeBinaryBuilder,
    link_span_id: &mut FixedSizeBinaryBuilder,
) -> Result<(), TraceqlError> {
    if let Some(link) = link {
        link_trace_id
            .append_value(link.trace_id)
            .map_err(|err| TraceqlError::Store(err.to_string()))?;
        link_span_id
            .append_value(link.span_id)
            .map_err(|err| TraceqlError::Store(err.to_string()))?;
    } else {
        link_trace_id.append_null();
        link_span_id.append_null();
    }
    Ok(())
}
