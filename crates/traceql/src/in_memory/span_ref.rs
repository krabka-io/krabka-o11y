use super::{InputSpan, NestedSet, SpanRef};

pub(crate) fn span_ref(span: &InputSpan, nested: &NestedSet) -> SpanRef {
    SpanRef {
        span_id: span.span_id,
        parent_span_id: span.parent_span_id,
        name: span.name.clone(),
        kind: span.kind,
        nested_set_left: nested.left,
        nested_set_right: nested.right,
        nested_set_parent: nested.parent_id,
        start_time_unix_nano: u64::try_from(span.start_unix_nano).unwrap_or(0),
        duration: span.duration,
        status_code: span.status_code,
        status_message: span.status_message.clone(),
        instrumentation_name: span.instrumentation_name.clone(),
        instrumentation_version: span.instrumentation_version.clone(),
        resource_attributes: Vec::new(),
        attributes: span.attrs.clone(),
        events: span.events.clone(),
        links: span.links.clone(),
    }
}
