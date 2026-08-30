use super::{AttrValue, EventRef, LinkRef, Result, SpanRef, TraceSpans, TracesData, attrs_from_otlp, fixed_16, fixed_8, time_from_nanos_u64};

pub(crate) fn trace_spans_from_otlp(trace_id: &[u8; 16], data: TracesData) -> Result<TraceSpans> {
    let mut trace = TraceSpans {
        trace_id: *trace_id,
        root_service_name: String::new(),
        root_trace_name: String::new(),
        resource_attributes: Vec::new(),
        spans: Vec::new(),
    };
    for resource_spans in data.resource_spans {
        let resource_attrs = resource_spans
            .resource
            .as_ref()
            .map_or_else(Vec::new, |resource| attrs_from_otlp(&resource.attributes));
        if trace.resource_attributes.is_empty() {
            trace.resource_attributes.clone_from(&resource_attrs);
        }
        if trace.root_service_name.is_empty() {
            trace.root_service_name = resource_attrs
                .iter()
                .find_map(|(key, value)| {
                    (key == "service.name").then(|| match value {
                        AttrValue::Str(value) => Some(value.clone()),
                        _ => None,
                    })?
                })
                .unwrap_or_default();
        }
        for scope_spans in resource_spans.scope_spans {
            let (instrumentation_name, instrumentation_version) = scope_spans
                .scope
                .map_or_else(Default::default, |scope| (scope.name, scope.version));
            for span in scope_spans.spans {
                let span_id = fixed_8(&span.span_id)?;
                let parent_span_id = if span.parent_span_id.is_empty() {
                    None
                } else {
                    Some(fixed_8(&span.parent_span_id)?)
                };
                let duration = time_from_nanos_u64(
                    span.end_time_unix_nano
                        .saturating_sub(span.start_time_unix_nano),
                );
                if trace.root_trace_name.is_empty() && parent_span_id.is_none() {
                    trace.root_trace_name.clone_from(&span.name);
                }
                let status = span.status.unwrap_or_default();
                trace.spans.push(SpanRef {
                    span_id,
                    parent_span_id,
                    name: span.name,
                    kind: span.kind,
                    nested_set_left: 0,
                    nested_set_right: 0,
                    nested_set_parent: 0,
                    start_time_unix_nano: span.start_time_unix_nano,
                    duration,
                    status_code: status.code,
                    status_message: status.message,
                    instrumentation_name: instrumentation_name.clone(),
                    instrumentation_version: instrumentation_version.clone(),
                    resource_attributes: resource_attrs.clone(),
                    attributes: attrs_from_otlp(&span.attributes),
                    events: span
                        .events
                        .into_iter()
                        .map(|event| EventRef {
                            time_since_start: time_from_nanos_u64(
                                event
                                    .time_unix_nano
                                    .saturating_sub(span.start_time_unix_nano),
                            ),
                            name: event.name,
                            attributes: attrs_from_otlp(&event.attributes),
                        })
                        .collect(),
                    links: span
                        .links
                        .into_iter()
                        .map(|link| {
                            Ok(LinkRef {
                                trace_id: fixed_16(&link.trace_id)?,
                                span_id: fixed_8(&link.span_id)?,
                                attributes: attrs_from_otlp(&link.attributes),
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                });
            }
        }
    }
    if trace.root_trace_name.is_empty() {
        trace.root_trace_name = trace
            .spans
            .first()
            .map(|span| span.name.clone())
            .unwrap_or_default();
    }
    Ok(trace)
}
