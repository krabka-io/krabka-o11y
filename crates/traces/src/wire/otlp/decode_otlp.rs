use super::*;

/// Decode OTLP `TracesData` into internal spans.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn decode_otlp(data: &TracesData) -> Result<Vec<Span>, WireError> {
    let mut out = Vec::new();
    for resource_spans in &data.resource_spans {
        let resource_attrs = resource_spans
            .resource
            .as_ref()
            .map(|resource| kvs(&resource.attributes))
            .unwrap_or_default();

        for scope_spans in &resource_spans.scope_spans {
            let scope_name = scope_spans
                .scope
                .as_ref()
                .map(|scope| scope.name.clone())
                .unwrap_or_default();
            let scope_version = scope_spans
                .scope
                .as_ref()
                .map(|scope| scope.version.clone())
                .unwrap_or_default();
            let instrumentation_attrs = scope_spans.scope.as_ref().map_or_else(Vec::new, |scope| {
                kvs(&scope.attributes)
                    .into_iter()
                    .map(|mut attribute| {
                        attribute.key = format!(
                            "{}{}",
                            krabka_traceql::INSTRUMENTATION_ATTR_PREFIX,
                            attribute.key
                        );
                        attribute
                    })
                    .collect::<Vec<_>>()
            });

            for span in &scope_spans.spans {
                let parent_span_id = if span.parent_span_id.is_empty() {
                    None
                } else {
                    Some(fixed8(&span.parent_span_id, "parent_span_id")?)
                };
                let (status, status_message) = status_of(span.status.as_ref());
                let events = span
                    .events
                    .iter()
                    .map(|event| EventRecord {
                        time_unix_nano: i64::try_from(event.time_unix_nano).unwrap_or(i64::MAX),
                        name: event.name.clone(),
                        attrs: kvs(&event.attributes),
                    })
                    .collect();
                let links = span
                    .links
                    .iter()
                    .map(|link| {
                        Ok(LinkRecord {
                            trace_id: fixed16(&link.trace_id, "link.trace_id")?,
                            span_id: fixed8(&link.span_id, "link.span_id")?,
                            attrs: kvs(&link.attributes),
                        })
                    })
                    .collect::<Result<Vec<_>, WireError>>()?;

                let mut span_attrs = kvs(&span.attributes);
                span_attrs.extend(instrumentation_attrs.clone());
                out.push(Span {
                    trace_id: fixed16(&span.trace_id, "trace_id")?,
                    span_id: fixed8(&span.span_id, "span_id")?,
                    parent_span_id,
                    name: span.name.clone(),
                    kind: kind_of(span.kind),
                    start_ns: i64::try_from(span.start_time_unix_nano).unwrap_or(i64::MAX),
                    duration_ns: i64::try_from(
                        span.end_time_unix_nano
                            .saturating_sub(span.start_time_unix_nano),
                    )
                    .unwrap_or(i64::MAX),
                    status,
                    status_message,
                    resource_attrs: resource_attrs.clone(),
                    span_attrs,
                    events,
                    links,
                    instrumentation_scope: scope_name.clone(),
                    instrumentation_version: scope_version.clone(),
                });
            }
        }
    }
    Ok(out)
}
