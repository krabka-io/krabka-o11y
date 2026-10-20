use super::*;

pub(crate) fn attr_values_json(values: &[&AttrValue]) -> Value {
    if let [value] = values {
        return attr_value_json(value);
    }
    json!({
        "arrayValue": {
            "values": values.iter().map(|value| attr_value_json(value)).collect::<Vec<_>>(),
        }
    })
}

pub(crate) fn attrs_json(attrs: &[Attribute]) -> Value {
    Value::Array(
        group_attrs(attrs)
            .into_iter()
            .map(|group| {
                json!({
                    "key": group.key,
                    "value": attr_values_json(&group.values),
                })
            })
            .collect(),
    )
}

pub(crate) const EVENT_TAGS: &[&str] = &["event:name", "event:timeSinceStart"];

/// An event's absolute instant: the span's start coordinate advanced by the
/// event's offset into the span.
pub(crate) fn event_unix_nano(span: &SpanRef, event: &krabka_traceql::EventRef) -> u64 {
    span.start_time_unix_nano
        .saturating_add(u64::try_from(event.time_since_start.nanos_i64()).unwrap_or(0))
}

pub(crate) fn events_json(span: &SpanRef) -> Value {
    Value::Array(
        span.events
            .iter()
            .map(|event| {
                json!({
                    "timeUnixNano": event_unix_nano(span, event).to_string(),
                    "name": event.name,
                    "attributes": attrs_json(&event.attributes),
                })
            })
            .collect(),
    )
}

pub(crate) fn instrumentation_attributes(span: &SpanRef) -> Vec<Attribute> {
    span.attributes
        .iter()
        .filter_map(|attribute| {
            attribute
                .key
                .strip_prefix(krabka_traceql::INSTRUMENTATION_ATTR_PREFIX)
                .map(|key| Attribute::new(key, attribute.value.clone()))
        })
        .collect()
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct InstrumentationKey {
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) attributes: Vec<krabka_traceql::Attribute>,
}

pub(crate) fn instrumentation_scope_json(
    name: &str,
    version: &str,
    attributes: &[krabka_traceql::Attribute],
) -> Value {
    let mut scope = Map::new();
    if !name.is_empty() {
        scope.insert("name".into(), json!(name));
    }
    if !version.is_empty() {
        scope.insert("version".into(), json!(version));
    }
    if !attributes.is_empty() {
        scope.insert("attributes".into(), attrs_json(attributes));
    }
    Value::Object(scope)
}

pub(crate) const INSTRUMENTATION_TAGS: &[&str] =
    &["instrumentation:name", "instrumentation:version"];

pub(crate) const LINK_TAGS: &[&str] = &["link:spanID", "link:traceID"];

pub(crate) fn links_json(links: &[krabka_traceql::LinkRef]) -> Value {
    Value::Array(
        links
            .iter()
            .map(|link| {
                json!({
                    "traceId": base64(link.trace_id),
                    "spanId": base64(link.span_id),
                    "attributes": attrs_json(&link.attributes),
                })
            })
            .collect(),
    )
}

pub(crate) fn otlp_attrs(attrs: &[Attribute]) -> Vec<OtlpKeyValue> {
    group_attrs(attrs)
        .into_iter()
        .map(|group| OtlpKeyValue {
            key: group.key.to_string(),
            value: Some(otlp_values(&group.values)),
            ..OtlpKeyValue::default()
        })
        .collect()
}

pub(crate) fn otlp_event(span: &SpanRef, event: &krabka_traceql::EventRef) -> OtlpEvent {
    OtlpEvent {
        time_unix_nano: event_unix_nano(span, event),
        name: event.name.clone(),
        attributes: otlp_attrs(&event.attributes),
        ..OtlpEvent::default()
    }
}

pub(crate) fn otlp_link(link: &krabka_traceql::LinkRef) -> OtlpLink {
    OtlpLink {
        trace_id: link.trace_id.to_vec(),
        span_id: link.span_id.to_vec(),
        attributes: otlp_attrs(&link.attributes),
        ..OtlpLink::default()
    }
}

pub(crate) fn otlp_scope_spans(
    trace_id: [u8; 16],
    input_spans: Vec<&SpanRef>,
) -> Vec<OtlpScopeSpans> {
    let groups = group_by_instrumentation(input_spans);
    groups
        .into_iter()
        .map(|group| OtlpScopeSpans {
            scope: (!group.key.name.is_empty()
                || !group.key.version.is_empty()
                || !group.key.attributes.is_empty())
            .then_some(InstrumentationScope {
                name: group.key.name,
                version: group.key.version,
                attributes: otlp_attrs(&group.key.attributes),
                ..InstrumentationScope::default()
            }),
            spans: group
                .spans
                .into_iter()
                .map(|span| otlp_span(trace_id, span))
                .collect(),
            ..OtlpScopeSpans::default()
        })
        .collect()
}

pub(crate) fn otlp_span(trace_id: [u8; 16], span: &SpanRef) -> OtlpSpan {
    OtlpSpan {
        trace_id: trace_id.to_vec(),
        span_id: span.span_id.to_vec(),
        parent_span_id: span
            .parent_span_id
            .map_or_else(Default::default, |parent| parent.to_vec()),
        name: span.name.clone(),
        kind: span.kind,
        start_time_unix_nano: span.start_time_unix_nano,
        end_time_unix_nano: span_end_unix_nano(span),
        attributes: otlp_attrs(&span_attributes(span)),
        events: span
            .events
            .iter()
            .map(|event| otlp_event(span, event))
            .collect(),
        links: span.links.iter().map(otlp_link).collect(),
        status: Some(otlp_status(span.status_code, &span.status_message)),
        ..OtlpSpan::default()
    }
}

pub(crate) fn otlp_status(code: i32, message: &str) -> OtlpStatus {
    // Tempo constructs a Status for every span, and Grafana's Tempo backend
    // dereferences `span.Status` when transforming the protobuf trace
    // (trace_transform.go) — an absent/nil status is a nil pointer dereference
    // that 500s the trace view. STATUS_CODE_UNSET (0) is a valid, present status,
    // so this is emitted unconditionally (wrapped in `Some` at the call site).
    OtlpStatus {
        code,
        message: message.to_string(),
    }
}

pub(crate) type OtlpTracesData = opentelemetry_proto::tonic::trace::v1::TracesData;

pub(crate) fn otlp_values(values: &[&AttrValue]) -> OtlpAnyValue {
    if let [value] = values {
        return otlp_value(value);
    }
    OtlpAnyValue {
        value: Some(OtlpValue::ArrayValue(OtlpArrayValue {
            values: values.iter().map(|value| otlp_value(value)).collect(),
        })),
    }
}

pub(crate) type ResourceAttrs = Vec<krabka_traceql::Attribute>;

pub(crate) struct ResourceSpanGroup<'a> {
    pub(crate) attributes: ResourceAttrs,
    pub(crate) spans: Vec<&'a SpanRef>,
}

pub(crate) fn resource_span_groups(
    trace: &TraceSpans,
    returned_spans: usize,
) -> Vec<ResourceSpanGroup<'_>> {
    let mut groups: Vec<ResourceSpanGroup<'_>> = Vec::new();
    for span in trace.spans.iter().take(returned_spans) {
        let attrs = span_resource_attributes(trace, span);
        if let Some(group) = groups.iter_mut().find(|group| group.attributes == attrs) {
            group.spans.push(span);
        } else {
            groups.push(ResourceSpanGroup {
                attributes: attrs,
                spans: vec![span],
            });
        }
    }
    groups
}

pub(crate) fn scope_spans_json(trace_id: [u8; 16], input_spans: Vec<&SpanRef>) -> Value {
    let groups = group_by_instrumentation(input_spans);
    Value::Array(
        groups
            .into_iter()
            .map(|group| {
                json!({
                    "scope": instrumentation_scope_json(&group.key.name, &group.key.version, &group.key.attributes),
                    "spans": group.spans
                        .into_iter()
                        .map(|span| trace_span_json(trace_id, span))
                        .collect::<Vec<_>>(),
                })
            })
            .collect(),
    )
}

pub(crate) fn span_attributes(span: &SpanRef) -> Vec<krabka_traceql::Attribute> {
    span.attributes
        .iter()
        .filter(|attribute| {
            !attribute
                .key
                .starts_with(krabka_traceql::INSTRUMENTATION_ATTR_PREFIX)
        })
        .cloned()
        .collect()
}

/// A span's end instant: its start coordinate advanced by its duration.
pub(crate) fn span_end_unix_nano(span: &SpanRef) -> u64 {
    span.start_time_unix_nano
        .saturating_add(u64::try_from(span.duration.nanos_i64()).unwrap_or(0))
}

pub(crate) fn span_kind_json(kind: i32) -> Option<&'static str> {
    match kind {
        1 => Some("SPAN_KIND_INTERNAL"),
        2 => Some("SPAN_KIND_SERVER"),
        3 => Some("SPAN_KIND_CLIENT"),
        4 => Some("SPAN_KIND_PRODUCER"),
        5 => Some("SPAN_KIND_CONSUMER"),
        _ => None,
    }
}

pub(crate) fn span_resource_attributes(
    trace: &TraceSpans,
    span: &SpanRef,
) -> Vec<krabka_traceql::Attribute> {
    if span.resource_attributes.is_empty() {
        trace_resource_attributes(trace)
    } else {
        dedup_attrs(&span.resource_attributes, "")
    }
}

pub(crate) fn span_status_json(code: i32, message: &str) -> Value {
    // Always emit a status object so the field is never missing (Tempo always
    // sets a Status; Grafana dereferences it). STATUS_CODE_UNSET (0) is the
    // protojson default and is omitted (rendered as `{}`); OK/ERROR are explicit.
    let mut status = Map::new();
    match code {
        1 => {
            status.insert("code".into(), json!("STATUS_CODE_OK"));
        }
        2 => {
            status.insert("code".into(), json!("STATUS_CODE_ERROR"));
        }
        _ => {}
    }
    if !message.is_empty() {
        status.insert("message".into(), json!(message));
    }
    Value::Object(status)
}

/// Tempo `TraceByIDResponse`, which is
/// `message TraceByIDResponse { Trace trace = 1; }`. It is the
/// `/api/v2/traces/{id}` protobuf body.
///
/// Grafana's Tempo datasource decodes the v2 trace-by-id response into this
/// message. The inner `Trace` is wire-identical to OTLP `TracesData`, so this
/// type models the field as `TracesData`.
#[derive(Clone, PartialEq, ::prost::Message)]
pub(crate) struct TraceByIdResponse {
    #[prost(message, optional, tag = "1")]
    pub(crate) trace: Option<OtlpTracesData>,
}

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

pub(crate) fn trace_duration(trace: &TraceSpans) -> Option<Time> {
    let start = trace
        .spans
        .iter()
        .map(|span| span.start_time_unix_nano)
        .min()?;
    let end = trace.spans.iter().map(span_end_unix_nano).max()?;
    Some(Time::from_nanos(
        i64::try_from(end.saturating_sub(start)).unwrap_or(i64::MAX),
    ))
}

pub(crate) fn trace_json(trace: &TraceSpans, max_trace_spans: usize) -> Value {
    let total_spans = trace.spans.len();
    let returned_spans = total_spans.min(max_trace_spans);
    let status = if returned_spans < total_spans {
        "PARTIAL"
    } else {
        "COMPLETE"
    };
    let message = if returned_spans < total_spans {
        format!("trace truncated after {returned_spans} spans")
    } else {
        String::new()
    };

    json!({
        "trace": {
            "resourceSpans": resource_span_groups(trace, returned_spans)
                .into_iter()
                .map(|group| {
                    json!({
                        "resource": {
                            "attributes": attrs_json(&group.attributes),
                        },
                        "scopeSpans": scope_spans_json(trace.trace_id, group.spans),
                    })
                })
                .collect::<Vec<_>>(),
        },
        "status": status,
        "message": message,
    })
}

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

pub(crate) fn trace_resource_attributes(trace: &TraceSpans) -> Vec<krabka_traceql::Attribute> {
    dedup_attrs(&trace.resource_attributes, trace.root_service_name.as_str())
}

pub(crate) fn trace_span_json(trace_id: [u8; 16], span: &SpanRef) -> Value {
    let mut obj = Map::new();
    obj.insert("traceId".into(), json!(base64(trace_id)));
    obj.insert("spanId".into(), json!(base64(span.span_id)));
    if let Some(parent_span_id) = span.parent_span_id {
        obj.insert("parentSpanId".into(), json!(base64(parent_span_id)));
    }
    obj.insert("name".into(), json!(span.name));
    if let Some(kind) = span_kind_json(span.kind) {
        obj.insert("kind".into(), json!(kind));
    }
    obj.insert(
        "startTimeUnixNano".into(),
        json!(span.start_time_unix_nano.to_string()),
    );
    obj.insert(
        "endTimeUnixNano".into(),
        json!(span_end_unix_nano(span).to_string()),
    );
    obj.insert(
        "status".into(),
        span_status_json(span.status_code, &span.status_message),
    );
    obj.insert("attributes".into(), attrs_json(&span_attributes(span)));
    if !span.events.is_empty() {
        obj.insert("events".into(), events_json(span));
    }
    if !span.links.is_empty() {
        obj.insert("links".into(), links_json(&span.links));
    }
    Value::Object(obj)
}

pub(crate) fn trace_traces_data(trace: &TraceSpans, max_trace_spans: usize) -> OtlpTracesData {
    OtlpTracesData {
        resource_spans: resource_span_groups(trace, trace.spans.len().min(max_trace_spans))
            .into_iter()
            .map(|group| OtlpResourceSpans {
                resource: Some(OtlpResource {
                    attributes: otlp_attrs(&group.attributes),
                    ..OtlpResource::default()
                }),
                scope_spans: otlp_scope_spans(trace.trace_id, group.spans),
                ..OtlpResourceSpans::default()
            })
            .collect(),
    }
}

pub(crate) fn traceql_query_error_response(err: &TraceqlError) -> Response {
    let status = if matches!(
        &err,
        TraceqlError::Parse(_) | TraceqlError::Plan(_) | TraceqlError::Unsupported(_)
    ) {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    super::status_text_response(status, err.to_string())
}

pub(crate) fn traceql_tag_field(key: &str) -> String {
    if key.contains(':') {
        key.to_string()
    } else {
        format!(".{}", key.strip_prefix('.').unwrap_or(key))
    }
}

pub(crate) fn typed_traceql_value(value: &TraceqlValue) -> Option<TypedValue> {
    let typed = match value {
        TraceqlValue::Str(value) => TypedValue {
            type_: "string".into(),
            value: value.clone(),
        },
        TraceqlValue::Int(value) | TraceqlValue::Duration(value) => TypedValue {
            type_: "int".into(),
            value: value.to_string(),
        },
        TraceqlValue::Float(value) if value.is_finite() => TypedValue {
            type_: "float".into(),
            value: value.to_string(),
        },
        TraceqlValue::Bool(value) => TypedValue {
            type_: "bool".into(),
            value: value.to_string(),
        },
        TraceqlValue::Array(_) | TraceqlValue::Float(_) | TraceqlValue::Nil => return None,
    };
    Some(typed)
}
