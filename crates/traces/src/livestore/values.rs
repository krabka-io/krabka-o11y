use std::borrow::Borrow;

use krabka_units::convert::TimeExt as _;

use super::*;

pub(crate) const INTRINSIC_TAGS: &[&str] = &[
    "span:childCount",
    "span:duration",
    "span:id",
    "span:kind",
    "span:name",
    "span:Parent",
    "span:parentID",
    "span:nestedSetLeft",
    "span:nestedSetParent",
    "span:nestedSetRight",
    "span:status",
    "span:statusMessage",
    "trace:duration",
    "trace:id",
    "trace:rootName",
    "trace:rootService",
];

pub(crate) fn collect_event_values(
    span: &Span,
    tag: &str,
    values: &mut BTreeSet<krabka_traceql::TypedValue>,
) {
    for event in &span.events {
        match tag {
            "event:name" => {
                values.insert(krabka_traceql::TypedValue::new(
                    "string",
                    event.name.clone(),
                ));
            }
            "event:timeSinceStart" => {
                values.insert(krabka_traceql::TypedValue::new(
                    "duration",
                    event
                        .time_unix_nano
                        .saturating_sub(span.start_ns)
                        .to_string(),
                ));
            }
            _ => {
                values.extend(
                    event
                        .attrs
                        .iter()
                        .filter(|attr| attr.key == tag)
                        .flat_map(|attr| typed_value_parts(&attr.value)),
                );
            }
        }
    }
}

pub(crate) fn collect_link_values(
    span: &Span,
    tag: &str,
    values: &mut BTreeSet<krabka_traceql::TypedValue>,
) {
    for link in &span.links {
        match tag {
            "link:traceID" => {
                values.insert(krabka_traceql::TypedValue::new(
                    "string",
                    bytes_to_hex(&link.trace_id),
                ));
            }
            "link:spanID" => {
                values.insert(krabka_traceql::TypedValue::new(
                    "string",
                    bytes_to_hex(&link.span_id),
                ));
            }
            _ => {
                values.extend(
                    link.attrs
                        .iter()
                        .filter(|attr| attr.key == tag)
                        .flat_map(|attr| typed_value_parts(&attr.value)),
                );
            }
        }
    }
}

pub(crate) fn collect_span_intrinsic_value(
    span: &Span,
    tag: &str,
    values: &mut BTreeSet<krabka_traceql::TypedValue>,
) {
    match tag {
        "span:duration" => {
            values.insert(krabka_traceql::TypedValue::new(
                "duration",
                span.duration_ns.to_string(),
            ));
        }
        "span:id" => {
            values.insert(krabka_traceql::TypedValue::new(
                "string",
                bytes_to_hex(&span.span_id),
            ));
        }
        "span:kind" => {
            values.insert(krabka_traceql::TypedValue::new(
                "int",
                span.kind.as_i32().to_string(),
            ));
        }
        "span:name" => {
            values.insert(krabka_traceql::TypedValue::new("string", span.name.clone()));
        }
        "span:parentID" => {
            if let Some(parent_id) = span.parent_span_id {
                values.insert(krabka_traceql::TypedValue::new(
                    "string",
                    bytes_to_hex(&parent_id),
                ));
            }
        }
        "span:status" => {
            values.insert(krabka_traceql::TypedValue::new(
                "int",
                span.status.as_i32().to_string(),
            ));
        }
        "span:statusMessage" => {
            if !span.status_message.is_empty() {
                values.insert(krabka_traceql::TypedValue::new(
                    "string",
                    span.status_message.clone(),
                ));
            }
        }
        "trace:id" => {
            values.insert(krabka_traceql::TypedValue::new(
                "string",
                bytes_to_hex(&span.trace_id),
            ));
        }
        _ => {}
    }
}

pub(crate) fn collect_trace_intrinsic_values(
    spans: &[&Span],
    tag: &str,
    values: &mut BTreeSet<krabka_traceql::TypedValue>,
) {
    if spans.is_empty() {
        return;
    }
    match tag {
        "trace:duration" => {
            let bounds = crate::trace_time_bounds(spans.iter().map(|span| crate::SpanTiming {
                start_ns: span.start_ns,
                duration_ns: span.duration_ns,
            }));
            values.insert(krabka_traceql::TypedValue::new(
                "duration",
                bounds.end_ns.saturating_sub(bounds.start_ns).to_string(),
            ));
        }
        "trace:rootName" => {
            if let Some(root) = root_span(spans) {
                values.insert(krabka_traceql::TypedValue::new("string", root.name.clone()));
            }
        }
        "trace:rootService" => {
            if let Some(root) = root_span(spans)
                && let Some(service) = attr_string(&root.resource_attrs, "service.name")
            {
                values.insert(krabka_traceql::TypedValue::new("string", service));
            }
        }
        _ => {}
    }
}

pub(crate) fn event_ref(span: &Span, event: &EventRecord) -> krabka_traceql::EventRef {
    krabka_traceql::EventRef {
        time_since_start: Time::from_nanos(event.time_unix_nano.saturating_sub(span.start_ns)),
        name: event.name.clone(),
        attributes: traceql_attributes(&event.attrs),
    }
}

pub(crate) fn order_spans(spans: &mut [impl Borrow<Span>]) {
    spans.sort_by(|left, right| {
        let left = left.borrow();
        let right = right.borrow();
        left.start_ns
            .cmp(&right.start_ns)
            .then_with(|| left.span_id.cmp(&right.span_id))
    });
}

pub(crate) fn span_ref(span: &Span, nested: nested_set::NestedSet) -> krabka_traceql::SpanRef {
    krabka_traceql::SpanRef {
        span_id: span.span_id,
        parent_span_id: span.parent_span_id,
        name: span.name.clone(),
        kind: span.kind.as_i32(),
        nested_set_left: nested.left,
        nested_set_right: nested.right,
        nested_set_parent: nested.parent_id,
        start_time_unix_nano: non_negative_u64(span.start_ns),
        duration: Time::from_nanos(span.duration_ns),
        status_code: span.status.as_i32(),
        status_message: span.status_message.clone(),
        instrumentation_name: span.instrumentation_scope.clone(),
        instrumentation_version: span.instrumentation_version.clone(),
        resource_attributes: span
            .resource_attrs
            .iter()
            .filter_map(|attr| {
                traceql_attr(attr)
                    .map(|value| krabka_traceql::Attribute::new(attr.key.clone(), value))
            })
            .collect(),
        attributes: span
            .span_attrs
            .iter()
            .filter_map(|attr| {
                traceql_attr(attr)
                    .map(|value| krabka_traceql::Attribute::new(attr.key.clone(), value))
            })
            .collect(),
        events: span
            .events
            .iter()
            .map(|event| event_ref(span, event))
            .collect(),
        links: span.links.iter().map(link_ref).collect(),
    }
}
